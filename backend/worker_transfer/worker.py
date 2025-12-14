import sys
import os
import time
import logging
import asyncio
import concurrent.futures
import json
import aiohttp
from urllib.parse import urlparse
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import TransferJob, JobStatus
from app.config import settings

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# DB Setup
engine = create_engine(settings.db_url)
SessionLocal = sessionmaker(bind=engine)

class JobWorker:
    def __init__(self, job_id, session):
        self.job_id = job_id
        self.session = session
        self.stopped = False
        self.process = None

    async def run(self):
        # Run DB operations in thread to avoid blocking main loop
        job = await asyncio.to_thread(self._get_job_start)
        if not job:
            return

        try:
            # Re-fetch connection details
            conn_details = await asyncio.to_thread(self._get_connection_details)
            if not conn_details:
                logger.error(f"Job {self.job_id}: Could not fetch connection details")
                return
            
            src_endpoint = settings.storage_src_endpoint
            src_ak = settings.storage_src_access_key
            src_sk = settings.storage_src_secret_key
            
            dst_endpoint = conn_details['endpoint']
            dst_ak = conn_details['ak']
            dst_sk = conn_details['sk']

            # # Resolve paths and endpoints
            # src_bucket, src_prefix = self.parse_path(job.src_dir, src_endpoint)
            # dst_bucket, dst_prefix = self.parse_path(job.dst_dir, dst_endpoint)

            # real_src_endpoint = self.clean_endpoint(src_endpoint, src_bucket)
            # real_dst_endpoint = self.clean_endpoint(dst_endpoint, dst_bucket)
            # # Construct URLs for r2s3 binary
            trans_src = f"{src_endpoint}/{job.src_dir}"
            trans_dest = f"{dst_endpoint}/{job.dst_dir}"

            # Check binary existence
            binary_path = '/usr/local/bin/r2s3'
            if not os.path.exists(binary_path):
                binary_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'r2s3')
                if not os.path.exists(binary_path):
                     raise Exception(f"r2s3 binary not found at /usr/local/bin/r2s3 or {binary_path}")

            while not self.stopped:
                logger.info(f"Job {self.job_id}: Starting sync cycle using r2s3")
                
                job_config = await asyncio.to_thread(self._get_job_config)
                
                # Run the binary
                stats = await self.run_r2s3(
                    binary_path, trans_src, trans_dest, 
                    src_ak, src_sk, dst_ak, dst_sk, 
                    job_config
                )
                
                if self.stopped:
                    break

                # Update job completion
                should_break = await asyncio.to_thread(self._finish_cycle, stats)
                if should_break:
                    break
                
                # If incremental, wait a bit
                await asyncio.sleep(60)
                
                # Check status
                if await asyncio.to_thread(self._check_stop_status):
                    break

        except Exception as e:
            logger.error(f"Job {self.job_id} failed: {e}", exc_info=True)
            await asyncio.to_thread(self._mark_failed, str(e))
        finally:
            if self.process and self.process.returncode is None:
                try:
                    self.process.terminate()
                    await self.process.wait()
                except Exception:
                    pass

    async def _monitor_stderr(self, stream):
        buffer = []
        async for line in stream:
            decoded_line = line.decode().strip()
            if decoded_line:
                logger.info(f"Job {self.job_id} [r2s3]: {decoded_line}")
                buffer.append(decoded_line)
                if len(buffer) > 50:
                    buffer.pop(0)
        return "\n".join(buffer)

    async def run_r2s3(self, binary_path, trans_src, trans_dest, src_ak, src_sk, dst_ak, dst_sk, job_config):
        env = os.environ.copy()
        env.update({
            'SOURCE_ACCESS_KEY_ID': src_ak,
            'SOURCE_SECRET_ACCESS_KEY': src_sk,
            'AWS_ACCESS_KEY_ID': dst_ak,
            'AWS_SECRET_ACCESS_KEY': dst_sk
        })

        args = [
            binary_path,
            '-trans_src', trans_src,
            '-trans_dest', trans_dest,
            '-service-url', settings.storage_transfer_service_url,
            '-threads', str(settings.max_worker_threads)
        ]
        if job_config.get('delete_source'):
            args.append('-delete_src')
        
        if job_config.get('include'):
            args.extend(['-include', job_config['include']])
        
        if job_config.get('exclude'):
            args.extend(['-exclude', job_config['exclude']])

        logger.info(f"Job {self.job_id} executing: {' '.join(args)}")

        self.process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )

        stderr_task = asyncio.create_task(self._monitor_stderr(self.process.stderr))

        stats = {'total': 0, 'transferred': 0, 'skipped': 0, 'deleted': 0, 'failed': 0}

        # Read stdout line by line
        async for line in self.process.stdout:
            decoded_line = line.decode().strip()
            if not decoded_line:
                continue
            try:
                data = json.loads(decoded_line)
                if data.get('type') == 'progress':
                    stats['total'] = data.get('total', 0)
                    stats['transferred'] = data.get('transferred', 0)
                    stats['skipped'] = data.get('skipped', 0)
                    stats['deleted'] = data.get('deleted', 0)
                    stats['failed'] = data.get('failed', 0)
                    # Update DB periodically (could be throttled, but for now we trust the binary reports 1/sec)
                    await asyncio.to_thread(self._update_stats_db, stats)
                
                # Check for stop signal during execution
                if await asyncio.to_thread(self._check_stop_status):
                    logger.info(f"Job {self.job_id}: Stop signal received. Terminating r2s3 process...")
                    self.stopped = True
                    self.process.terminate()
                    break

            except json.JSONDecodeError:
                pass # Ignore non-JSON output

        await self.process.wait()
        stderr_tail = await stderr_task

        if self.process.returncode != 0 and not self.stopped:
            raise Exception(f"r2s3 process failed with code {self.process.returncode}: {stderr_tail}")

        return stats

    def _get_job_start(self):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            if not job:
                return None
            job.status = JobStatus.RUNNING
            job.start_time = datetime.now(timezone.utc)
            job.result_message = "Starting..."
            db.commit()
            db.refresh(job)
            return job

    def _get_connection_details(self):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            if not job: return None
            metadata = job.metadata_rel
            sk = metadata.sk_encrypted
            if sk.startswith("enc_"):
                sk = sk[4:]
            return {
                'endpoint': metadata.endpoint,
                'ak': metadata.ak,
                'sk': sk
            }

    def _get_job_config(self):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            if not job:
                return {}
            return {
                'delete_source': job.delete_source,
                'include': job.include,
                'exclude': job.exclude
            }

    def _check_stop_status(self):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            return job and job.status in [JobStatus.STOPPED, JobStatus.FAILED, JobStatus.PAUSED]

    def _finish_cycle(self, stats):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            if not job: return True
            
            job.execution_count += 1
            job.duration_seconds = self.calculate_duration(job.start_time)
            
            msg = f"Completed. Total: {stats.get('total', 0)}, Transferred: {stats['transferred']}, Skipped: {stats['skipped']}, Deleted: {stats['deleted']}, Failed: {stats.get('failed', 0)}."
            
            if job.is_incremental:
                job.result_message = "Cycle " + msg
                db.commit()
                return False
            else:
                job.status = JobStatus.COMPLETED
                job.end_time = datetime.now(timezone.utc)
                job.result_message = msg
                db.commit()
                return True

    def _mark_failed(self, error_msg):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            if job:
                job.status = JobStatus.FAILED
                job.result_message = error_msg
                job.end_time = datetime.now(timezone.utc)
                db.commit()

    def _update_stats_db(self, stats):
         with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            if job:
                job.result_message = f"In Progress. Total: {stats.get('total', 0)}, Transferred: {stats['transferred']}, Skipped: {stats['skipped']}, Deleted: {stats['deleted']}, Failed: {stats.get('failed', 0)}."
                job.duration_seconds = self.calculate_duration(job.start_time)
                db.commit()

    def clean_endpoint(self, endpoint, bucket):
        u = urlparse(endpoint)
        netloc = u.netloc
        if netloc.startswith(f"{bucket}."):
            netloc = netloc[len(bucket)+1:]
        return u._replace(scheme='https', netloc=netloc).geturl()

    def parse_path(self, path, endpoint):
        u = urlparse(endpoint)
        host = u.netloc if u.netloc else u.path.split('/')[0]
        bucket = host.split('.')[0]
        prefix = path.lstrip('/')
        return bucket, prefix

    def construct_r2s3_url(self, endpoint, bucket, prefix):
        u = urlparse(endpoint)
        path = f"/{bucket}"
        if prefix:
            path = f"{path}/{prefix}"
        return u._replace(path=path).geturl()

    def calculate_duration(self, start_time):
        if not start_time:
            return 0
        now = datetime.now(timezone.utc)
        if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        return int((now - start_time).total_seconds())

class WorkerManager:
    def __init__(self):
        self.active_workers = {} # job_id -> task

    async def run(self):
        # Configure ThreadPoolExecutor
        loop = asyncio.get_running_loop()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=settings.max_worker_threads)
        loop.set_default_executor(self.executor)

        logger.info(f"Manager started with {settings.max_worker_threads} threads. Polling for jobs (Async)...")
        async with aiohttp.ClientSession() as session:
            self.session = session
            while True:
                try:
                    await self.poll()
                except Exception as e:
                    logger.error(f"Manager poll error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def poll(self):
        # DB Query in thread
        jobs = await asyncio.to_thread(self._get_pending_jobs)
        
        for job_id in jobs:
            if job_id not in self.active_workers:
                self.start_worker(job_id)
        
        # Cleanup finished tasks
        for jid in list(self.active_workers.keys()):
            task = self.active_workers[jid]
            if task.done():
                try:
                    task.result() # check for exceptions
                except Exception as e:
                    logger.error(f"Worker for job {jid} crashed: {e}")
                del self.active_workers[jid]

    def _get_pending_jobs(self):
        with SessionLocal() as db:
             # Just fetch IDs to avoid detachment issues
             jobs = db.query(TransferJob.job_id).filter(TransferJob.status.in_([JobStatus.PENDING, JobStatus.RUNNING])).all()
             return [j[0] for j in jobs]

    def start_worker(self, job_id):
        worker = JobWorker(job_id, self.session)
        task = asyncio.create_task(worker.run())
        self.active_workers[job_id] = task
        logger.info(f"Started async worker for job {job_id}")

if __name__ == "__main__":
    manager = WorkerManager()
    try:
        asyncio.run(manager.run())
    except KeyboardInterrupt:
        logger.info("Stopping manager...")
