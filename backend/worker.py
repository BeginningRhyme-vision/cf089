import sys
import os
import time
import logging
import asyncio
import concurrent.futures
import boto3
import json
import aiohttp
from urllib.parse import urlparse
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import TransferJob, JobStatus, TransferMetadata
from app.config import settings
from botocore.exceptions import ClientError

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# DB Setup
engine = create_engine(settings.db_url)
SessionLocal = sessionmaker(bind=engine)

# Constants & Limits matching r2s3.go logic
DEFAULT_PART_SIZE = 16 * 1024 * 1024 # 16MB
MAX_CONCURRENT_FILES = settings.max_worker_threads/2
MAX_CONCURRENT_PARTS = settings.max_worker_threads*10

def get_s3_client(endpoint=None, ak=None, sk=None, region="auto", is_r2=False):
    if is_r2:
        config = boto3.session.Config(s3={'addressing_style': "path"}, signature_version='s3v4')
    else:
        config = boto3.session.Config(s3={'addressing_style': "virtual"}, signature_version='s3v4')
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=region,
        config=config
    )

class JobWorker:
    def __init__(self, job_id, session):
        self.job_id = job_id
        self.session = session
        self.stopped = False
        self.file_sem = asyncio.Semaphore(MAX_CONCURRENT_FILES)
        self.part_sem = asyncio.Semaphore(MAX_CONCURRENT_PARTS)
        self.src_cfg = {}
        self.dst_cfg = {}

    async def run(self):
        # Run DB operations in thread to avoid blocking main loop
        job = await asyncio.to_thread(self._get_job_start)
        if not job:
            return

        try:
            # Setup clients (Boto3 clients are thread-safe enough for our usage, 
            # but their methods block, so we will use to_thread for calls)
            # We create them here in the main thread (async loop thread)
            
            # Resolve endpoints
            src_bucket, src_prefix = self.parse_path(job.src_dir, settings.storage_src_endpoint)
            # For destination, we need metadata which we got in _get_job_start but need to access it.
            # However, `job` is a detached object or attached to a closed session?
            # We should be careful with lazy loading.
            # Let's re-query or fetch needed data in _get_job_start.
            
            # Actually, let's keep it simple: use a context manager for DB scope when needed.
            # But we need to keep job status updated.
            
            # Re-fetch connection details
            conn_details = await asyncio.to_thread(self._get_connection_details)
            if not conn_details:
                return
            
            src_endpoint = settings.storage_src_endpoint
            src_ak = settings.storage_src_access_key
            src_sk = settings.storage_src_secret_key
            
            dst_endpoint = conn_details['endpoint']
            dst_ak = conn_details['ak']
            dst_sk = conn_details['sk']

            dst_bucket, dst_prefix = self.parse_path(job.dst_dir, dst_endpoint)

            real_src_endpoint = self.clean_endpoint(src_endpoint, src_bucket)
            real_dst_endpoint = self.clean_endpoint(dst_endpoint, dst_bucket)

            # Init clients
            # Note: client creation is fast enough, can be done in main thread
            src_client = get_s3_client(endpoint=real_src_endpoint, ak=src_ak, sk=src_sk, is_r2=True)
            dst_client = get_s3_client(endpoint=real_dst_endpoint, ak=dst_ak, sk=dst_sk)

            self.src_cfg = {'endpoint': real_src_endpoint, 'bucket': src_bucket, 'prefix': src_prefix}
            self.dst_cfg = {'endpoint': real_dst_endpoint, 'bucket': dst_bucket, 'prefix': dst_prefix}

            while not self.stopped:
                logger.info(f"Job {self.job_id}: Starting sync cycle")
                
                # We need 'delete_source' flag
                delete_source = await asyncio.to_thread(self._get_delete_source)
                
                stats = await self.sync_bucket(src_client, src_bucket, src_prefix, dst_client, dst_bucket, dst_prefix, delete_source)
                
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
            return job # Detached, but simple fields are accessible. access rels might fail if not eager loaded.

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

    def _get_delete_source(self):
        with SessionLocal() as db:
            job = db.query(TransferJob).filter(TransferJob.job_id == self.job_id).first()
            return job.delete_source if job else False

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
            
            msg = f"Completed. Total: {stats.get('total', 0)}, Transferred: {stats['transferred']}, Skipped: {stats['skipped']}, Deleted: {stats['deleted']}."
            
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
                job.result_message = f"In Progress. Total: {stats.get('total', 0)}, Transferred: {stats['transferred']}, Skipped: {stats['skipped']}, Deleted: {stats['deleted']}."
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

    def list_objects(self, client, bucket, prefix):
        # Blocking call, run in executor
        paginator = client.get_paginator('list_objects_v2')
        objects = {}
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('/'): continue
                    
                    rel_key = key
                    if prefix:
                        if key == prefix: continue
                        if key.startswith(prefix + "/"):
                            rel_key = key[len(prefix)+1:]
                        elif key.startswith(prefix):
                            rel_key = key[len(prefix):]
                    
                    objects[rel_key] = {
                        'Key': key,
                        'Size': obj['Size'],
                        'ETag': obj.get('ETag', '').strip('"')
                    }
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                raise Exception(f"Bucket {bucket} not found")
            raise e
        return objects

    def calculate_duration(self, start_time):
        if not start_time:
            return 0
        now = datetime.now(timezone.utc)
        if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        return int((now - start_time).total_seconds())

    async def sync_bucket(self, src_client, src_bucket, src_prefix, dst_client, dst_bucket, dst_prefix, delete_source):
        stats = {'total': 0, 'transferred': 0, 'skipped': 0, 'deleted': 0}
        
        # List objects in thread pool
        dst_objs = await asyncio.to_thread(self.list_objects, dst_client, dst_bucket, dst_prefix)
        src_objs = await asyncio.to_thread(self.list_objects, src_client, src_bucket, src_prefix)
        
        stats['total'] = len(src_objs)
        
        # Stats updater task
        async def stats_updater():
            while True:
                await asyncio.sleep(10)
                await asyncio.to_thread(self._update_stats_db, stats)

        updater_task = asyncio.create_task(stats_updater())
        
        tasks = []
        try:
            for rel_key, src_obj in src_objs.items():
                if self.stopped: break
                
                should_transfer = True
                dst_key = os.path.join(dst_prefix, rel_key) if dst_prefix else rel_key
                dst_key = dst_key.replace("//", "/")
                
                if rel_key in dst_objs:
                    dst_obj = dst_objs[rel_key]
                    if src_obj['Size'] == dst_obj['Size']:
                        should_transfer = False
                        stats['skipped'] += 1
                
                if should_transfer:
                    tasks.append(asyncio.create_task(
                        self.process_object(src_client, dst_client, src_obj, dst_key, delete_source, stats)
                    ))
                elif delete_source:
                    # Run delete in thread
                    await asyncio.to_thread(self._safe_delete, src_client, src_bucket, src_obj['Key'], stats)

            if tasks:
                await asyncio.gather(*tasks)
                
        finally:
            updater_task.cancel()
            try:
                await updater_task
            except asyncio.CancelledError:
                pass
            # Final stats update
            await asyncio.to_thread(self._update_stats_db, stats)

        return stats

    def _safe_delete(self, client, bucket, key, stats):
        try:
            client.delete_object(Bucket=bucket, Key=key)
            stats['deleted'] += 1
        except: pass

    def construct_url(self, endpoint, bucket, key):
        u = urlparse(endpoint)
        s = u.scheme
        return f"{s}://{bucket}.{u.netloc}/{key}"

    async def process_object(self, src_client, dst_client, src_obj, dst_key, delete_source, stats):
        async with self.file_sem:
            try:
                r2_key_url = self.construct_url(self.src_cfg['endpoint'], self.src_cfg['bucket'], src_obj['Key'])
                s3_url = self.construct_url(self.dst_cfg['endpoint'], self.dst_cfg['bucket'], dst_key)
                
                size = src_obj['Size']
                # Logger might be too verbose for async, but keep for now
                # logger.info(f"Transferring {src_obj['Key']} -> {dst_key} ({size} bytes)")

                if size < DEFAULT_PART_SIZE:
                    await self.try_action(lambda: self.call_external_service(r2_key_url, s3_url, size, 0, "", -1))
                else:
                    await self.handle_multipart(dst_client, dst_key, size, r2_key_url, s3_url)

                if delete_source:
                     await asyncio.to_thread(src_client.delete_object, Bucket=self.src_cfg['bucket'], Key=src_obj['Key'])
                
                stats['transferred'] += 1
            except Exception as e:
                logger.error(f"File transfer failed for {src_obj['Key']}: {e}")

    async def handle_multipart(self, dst_client, dst_key, size, r2_key_url, s3_url):
        # Create Multipart - Blocking
        mp = await asyncio.to_thread(dst_client.create_multipart_upload, Bucket=self.dst_cfg['bucket'], Key=dst_key)
        upload_id = mp['UploadId']
        
        try:
            part_size = self.calculate_part_size(size)
            num_parts = (size + part_size - 1) // part_size
            
            parts = []
            part_tasks = []
            
            for i in range(num_parts):
                part_num = i + 1
                offset = i * part_size
                p_size = min(part_size, size - offset)
                
                part_tasks.append(asyncio.create_task(
                    self.transfer_part(r2_key_url, s3_url, p_size, offset, upload_id, part_num)
                ))

            # Wait for all parts
            results = await asyncio.gather(*part_tasks)
            
            # Results are ETags
            for i, etag in enumerate(results):
                parts.append({'PartNumber': i + 1, 'ETag': etag})
            
            parts.sort(key=lambda x: x['PartNumber'])
            
            # Complete - Blocking
            await asyncio.to_thread(
                dst_client.complete_multipart_upload,
                Bucket=self.dst_cfg['bucket'], 
                Key=dst_key, 
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            await asyncio.to_thread(dst_client.abort_multipart_upload, Bucket=self.dst_cfg['bucket'], Key=dst_key, UploadId=upload_id)
            raise e

    async def transfer_part(self, r2_key, s3_url, size, offset, upload_id, part_num):
        async with self.part_sem:
            return await self.try_action(lambda: self.call_external_service(r2_key, s3_url, size, offset, upload_id, part_num))

    def calculate_part_size(self, size):
        part_size = DEFAULT_PART_SIZE
        if size > part_size * 10000:
            part_size = size // 10000
            part_size = ((part_size - 1) // 1048576 + 1) * 1048576
        return int(part_size)

    async def try_action(self, func, retries=3):
        for i in range(retries):
            try:
                return await func()
            except Exception as e:
                if i == retries - 1: raise e
                await asyncio.sleep(i + 1)

    async def call_external_service(self, r2_key, s3_url, size, offset, upload_id, part_num):
        payload = {
            "r2Key": r2_key,
            "s3Url": s3_url,
            "size": size,
            "offset": offset,
            "uploadId": upload_id,
            "partNumber": part_num
        }
        
        async with self.session.post(settings.storage_transfer_service_url, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if "etag" not in data:
                raise Exception("No etag in response")
            return data["etag"]

class WorkerManager:
    def __init__(self):
        self.active_workers = {} # job_id -> task

    async def run(self):
        # Configure ThreadPoolExecutor
        loop = asyncio.get_running_loop()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=settings.max_worker_threads)
        loop.set_default_executor(self.executor)

        logger.info(f"Manager started with {settings.max_worker_threads} threads. Polling for jobs (Async)...")
        # Use a single shared session for all jobs? 
        # Or one per job? One shared is better for connection pooling.
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