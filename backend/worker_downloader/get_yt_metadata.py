import sys
import json
import yt_dlp
import urllib.request
import urllib.error
import urllib.parse
import time
import ssl
import concurrent.futures
import threading
import os
from datetime import datetime
from sqlalchemy import or_, not_

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Please install it with `pip install PyYAML`.")
    sys.exit(1)

try:
    import boto3
    from botocore.config import Config
except ImportError:
    print("Error: boto3 is required. Please install it with `pip install boto3`.")
    sys.exit(1)

# Add backend directory to sys.path to allow importing app modules
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

try:
    from app.database import SessionLocal
    from app.models import YoutubeJob, YoutubeRecord, JobStatus
except ImportError as e:
    print(f"Error importing app modules: {e}")
    sys.exit(1)

CHUNK_SIZE = 6 * 1024 * 1024  # 6MB

# Bypass SSL verification
ssl_context = ssl._create_unverified_context()

# R2 Configuration
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'config.yaml')

if not os.path.exists(CONFIG_PATH):
    print(f"Error: Config file not found at {CONFIG_PATH}")
    sys.exit(1)

with open(CONFIG_PATH, 'r') as f:
    config_data = yaml.safe_load(f)

storage_root = config_data.get('storage', {})
storage_conf = storage_root.get('src', {})

full_endpoint = storage_conf.get('endpoint')
R2_ACCESS_KEY_ID = storage_conf.get('access_key')
R2_SECRET_ACCESS_KEY = storage_conf.get('secret_key')
WORKER_UPLOAD_URL = storage_root.get('download_service_url')

if not all([full_endpoint, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, WORKER_UPLOAD_URL]):
    print("Error: Missing R2 configuration in config.yaml (storage.src or storage.download_service_url).")
    sys.exit(1)

# Parse endpoint and bucket
parsed_url = urllib.parse.urlparse(full_endpoint)
R2_ENDPOINT_URL = f"{parsed_url.scheme}://{parsed_url.netloc}"
R2_BUCKET_NAME = parsed_url.path.strip('/')

if not R2_BUCKET_NAME:
    print("Error: Could not derive bucket name from storage.src.endpoint in config.yaml")
    sys.exit(1)

s3_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'}),
    region_name='auto'
)

def get_content_length(url):
    try:
        req = urllib.request.Request(url, method='HEAD')
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        with urllib.request.urlopen(req, context=ssl_context) as response:
            return int(response.headers.get('Content-Length', 0))
    except Exception as e:
        return 0

def send_json_request(url, data):
    req = urllib.request.Request(url, method='POST')
    req.add_header('Content-Type', 'application/json')
    req.add_header('User-Agent', 'yt-dlp-upload-client')
    json_data = json.dumps(data).encode('utf-8')
    
    try:
        with urllib.request.urlopen(req, data=json_data, context=ssl_context) as response:
            if 200 <= response.status < 300:
                return json.loads(response.read().decode('utf-8'))
            else:
                raise Exception(f"HTTP {response.status}: {response.read().decode('utf-8')}")
    except urllib.error.HTTPError as e:
        raise Exception(f"HTTP {e.code}: {e.read().decode('utf-8')}")

def upload_chunk(task_data):
    part_number = task_data['partNumber']
    destination_url = task_data['destinationUrl']
    
    payload = {
        "fileUrl": task_data['url'],
        "offset": task_data['start'],
        "size": task_data['end'] - task_data['start'] + 1,
        "r2Key": destination_url,
        "partNumber": part_number,
        "uploadId": task_data['uploadId']
    }

    for attempt in range(3):
        try:
            res = send_json_request(WORKER_UPLOAD_URL, payload)
            return {"PartNumber": part_number, "ETag": res['etag']}
        except Exception as e:
            # print(f"Part {part_number} failed attempt {attempt + 1}: {e}")
            time.sleep(1)
    
    raise Exception(f"Part {part_number} failed after 3 attempts")

def abort_upload(key, upload_id):
    print(f"Aborting upload for {key}...")
    try:
        s3_client.abort_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key, UploadId=upload_id)
        print("Upload aborted successfully.")
    except Exception as e:
        print(f"Failed to abort upload: {e}")

def process_upload(source_url, filename, filesize, r2_prefix):
    print(f"\n--- Starting upload for {filename} ---")
    
    if not filesize:
        print("Filesize not found in metadata, attempting to fetch via HEAD request...")
        filesize = get_content_length(source_url)
        if not filesize:
            raise Exception(f"Could not determine filesize for {filename}. Skipping upload.")

    print(f"Source URL: {source_url}")
    print(f"Total Size: {filesize} bytes")
    print(f"Worker URL: {WORKER_UPLOAD_URL}")

    # Construct Key with R2 Prefix
    # Ensure r2_prefix ends with / if it's a folder, or handle it smartly
    prefix = r2_prefix.strip()
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    key = prefix + filename

    # 1. Initiate (Local S3)
    try:
        mpu = s3_client.create_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key)
        upload_id = mpu['UploadId']
        print(f"Initiated upload. Key: {key}, Upload ID: {upload_id}")
    except Exception as e:
        raise Exception(f"Failed to initiate upload: {e}")

    # 2. Upload Parts Concurrently
    parts = []
    tasks = []
    start = 0
    part_number = 1
    
    while start < filesize:
        end = min(start + CHUNK_SIZE - 1, filesize - 1)
        
        # Generate R2 Key URL (matching r2s3.go logic)
        presigned_url = f"{parsed_url.scheme}://{R2_BUCKET_NAME}.{parsed_url.netloc}/{key}"

        tasks.append({
            "url": source_url,
            "start": start,
            "end": end,
            "destinationUrl": presigned_url,
            "partNumber": part_number,
            "uploadId": upload_id
        })
        start += CHUNK_SIZE
        part_number += 1

    print(f"Prepared {len(tasks)} chunks. Starting concurrent execution...")

    # Using a reasonably large pool for concurrency
    max_workers = min(len(tasks), 10) 
    success_count = 0
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    try:
        future_to_part = {executor.submit(upload_chunk, task): task for task in tasks}
        
        for future in concurrent.futures.as_completed(future_to_part):
            try:
                result = future.result()
                parts.append(result)
                success_count += 1
                # sys.stdout.write(f"\rProgress: {success_count}/{len(tasks)} parts uploaded")
                # sys.stdout.flush()
            except Exception as e:
                print(f"\nFailed to upload a part: {e}")
                raise e

        # print("\nAll parts uploaded. Completing multipart upload...")

        # 3. Complete (Local S3)
        parts.sort(key=lambda x: x['PartNumber'])
        
        res = s3_client.complete_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        print(f"Upload completed successfully! Key: {res['Key']}")

    except Exception as e:
        print(f"\nError during upload process: {e}")
        print("Aborting upload...")
        executor.shutdown(wait=False, cancel_futures=True)
        abort_upload(key, upload_id)
        raise Exception(f"Upload aborted and failed: {e}")
    finally:
        executor.shutdown(wait=True)

class HostLockManager:
    def __init__(self):
        self._locks = {}
        self._global_lock = threading.Lock()

    def get_lock(self, url):
        try:
            hostname = urllib.parse.urlparse(url).hostname
        except:
            hostname = "unknown"
            
        if not hostname:
             hostname = "unknown"

        with self._global_lock:
            if hostname not in self._locks:
                self._locks[hostname] = threading.Lock()
            return self._locks[hostname]

host_lock_manager = HostLockManager()

def update_record_metadata(record_id, title, video_id):
    db = SessionLocal()
    try:
        record = db.query(YoutubeRecord).filter(YoutubeRecord.id == record_id).first()
        if record:
            if title: record.title = title
            if video_id: record.video_id = video_id
            db.commit()
    finally:
        db.close()

def update_record_status(record_id, status, error_message=None):
    db = SessionLocal()
    try:
        record = db.query(YoutubeRecord).filter(YoutubeRecord.id == record_id).first()
        if record:
            record.status = status
            record.error_message = error_message
            db.commit()
    finally:
        db.close()

def process_video_record(record_id, url, r2_prefix, ydl_opts):
    print(f"Processing Record ID {record_id} - URL: {url}")
    
    try:
        # Retry loop for stability within a single run
        max_retries = 3
        success = False
        last_error = None

        for attempt in range(max_retries):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
                    # Update Title/ID if available
                    update_record_metadata(record_id, info.get('title'), info.get('id'))

                    print(f"\n--- Parsed Download Links & Uploading ({url}) ---")

                    formats = info.get('formats', [])
                    video_id = info.get('id', 'video')
                    
                    # Select best video/audio based on yt-dlp's default sorting (worst to best)
                    best_audio = next((f for f in reversed(formats) 
                                       if f.get('vcodec') == 'none' and f.get('acodec') != 'none'), None)

                    best_video = next((f for f in reversed(formats) 
                                       if f.get('vcodec') != 'none' and f.get('acodec') == 'none'), None)

                    if best_audio:
                        print(f"\n[Audio] Best found: {best_audio.get('format_id')} ({best_audio.get('ext')})")
                        audio_filename = f"{video_id}_audio.{best_audio.get('ext')}"
                        audio_url = best_audio.get('url')
                        
                        lock = host_lock_manager.get_lock(audio_url)
                        with lock:
                            try:
                                process_upload(
                                    audio_url, 
                                    audio_filename, 
                                    best_audio.get('filesize'),
                                    r2_prefix
                                )
                            except Exception as e:
                                print(f"Error uploading audio {audio_filename}: {e}")
                                # Throttle
                                time.sleep(10)
                                raise e
                    else:
                        print(f"No audio format found for {url}.")

                    if best_video:
                        print(f"\n[Video] Best found: {best_video.get('format_id')} ({best_video.get('ext')})")
                        video_filename = f"{video_id}_video.{best_video.get('ext')}"
                        video_url = best_video.get('url')

                        lock = host_lock_manager.get_lock(video_url)
                        with lock:
                            try:
                                process_upload(
                                    video_url, 
                                    video_filename, 
                                    best_video.get('filesize'),
                                    r2_prefix
                                )
                            except Exception as e:
                                print(f"Error uploading video {video_filename}: {e}")
                                time.sleep(10)
                                raise e
                    else:
                        print(f"No video format found for {url}.")
                
                success = True
                break # Success

            except Exception as e:
                last_error = e
                print(f"Error processing {url} (Attempt {attempt+1}/{max_retries}): {e}")
                
                if "Video unavailable" in str(e):
                    print(f"Video unavailable for {url}, skipping retries.")
                    break
                
                time.sleep(5)

        if success:
            update_record_status(record_id, JobStatus.COMPLETED)
        else:
            update_record_status(record_id, JobStatus.FAILED, str(last_error))

    except Exception as e:
        print(f"Critical error for record {record_id}: {e}")
        update_record_status(record_id, JobStatus.FAILED, f"Critical: {str(e)}")

def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python get_yt_metadata.py <job_id> [proxy_address]")
        sys.exit(1)

    try:
        job_id = int(sys.argv[1])
        proxy_address = sys.argv[2] if len(sys.argv) == 3 else None
    except ValueError:
        print("Job ID must be an integer.")
        sys.exit(1)

    print(f"Starting worker for Job ID: {job_id}")

    db = SessionLocal()
    job = db.query(YoutubeJob).filter(YoutubeJob.id == job_id).first()
    
    if not job:
        print(f"Job {job_id} not found.")
        db.close()
        sys.exit(1)
    
    # Update Job status
    job.status = JobStatus.RUNNING
    db.commit()

    r2_prefix = job.r2_prefix

    # Fetch pending or failed records
    # We want to retry failed ones too, but skip those that are definitely unavailable
    records = db.query(YoutubeRecord).filter(
        YoutubeRecord.job_id == job_id,
        YoutubeRecord.status.in_([JobStatus.PENDING, JobStatus.FAILED]),
        or_(
            YoutubeRecord.error_message == None,
            not_(YoutubeRecord.error_message.contains("Video unavailable"))
        )
    ).all()
    
    # Detach objects so we can close session or pass data to threads safely
    # Actually, we will just pass IDs and let threads open their own sessions
    record_ids = [r.id for r in records]
    record_urls = {r.id: r.url for r in records}
    
    db.close() # Close main session

    print(f"Found {len(records)} records to process.")
    
    # yt-dlp options
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'simulate': True,
        'skip_download': True,
        'nocheckcertificate': True,
    }
    
    if proxy_address:
        ydl_opts['proxy'] = proxy_address

    max_parallel_videos = 256
    print(f"Starting parallel processing with max {max_parallel_videos} concurrent videos...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_videos) as executor:
        future_to_id = {
            executor.submit(process_video_record, rid, record_urls[rid], r2_prefix, ydl_opts): rid 
            for rid in record_ids
        }
        
        for future in concurrent.futures.as_completed(future_to_id):
            rid = future_to_id[future]
            try:
                future.result()
            except Exception as e:
                print(f"CRITICAL: Thread for record {rid} exited with error: {e}")

    # Check if all completed
    db = SessionLocal()
    pending_count = db.query(YoutubeRecord).filter(
        YoutubeRecord.job_id == job_id,
        YoutubeRecord.status.in_([JobStatus.PENDING, JobStatus.RUNNING])
    ).count()
    
    failed_count = db.query(YoutubeRecord).filter(
        YoutubeRecord.job_id == job_id,
        YoutubeRecord.status == JobStatus.FAILED
    ).count()

    job = db.query(YoutubeJob).filter(YoutubeJob.id == job_id).first()
    if pending_count == 0:
        if failed_count > 0:
             # If some failed, maybe keep job as RUNNING or PARTIAL? 
             # Or set to COMPLETED/FAILED based on threshold?
             # For now, if no pending, we mark job as COMPLETED (with errors) or FAILED.
             # Let's say COMPLETED means "Done trying". Users can see failed count.
             job.status = JobStatus.COMPLETED 
        else:
             job.status = JobStatus.COMPLETED
    else:
        # If script finishes but pending remain (shouldn't happen with wait), something wrong
        pass

    db.commit()
    db.close()
    print("Job processing finished.")

if __name__ == "__main__":
    main()
