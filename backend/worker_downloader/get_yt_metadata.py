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
import requests
import uuid
from datetime import datetime

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

CHUNK_SIZE = 6 * 1024 * 1024  # 6MB
WORKER_ID = f"worker-{uuid.uuid4()}"

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

# API Configuration
API_BASE_URL = os.environ.get("BACKEND_API_URL", "http://localhost:8000")

def api_acquire_tasks(limit=10):
    try:
        resp = requests.post(
            f"{API_BASE_URL}/youtube-jobs/tasks/acquire", 
            json={"worker_id": WORKER_ID, "limit": limit}
        )
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return []
        else:
            print(f"API Error acquiring tasks: {resp.text}")
            return []
    except Exception as e:
        print(f"Connection Error: {e}")
        return []

def api_get_job(job_id):
    resp = requests.get(f"{API_BASE_URL}/youtube-jobs/{job_id}")
    resp.raise_for_status()
    return resp.json()

def api_update_task(task_id, data):
    try:
        resp = requests.patch(f"{API_BASE_URL}/youtube-jobs/tasks/{task_id}", json=data)
        if resp.status_code != 200:
            print(f"Failed to update task {task_id}: {resp.text}")
    except Exception as e:
        print(f"Failed to update task {task_id} due to connection error: {e}")

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
    # print(f"\n--- Starting upload for {filename} ---")
    
    if not filesize:
        filesize = get_content_length(source_url)
        if not filesize:
            raise Exception(f"Could not determine filesize for {filename}. Skipping upload.")

    # Construct Key with R2 Prefix
    prefix = r2_prefix.strip()
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    key = prefix + filename

    # 1. Initiate (Local S3)
    try:
        mpu = s3_client.create_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key)
        upload_id = mpu['UploadId']
    except Exception as e:
        raise Exception(f"Failed to initiate upload: {e}")

    # 2. Upload Parts Concurrently
    parts = []
    tasks = []
    start = 0
    part_number = 1
    
    while start < filesize:
        end = min(start + CHUNK_SIZE - 1, filesize - 1)
        
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

    max_workers = min(len(tasks), 10) 
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    try:
        future_to_part = {executor.submit(upload_chunk, task): task for task in tasks}
        
        for future in concurrent.futures.as_completed(future_to_part):
            try:
                result = future.result()
                parts.append(result)
            except Exception as e:
                raise e

        # 3. Complete (Local S3)
        parts.sort(key=lambda x: x['PartNumber'])
        
        res = s3_client.complete_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        print(f"Upload completed: {res['Key']}")

    except Exception as e:
        executor.shutdown(wait=False, cancel_futures=True)
        abort_upload(key, upload_id)
        raise Exception(f"Upload failed: {e}")
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

def check_file_exists(key):
    try:
        s3_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
        return True
    except Exception:
        return False

def update_task_metadata(task_id, title, video_id):
    api_update_task(task_id, {"title": title, "video_id": video_id})

def update_task_status(task_id, status, error_message=None):
    data = {"status": status}
    if error_message is not None:
        data["error_message"] = error_message
    api_update_task(task_id, data)

def process_video_task(task_id, url, r2_prefix, ydl_opts):
    print(f"Processing Task ID {task_id} - URL: {url}")
    
    try:
        max_retries = 3
        success = False
        last_error = None
        
        prefix = r2_prefix.strip()
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        for attempt in range(max_retries):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
                    update_task_metadata(task_id, info.get('title'), info.get('id'))

                    formats = info.get('formats', [])
                    video_id = info.get('id', 'video')
                    
                    audio_formats = [f for f in reversed(formats) 
                                     if f.get('vcodec') == 'none' and f.get('acodec') != 'none']
                    
                    best_audio = None
                    if audio_formats:
                        best_audio = next((f for f in audio_formats if f.get('language') == 'en'), None)
                        if not best_audio:
                            best_audio = audio_formats[0]

                    best_video = next((f for f in reversed(formats) 
                                       if f.get('vcodec') != 'none' and f.get('acodec') == 'none'), None)

                    if best_audio:
                        audio_filename = f"{video_id}_audio.{best_audio.get('ext')}"
                        audio_url = best_audio.get('url')
                        audio_key = prefix + audio_filename
                        
                        if not check_file_exists(audio_key):
                            lock = host_lock_manager.get_lock(audio_url)
                            with lock:
                                process_upload(
                                    audio_url, 
                                    audio_filename, 
                                    best_audio.get('filesize'),
                                    r2_prefix
                                )
                    
                    if best_video:
                        video_filename = f"{video_id}_video.{best_video.get('ext')}"
                        video_url = best_video.get('url')
                        video_key = prefix + video_filename
                        
                        if not check_file_exists(video_key):
                            lock = host_lock_manager.get_lock(video_url)
                            with lock:
                                process_upload(
                                    video_url, 
                                    video_filename, 
                                    best_video.get('filesize'),
                                    r2_prefix
                                )
                
                success = True
                break

            except Exception as e:
                last_error = e
                print(f"Error processing {url}: {e}")
                
                if "Video unavailable" in str(e) or "This video is private" in str(e):
                    break
                
                time.sleep(5)

        if success:
            update_task_status(task_id, "COMPLETED")
        else:
            update_task_status(task_id, "FAILED", str(last_error))

    except Exception as e:
        print(f"Critical error for task {task_id}: {e}")
        update_task_status(task_id, "FAILED", f"Critical: {str(e)}")

def process_tasks(tasks, proxy_address=None):
    if not tasks:
        return

    print(f"Processing batch of {len(tasks)} tasks...")

    # We need the prefix. It's not in the task object directly unless we join.
    # The API 'YoutubeTask' schema in list doesn't include job details usually, 
    # but the worker needs `r2_prefix`.
    # Optimization: The worker could fetch the job details once per job_id seen.
    # OR: The acquire endpoint should return job_r2_prefix.
    # Let's check `schemas.py`. `YoutubeTask` has `job_id`.
    # We will fetch job details for each unique job_id in the batch.
    
    job_cache = {}
    
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_videos) as executor:
        futures = []
        for task in tasks:
            jid = task['job_id']
            if jid not in job_cache:
                try:
                    job_info = api_get_job(jid)
                    job_cache[jid] = job_info['r2_prefix']
                except:
                    print(f"Failed to get job info for {jid}, skipping task {task['id']}")
                    continue
            
            r2_prefix = job_cache[jid]
            futures.append(executor.submit(process_video_task, task['id'], task['url'], r2_prefix, ydl_opts))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread error: {e}")

def main():
    proxy_address = None
    if len(sys.argv) > 1:
        proxy_address = sys.argv[1]

    print(f"Worker {WORKER_ID} started. Polling for tasks...")

    while True:
        tasks = api_acquire_tasks(limit=256) # Small batch for testing/stability
        if not tasks:
            # print("No pending tasks. Waiting...")
            time.sleep(5)
            continue
        
        process_tasks(tasks, proxy_address)

if __name__ == "__main__":
    main()