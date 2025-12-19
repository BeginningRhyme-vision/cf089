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
    region_name='auto' # R2 usually ignores region, but 'auto' is common
)

def get_content_length(url):
    try:
        req = urllib.request.Request(url, method='HEAD')
        # Add user agent to avoid 403
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        with urllib.request.urlopen(req, context=ssl_context) as response:
            return int(response.headers.get('Content-Length', 0))
    except Exception as e:
        # print(f"Error getting content length: {e}")
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
            # print(f"Uploading part {part_number} (Attempt {attempt + 1})...")
            res = send_json_request(WORKER_UPLOAD_URL, payload)
            # print(f"Part {part_number} success.")
            return {"PartNumber": part_number, "ETag": res['etag']}
        except Exception as e:
            print(f"Part {part_number} failed attempt {attempt + 1}: {e}")
            time.sleep(1)
    
    raise Exception(f"Part {part_number} failed after 3 attempts")

def abort_upload(key, upload_id):
    print(f"Aborting upload for {key}...")
    try:
        s3_client.abort_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key, UploadId=upload_id)
        print("Upload aborted successfully.")
    except Exception as e:
        print(f"Failed to abort upload: {e}")

def process_upload(source_url, filename, filesize):
    print(f"\n--- Starting upload for {filename} ---")
    
    if not filesize:
        print("Filesize not found in metadata, attempting to fetch via HEAD request...")
        filesize = get_content_length(source_url)
        if not filesize:
            raise Exception(f"Could not determine filesize for {filename}. Skipping upload.")

    print(f"Source URL: {source_url}")
    print(f"Total Size: {filesize} bytes")
    print(f"Worker URL: {WORKER_UPLOAD_URL}")

    # 1. Initiate (Local S3)
    try:
        key = "ytb-test/" + filename
        mpu = s3_client.create_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key)
        upload_id = mpu['UploadId']
        print(f"Initiated upload. Upload ID: {upload_id}")
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
                sys.stdout.write(f"\rProgress: {success_count}/{len(tasks)} parts uploaded")
                sys.stdout.flush()
            except Exception as e:
                print(f"\nFailed to upload a part: {e}")
                raise e

        print("\nAll parts uploaded. Completing multipart upload...")

        # 3. Complete (Local S3)
        # Sort parts by PartNumber just in case
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

def process_video_url(url, ydl_opts):
    print(f"Processing URL: {url}")
    while True:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                print(f"\n--- Parsed Download Links & Uploading ({url}) ---")

                formats = info.get('formats', [])
                video_id = info.get('id', 'video')
                
                # Filter for Audio: highest m4a container AAC encoding, excluding DRC
                audio_formats = [
                    f for f in formats 
                    if f.get('vcodec') == 'none' 
                    and f.get('acodec') != 'none' 
                    and f.get('ext') == 'webm'
                    and (f.get('acodec') or '').startswith('opus')
                    and 'drc' not in (f.get('format_note') or '').lower()
                    and 'drc' not in (f.get('format_id') or '').lower()
                ]
                best_audio = None
                if audio_formats:
                    best_audio = max(audio_formats, key=lambda f: f.get('abr') or 0)

                # Filter for Video: highest quality mp4 format H.264 encoding
                video_formats = [
                    f for f in formats 
                    if f.get('vcodec') != 'none' 
                    and f.get('acodec') == 'none' 
                    and f.get('ext') == 'webm'
                    and (f.get('vcodec') or '').startswith('vp9')
                ]
                
                best_video = None
                if video_formats:
                    # Sort by height, then tbr
                    best_video = max(video_formats, key=lambda f: (f.get('height') or 0, f.get('tbr') or 0))

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
                                best_audio.get('filesize')
                            )
                        except Exception as e:
                            print(f"Error uploading audio {audio_filename}: {e}")
                            print("Sleeping 60s inside host lock to throttle...")
                            time.sleep(60)
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
                                best_video.get('filesize')
                            )
                        except Exception as e:
                            print(f"Error uploading video {video_filename}: {e}")
                            print("Sleeping 60s inside host lock to throttle...")
                            time.sleep(60)
                            raise e
                else:
                    print(f"No video format found for {url}.")
            
            # Success, break retry loop and continue
            break

        except Exception as e:
            print(f"Error processing {url}: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_yt_metadata.py <url_list_file>")
        sys.exit(1)

    url_list_file = sys.argv[1]

    # Options to simulate --dump-json and getting metadata
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'simulate': True,
        'skip_download': True,
        'nocheckcertificate': True,
    }

    try:
        with open(url_list_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading file {url_list_file}: {e}")
        sys.exit(1)

    print(f"Found {len(urls)} URLs to process.")
    
    # Use ThreadPoolExecutor for parallel processing of videos
    # Adjust max_workers as needed to balance load
    max_parallel_videos = 1
    print(f"Starting parallel processing with max {max_parallel_videos} concurrent videos...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_videos) as executor:
        future_to_url = {executor.submit(process_video_url, url, ydl_opts): url for url in urls}
        
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as e:
                # This should theoretically not be reached because process_video_url has an infinite retry loop
                # but good to catch unexpected errors
                print(f"CRITICAL: Thread for {url} exited with error: {e}")

if __name__ == "__main__":
    main()