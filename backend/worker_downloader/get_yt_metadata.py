import sys
import json
import yt_dlp
import time
import concurrent.futures
import requests
import uuid
import os
import yaml
from prometheus_client import start_http_server, Counter, Histogram

WORKER_ID = f"worker-meta-{uuid.uuid4()}"

# Metrics
TASKS_PROCESSED = Counter('worker_meta_tasks_processed_total', 'Total metadata tasks processed', ['status'])
TASK_DURATION = Histogram('worker_meta_task_duration_seconds', 'Duration of metadata extraction')

# API Configuration
API_BASE_URL = os.environ.get("BACKEND_API_URL", "http://localhost:8080/api")

JOB_CACHE = {}

def get_job_info(job_id):
    if job_id in JOB_CACHE:
        return JOB_CACHE[job_id]
    
    try:
        resp = requests.get(f"{API_BASE_URL}/youtube-jobs/{job_id}")
        if resp.status_code == 200:
            job = resp.json()
            JOB_CACHE[job_id] = job
            return job
    except Exception as e:
        print(f"Error fetching job info for {job_id}: {e}")
    
    return {}

def load_config():
    # Try multiple paths to find config.yaml
    paths = ["../../config.yaml", "../config.yaml", "config.yaml"]
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r') as f:
                    return yaml.safe_load(f)
            except Exception as e:
                print(f"Error loading config from {p}: {e}")
    print("Warning: config.yaml not found")
    return {}

def api_acquire_tasks(limit=10):
    try:
        resp = requests.post(
            f"{API_BASE_URL}/tasks/acquire", 
            json={"worker_id": WORKER_ID, "limit": limit, "stage": "metadata"}
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            print(f"Unexpected API response format: {type(data)}")
            return []
        elif resp.status_code == 404:
            return []
        else:
            print(f"API Error acquiring tasks: {resp.text}")
            return []
    except Exception as e:
        print(f"Connection Error: {e}")
        return []

def api_update_task_batch(updates):
    try:
        resp = requests.post(f"{API_BASE_URL}/tasks/update", json=updates)
        if resp.status_code != 200:
            print(f"Failed to update tasks: {resp.text}")
    except Exception as e:
        print(f"Failed to update tasks due to connection error: {e}")

@TASK_DURATION.time()
def process_metadata(task, ydl_opts):
    url = task.get('url')
    if not url:
        TASKS_PROCESSED.labels(status="failed").inc()
        return None
        
    print(f"Processing Task {task['id']}: {url}")
    
    job_info = get_job_info(task['job_id'])
    download_mode = job_info.get('download_mode', 'both') # both, audio, video

    result = {
        "id": task['id'],
        "job_id": task['job_id'],
        "status": "METADATA_FETCHED", # Success state for this worker
        "worker_id": WORKER_ID
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
            result["title"] = info.get('title', '')
            result["video_id"] = info.get('id', '')
            
            formats = info.get('formats', [])
            
            # Find best audio
            best_audio = None
            if download_mode in ['both', 'audio']:
                audio_formats = [f for f in reversed(formats) 
                                 if f.get('vcodec') == 'none' and f.get('acodec') != 'none']
                if audio_formats:
                    best_audio = next((f for f in audio_formats if f.get('language') == 'en'), None)
                    if not best_audio:
                        best_audio = audio_formats[0]
            
            # Find best video
            best_video = None
            if download_mode in ['both', 'video']:
                best_video = next((f for f in reversed(formats) 
                                   if f.get('vcodec') != 'none' and f.get('acodec') == 'none'), None)
            
            if best_audio:
                result["audio_url"] = best_audio.get('url')
                result["audio_size"] = best_audio.get('filesize') or best_audio.get('filesize_approx')
            if best_video:
                result["video_url"] = best_video.get('url')
                result["video_size"] = best_video.get('filesize') or best_video.get('filesize_approx')

            if not best_audio and not best_video:
                # Fallback to mixed if separate not found (rare for yt-dlp)
                pass
        
        if not result.get("audio_url") and not result.get("video_url"):
            result["status"] = "FAILED"
            result["error_message"] = "No video or audio URL found"
            result["is_download_fail"] = True
            TASKS_PROCESSED.labels(status="failed").inc()
        else:
            TASKS_PROCESSED.labels(status="success").inc()

    except Exception as e:
        print(f"Error processing {url}: {e}")
        result["status"] = "FAILED"
        result["error_message"] = str(e)
        if "Sign in to confirm you’re not a bot" in str(e):
            result["is_download_fail"] = True
        TASKS_PROCESSED.labels(status="failed").inc()
        
    return result

def main():
    # Start metrics server
    start_http_server(9092)
    print("Metrics server listening on :9092")

    config = load_config()
    proxy_address = config.get('worker', {}).get('proxy_url')
    
    if proxy_address:
        print(f"Using proxy: {proxy_address}")
    else:
        print("No proxy configured.")

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

    max_workers = 512
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    print(f"Metadata Worker {WORKER_ID} started with {max_workers} workers.")
    
    futures = set()
    pending_updates = []
    last_flush_time = time.time()
    
    # Threshold to fetch new tasks (avoid spamming acquire for 1 task)
    FETCH_THRESHOLD = 50 

    while True:
        try:
            # 1. Process completed tasks
            wait_timeout = 0.1 
            if len(futures) >= max_workers:
                wait_timeout = None # Block if full
            
            if futures:
                done, _ = concurrent.futures.wait(futures, timeout=wait_timeout, return_when=concurrent.futures.FIRST_COMPLETED)
            else:
                done = []
            
            for f in done:
                futures.remove(f)
                try:
                    res = f.result()
                    if res:
                        pending_updates.append(res)
                except Exception as e:
                    print(f"Task execution error: {e}")

            # 2. Flush updates
            now = time.time()
            if (len(pending_updates) >= 20 or 
                (pending_updates and (now - last_flush_time > 2)) or 
                (pending_updates and not futures)):
                
                api_update_task_batch(pending_updates)
                print(f"Updated {len(pending_updates)} tasks.")
                pending_updates = []
                last_flush_time = now
                
            # 3. Refill tasks
            slots = max_workers - len(futures)
            should_fetch = slots >= FETCH_THRESHOLD or (slots > 0 and len(futures) == 0)
            
            if should_fetch:
                tasks = api_acquire_tasks(limit=slots)
                if tasks:
                    print(f"Acquired {len(tasks)} tasks. (Running: {len(futures)})")
                    
                    # Mark RUNNING
                    running_updates = [{
                        "id": t['id'],
                        "job_id": t['job_id'],
                        "status": "RUNNING",
                        "worker_id": WORKER_ID
                    } for t in tasks]
                    api_update_task_batch(running_updates)
                    
                    # Submit
                    for t in tasks:
                        f = executor.submit(process_metadata, t, ydl_opts)
                        futures.add(f)
                else:
                    # No tasks available
                    if not futures:
                        time.sleep(2)
                    else:
                        time.sleep(1)

        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
