import sys
import json
import yt_dlp
import time
import concurrent.futures
import requests
import uuid
import os

WORKER_ID = f"worker-meta-{uuid.uuid4()}"

# API Configuration
API_BASE_URL = os.environ.get("BACKEND_API_URL", "http://localhost:8080/api")

def api_acquire_tasks(limit=10):
    try:
        resp = requests.post(
            f"{API_BASE_URL}/tasks/acquire", 
            json={"worker_id": WORKER_ID, "limit": limit, "stage": "metadata"}
        )
        if resp.status_code == 200:
            return resp.json() # Expecting list of tasks
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

def process_metadata(task, ydl_opts):
    url = task.get('url')
    if not url:
        return None
        
    print(f"Processing Task {task['id']}: {url}")
    
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
            audio_formats = [f for f in reversed(formats) 
                             if f.get('vcodec') == 'none' and f.get('acodec') != 'none']
            best_audio = None
            if audio_formats:
                best_audio = next((f for f in audio_formats if f.get('language') == 'en'), None)
                if not best_audio:
                    best_audio = audio_formats[0]
            
            # Find best video
            best_video = next((f for f in reversed(formats) 
                               if f.get('vcodec') != 'none' and f.get('acodec') == 'none'), None)
            
            if best_audio:
                result["audio_url"] = best_audio.get('url')
            if best_video:
                result["video_url"] = best_video.get('url')

            if not best_audio and not best_video:
                # Fallback to mixed if separate not found (rare for yt-dlp)
                pass

    except Exception as e:
        print(f"Error processing {url}: {e}")
        result["status"] = "FAILED"
        result["error_message"] = str(e)
        
    return result

def main():
    proxy_address = None
    if len(sys.argv) > 1:
        proxy_address = sys.argv[1]

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

    max_workers = 16
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    print(f"Metadata Worker {WORKER_ID} started.")

    while True:
        try:
            tasks = api_acquire_tasks(limit=max_workers)
            
            if not tasks:
                time.sleep(2)
                continue
                
            print(f"Acquired {len(tasks)} tasks.")
            
            futures = {executor.submit(process_metadata, t, ydl_opts): t for t in tasks}
            
            updates = []
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res:
                    updates.append(res)
            
            if updates:
                api_update_task_batch(updates)
                print(f"Updated {len(updates)} tasks.")
                
        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
