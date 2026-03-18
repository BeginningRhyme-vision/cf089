import requests
import time
import threading

proxy = "http://furion2025-zone-adam:furion2025@i7g4h7p1q3e7-na.grassdata.net:2345"
proxies = {
    "http": proxy,
    "https": proxy,
}

success = 0
failed = 0
lock = threading.Lock()

def test_req(i):
    global success, failed
    try:
        # We expect 405 from our worker
        resp = requests.get("https://r2-worker.cf022.workers.dev/upload-part", proxies=proxies, timeout=10)
        with lock:
            if resp.status_code == 405:
                success += 1
            else:
                print(f"[{i}] Unexpected status: {resp.status_code}")
                failed += 1
    except Exception as e:
        with lock:
            print(f"[{i}] Error: {e}")
            failed += 1

threads = []
for i in range(10):
    t = threading.Thread(target=test_req, args=(i,))
    threads.append(t)
    t.start()
    if i % 2 == 1:
        time.Sleep(0.5)

for t in threads:
    t.join()

print(f"Total: 10, Success: {success}, Failed: {failed}")
