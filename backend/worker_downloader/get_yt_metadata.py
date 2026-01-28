import sys
import json
import yt_dlp
import time
import concurrent.futures
import requests
import uuid
import os
import yaml
import tempfile
import socket
import threading
from collections import deque
from urllib.parse import urlparse
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Histogram
import boto3
from botocore.exceptions import ClientError

WORKER_ID = f"worker-meta-{uuid.uuid4()}"

# Metrics
TASKS_PROCESSED = Counter('worker_meta_tasks_processed_total', 'Total metadata tasks processed', ['status'])
TASK_DURATION = Histogram('worker_meta_task_duration_seconds', 'Duration of metadata extraction')

# API Configuration
API_BASE_URL = os.environ.get("BACKEND_API_URL", "http://localhost:8080/api")

def format_time_go_compatible(dt=None):
    """
    生成与 Go 后端兼容的时间格式 (RFC3339Nano with timezone)
    
    Go 使用 time.Now() 生成的时间格式: "2026-01-24T23:07:33.318898448+09:00"
    - 9位纳秒精度
    - 包含时区信息
    
    Python datetime 只有微秒精度，需要补零到纳秒
    
    Args:
        dt: datetime 对象，如果为 None 则使用当前时间
    
    Returns:
        str: RFC3339Nano 格式的时间字符串，例如 "2026-01-24T23:07:33.318898448+00:00"
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        # 如果没有时区信息，假设为 UTC
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # 转换为 UTC
        dt = dt.astimezone(timezone.utc)
    
    # Python datetime 只有微秒精度，需要补零到纳秒（9位）
    microseconds = dt.microsecond
    nanoseconds_str = f"{microseconds:06d}000"  # 补零到9位纳秒
    
    # 格式化为 RFC3339Nano: YYYY-MM-DDTHH:MM:SS.nnnnnnnnn+00:00
    return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{nanoseconds_str}+00:00")

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

def get_machine_name():
    """获取机器名，优先使用环境变量，否则使用主机名"""
    machine_name = os.environ.get('MACHINE_NAME')
    if machine_name:
        return machine_name
    try:
        return socket.gethostname()
    except:
        return "unknown"

# 全局变量存储 cookie 配置
COOKIE_CONFIG = {
    'cookie_file': None,
    'parse_rate_limit': 0,  # 每分钟请求数，0 表示无限制
    'download_rate_limit': 0,  # 每秒MB数，0 表示无限制
}

class RateLimiter:
    """限速器：控制每分钟的请求数（支持动态更新限速值）"""
    def __init__(self, requests_per_minute):
        self.requests_per_minute = requests_per_minute
        self.request_times = deque()
        self.lock = threading.Lock()
    
    def update_rate(self, new_requests_per_minute):
        """动态更新限速值"""
        with self.lock:
            old_rate = self.requests_per_minute
            self.requests_per_minute = new_requests_per_minute
            if old_rate != new_requests_per_minute:
                print(f"限速器已更新: {old_rate} -> {new_requests_per_minute} requests/min")
    
    def get_rate(self):
        """获取当前限速值"""
        with self.lock:
            return self.requests_per_minute
    
    def acquire(self):
        """获取许可，如果超过限制则等待"""
        rate = self.get_rate()
        if rate <= 0:
            return  # 无限制
        
        with self.lock:
            now = time.time()
            # 移除一分钟之前的请求记录
            removed_count = 0
            while self.request_times and self.request_times[0] < now - 60:
                self.request_times.popleft()
                removed_count += 1
            
            current_count = len(self.request_times)
            
            # 如果当前分钟内的请求数已达到限制，等待
            if current_count >= rate:
                # 计算需要等待的时间（直到最早的请求超过1分钟）
                oldest_time = self.request_times[0]
                wait_time = 60 - (now - oldest_time) + 0.1  # 加0.1秒缓冲
                if wait_time > 0:
                    print(f"[限流] 当前队列长度: {current_count}, 限流值: {rate}/min, 需要等待 {wait_time:.2f} 秒 (最早请求时间: {oldest_time:.2f}, 当前时间: {now:.2f})")
                    time.sleep(wait_time)
                    # 等待后更新当前时间，并再次清理过期记录
                    now = time.time()
                    while self.request_times and self.request_times[0] < now - 60:
                        self.request_times.popleft()
                    current_count = len(self.request_times)
            
            # 记录本次请求时间（使用当前时间，确保时间戳准确）
            self.request_times.append(now)
            print(f"[限流] 允许处理，当前队列长度: {len(self.request_times)}, 限流值: {rate}/min (移除了 {removed_count} 个过期记录)")

# 全局限速器
parse_rate_limiter = None
# 限速配置检查相关
last_config_check_time = 0
config_check_interval = 60  # 每60秒检查一次配置
config_check_lock = threading.Lock()

def update_rate_limiter_from_config(config):
    """从配置中更新限速器（不更新 cookie 文件）"""
    global parse_rate_limiter, COOKIE_CONFIG
    
    parse_rate_limit = config.get('parse_rate_limit', 0)
    download_rate_limit = config.get('download_rate_limit', 0)
    
    # 更新全局配置中的限流参数
    old_parse_rate = COOKIE_CONFIG.get('parse_rate_limit', 0)
    COOKIE_CONFIG['parse_rate_limit'] = parse_rate_limit
    COOKIE_CONFIG['download_rate_limit'] = download_rate_limit
    
    # 如果限速值发生变化，更新限速器
    if old_parse_rate != parse_rate_limit:
        if parse_rate_limit > 0:
            if parse_rate_limiter is None:
                # 创建新的限速器
                parse_rate_limiter = RateLimiter(parse_rate_limit)
                print(f"限速器已创建: {parse_rate_limit} requests/min")
            else:
                # 更新现有限速器的限速值
                parse_rate_limiter.update_rate(parse_rate_limit)
        else:
            # 限速值变为0，表示无限制
            if parse_rate_limiter is not None:
                parse_rate_limiter.update_rate(0)
                print(f"限速器已更新: 无限制（0 requests/min）")
    
    return parse_rate_limit, download_rate_limit

def check_and_update_rate_limit(machine_name):
    """检查并更新限速配置（仅检查限速参数，不更新 cookie 文件）"""
    global last_config_check_time, config_check_interval
    
    current_time = time.time()
    with config_check_lock:
        # 如果距离上次检查时间不足，跳过
        if current_time - last_config_check_time < config_check_interval:
            return
        
        last_config_check_time = current_time
    
    try:
        url = f"{API_BASE_URL}/worker-cookie-configs"
        params = {"machine_name": machine_name}
        resp = requests.get(url, params=params, timeout=5)  # 使用较短的超时时间
        if resp.status_code == 200:
            config = resp.json()
            update_rate_limiter_from_config(config)
        elif resp.status_code == 404:
            # 配置不存在，保持当前限速器不变
            pass
        else:
            # 其他错误，记录但不影响运行
            pass
    except Exception as e:
        # 检查失败不影响主流程，静默处理
        pass

def get_cookie_from_backend(machine_name):
    """从后端获取 cookie 配置，包括限流参数。
    
    后端现在返回的是该机器名下所有启用的 cookie 配置（数组），
    这里会为每条配置生成一个本地临时文件，并通过简单的轮询在多 cookie 之间切换。
    """
    global COOKIE_CONFIG, parse_rate_limiter
    
    # 在进程内缓存已生成的 cookie 文件列表和轮询位置
    if not hasattr(get_cookie_from_backend, "_cookie_pool"):
        get_cookie_from_backend._cookie_pool = {}      # machine_name -> [file_path, ...]
        get_cookie_from_backend._cookie_index = {}     # machine_name -> next index
    
    pool = get_cookie_from_backend._cookie_pool
    idx_map = get_cookie_from_backend._cookie_index

    try:
        # 如果已经有缓存的 cookie 文件列表，直接做轮询选择
        if machine_name in pool and pool[machine_name]:
            files = pool[machine_name]
            idx = idx_map.get(machine_name, 0) % len(files)
            idx_map[machine_name] = idx + 1
            chosen = files[idx]
            COOKIE_CONFIG['cookie_file'] = chosen
            return chosen

        # 第一次或缓存为空时，从后端获取
        url = f"{API_BASE_URL}/worker-cookie-configs"
        params = {"machine_name": machine_name}
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            # 兼容老接口（单个对象）和新接口（数组）
            if isinstance(data, list):
                configs = data
            elif isinstance(data, dict):
                configs = [data]
            else:
                print(f"从后端获取 cookie 配置返回未知格式: {type(data)}")
                return None

            if not configs:
                print(f"后端未返回机器 '{machine_name}' 的任何 cookie 配置")
                return None

            temp_files = []
            # 使用第一条配置更新限速器（假设同一机器的多条记录限流一致）
            first_config = configs[0]

            for cfg in configs:
                cookie_content = cfg.get('cookie_content', '')
                if not cookie_content:
                    continue

                # 处理可能的 JSON 转义字符（如 \n 需要转换为真正的换行符）
                cookie_content = cookie_content.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
                cookie_content = cookie_content.strip()

                # 验证 cookie 格式（至少应该包含 Netscape 格式的特征）
                if not cookie_content.startswith('# Netscape'):
                    if '\t' in cookie_content:
                        print(f"警告: Cookie 内容缺少 Netscape 文件头，自动添加")
                        cookie_content = '# Netscape HTTP Cookie File\n# This file is generated by yt-dlp.  Do not edit.\n\n' + cookie_content
                    else:
                        print(f"警告: Cookie 内容可能不是 Netscape 格式（缺少制表符）")
                        cookie_content = '# Netscape HTTP Cookie File\n# This file is generated by yt-dlp.  Do not edit.\n\n' + cookie_content

                if not cookie_content.endswith('\n'):
                    cookie_content += '\n'

                try:
                    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8', newline='')
                    temp_file.write(cookie_content)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                    temp_file.close()
                    if os.path.exists(temp_file.name):
                        temp_files.append(temp_file.name)
                    else:
                        print(f"错误: Cookie 临时文件创建失败: {temp_file.name}")
                except Exception as e:
                    print(f"创建 Cookie 临时文件失败: {e}")

            if not temp_files:
                print(f"从后端获取 cookie 配置失败: 所有配置内容为空或写入文件失败")
                return None

            # 调试信息：显示第一份 cookie 文件的前几行
            try:
                with open(temp_files[0], 'r', encoding='utf-8') as f:
                    first_lines = ''.join(f.readlines()[:3])
                    if first_lines.strip():
                        print(f"Cookie 文件前3行预览: {repr(first_lines[:100])}")
            except Exception as e:
                print(f"警告: 无法读取 cookie 文件进行验证: {e}")

            # 更新限速器（仅使用第一条配置的限流参数）
            parse_rate_limit, download_rate_limit = update_rate_limiter_from_config(first_config)

            # 初始化或更新限速器
            if parse_rate_limit > 0:
                if parse_rate_limiter is None:
                    parse_rate_limiter = RateLimiter(parse_rate_limit)
                print(f"从后端获取到 cookie 配置 (机器名: {machine_name}, 解析限流: {parse_rate_limit} requests/min, 条数: {len(temp_files)})")
            else:
                if parse_rate_limiter is not None:
                    parse_rate_limiter.update_rate(0)
                print(f"从后端获取到 cookie 配置 (机器名: {machine_name}, 无解析限流, 条数: {len(temp_files)})")

            # 写入缓存并返回第一条
            pool[machine_name] = temp_files
            idx_map[machine_name] = 1 if len(temp_files) > 1 else 0
            chosen = temp_files[0]
            COOKIE_CONFIG['cookie_file'] = chosen
            return chosen
        elif resp.status_code == 404:
            print(f"后端未找到机器 '{machine_name}' 的启用 cookie 配置")
        else:
            print(f"从后端获取 cookie 配置失败: HTTP {resp.status_code}")
    except Exception as e:
        print(f"从后端获取 cookie 配置时出错: {e}")
    
    return None

def load_config():
    # 优先检查环境变量 CONFIG_FILE
    if env_path := os.environ.get("CONFIG_FILE"):
        if os.path.exists(env_path):
            try:
                with open(env_path, 'r') as f:
                    print(f"Loaded config from: {env_path} (from CONFIG_FILE env)")
                    return yaml.safe_load(f)
            except Exception as e:
                print(f"Error loading config from {env_path}: {e}")
        else:
            print(f"Warning: Config file specified in CONFIG_FILE not found: {env_path}")
    
    # Try multiple paths to find config.yaml or config_back_local.yaml
    paths = ["../../config_back_local.yaml", "../../config.yaml", "../config_back_local.yaml", "../config.yaml", "config_back_local.yaml", "config.yaml"]
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r') as f:
                    print(f"Loaded config from: {p}")
                    return yaml.safe_load(f)
            except Exception as e:
                print(f"Error loading config from {p}: {e}")
    print("Warning: config.yaml or config_back_local.yaml not found")
    return {}

def api_acquire_tasks(limit=10):
    try:
        url = f"{API_BASE_URL}/tasks/acquire"
        machine_name = get_machine_name()
        payload = {"worker_id": WORKER_ID, "limit": limit, "stage": "metadata", "machine_name": machine_name}
        print(f"  [API] Calling {url} with limit={limit}, worker_id={WORKER_ID}, machine_name={machine_name}")
        resp = requests.post(url, json=payload, timeout=10)
        print(f"  [API] Response status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                print(f"  [API] Received {len(data)} tasks")
                if len(data) > 0:
                    for t in data:
                        task_id = t.get('id')
                        video_id = t.get('video_id', '')
                        status = t.get('status', '')
                        url_preview = t.get('url', '')[:50]
                        print(f"    - Task {task_id}: video_id='{video_id}', status={status}, url={url_preview}")
                return data
            print(f"  [API] Unexpected response format: {type(data)}, content: {data}")
            return []
        elif resp.status_code == 404:
            print(f"  [API] 404 - No tasks available")
            return []
        else:
            print(f"  [API] Error {resp.status_code}: {resp.text}")
            return []
    except Exception as e:
        print(f"  [API] Exception: {e}")
        import traceback
        traceback.print_exc()
        return []

def api_update_task_batch(updates):
    try:
        if not updates:
            return
        
        # 记录要更新的任务信息
        print(f"  [API] Updating {len(updates)} tasks...")
        for u in updates:
            task_id = u.get('id')
            status = u.get('status', '')
            video_id = u.get('video_id', '')
            has_audio = bool(u.get('audio_url'))
            has_video = bool(u.get('video_url'))
            title = u.get('title', '')[:40] if u.get('title') else ''
            print(f"    - Task {task_id}: status={status}, video_id='{video_id}', audio={has_audio}, video={has_video}, title='{title}'")
        
        # 添加 machine_name 到请求中，这样后端可以将任务推入到对应机器的下载队列
        machine_name = get_machine_name()
        payload = {
            "updates": updates,
            "machine_name": machine_name
        }
        resp = requests.post(f"{API_BASE_URL}/tasks/update", json=payload, timeout=10)
        print(f"  [API] Update response: status={resp.status_code}")
        if resp.status_code != 200:
            print(f"  [API] Update failed: {resp.text}")
        else:
            print(f"  [API] ✓ Successfully updated {len(updates)} tasks")
    except Exception as e:
        print(f"  [API] Exception updating tasks: {e}")
        import traceback
        traceback.print_exc()

def api_save_task_record(task_data):
    """保存任务记录到数据库（使用 job_id + id 作为唯一索引）"""
    try:
        url = f"{API_BASE_URL}/youtube-tasks/update"
        resp = requests.post(url, json=task_data, timeout=10)
        if resp.status_code == 200 or resp.status_code == 201:
            return True
        else:
            print(f"  [API] Failed to save task record: HTTP {resp.status_code}, {resp.text}")
            return False
    except Exception as e:
        print(f"  [API] Exception saving task record: {e}")
        import traceback
        traceback.print_exc()
        return False

# R2 客户端缓存
_r2_client_cache = None
_r2_bucket_cache = None

def get_r2_client():
    """获取 R2 客户端（从配置中读取，带缓存）"""
    global _r2_client_cache, _r2_bucket_cache
    
    # 如果已缓存，直接返回
    if _r2_client_cache is not None:
        return _r2_client_cache, _r2_bucket_cache
    
    config = load_config()
    storage = config.get('storage', {})
    src = storage.get('src', {})
    
    endpoint = src.get('endpoint')
    access_key = src.get('access_key')
    secret_key = src.get('secret_key')
    
    if not all([endpoint, access_key, secret_key]):
        return None, None
    
    try:
        # 从 endpoint 中提取 bucket 名称（如果包含在路径中）
        # 例如: https://account.r2.cloudflarestorage.com/bucket-name
        parsed = urlparse(endpoint)
        bucket_name = parsed.path.strip('/')
        
        # 构建基础 endpoint（不包含 bucket 路径）
        base_endpoint = f"{parsed.scheme}://{parsed.netloc}"
        
        # 创建 S3 客户端（R2 兼容 S3 API）
        s3_client = boto3.client(
            's3',
            endpoint_url=base_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto'
        )
        
        # 缓存客户端和 bucket 名称
        _r2_client_cache = s3_client
        _r2_bucket_cache = bucket_name if bucket_name else None
        
        return s3_client, _r2_bucket_cache
    except Exception as e:
        print(f"  ⚠ 创建 R2 客户端失败: {e}")
        return None, None

def check_r2_object_exists(key):
    """检查 R2 中是否存在指定的对象"""
    try:
        s3_client, bucket_name = get_r2_client()
        if not s3_client or not bucket_name:
            return False
        
        # 使用 HeadObject 检查对象是否存在
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            return False
        # 其他错误（如权限问题）也返回 False，但不打印（避免日志过多）
        return False
    except Exception as e:
        # 静默处理错误，避免日志过多
        return False

def build_r2_key(r2_prefix, video_id, download_mode, file_type, job_info):
    """
    构建 R2 存储的 key（文件路径）
    与 Go 代码中的 generateFilename 逻辑保持一致
    
    参数:
        r2_prefix: R2 前缀路径
        video_id: YouTube 视频 ID
        download_mode: 'both', 'audio', 'video'
        file_type: 'audio' 或 'video'
        job_info: Job 信息（包含 filename_template, audio_extension, video_extension）
    
    返回:
        str: R2 key（文件路径）
    """
    # 获取文件扩展名
    if file_type == 'audio':
        ext = job_info.get('audio_extension', 'm4a')
    else:  # video
        ext = job_info.get('video_extension', 'mp4')
    
    # 检查是否有自定义文件名模板
    filename_template = job_info.get('filename_template', '')
    
    if filename_template:
        # 使用自定义模板（类似 Go 代码中的 generateFilename）
        from datetime import datetime
        import re
        filename = filename_template
        
        # 1. 替换日期变量 $(date +FORMAT)
        while True:
            match = re.search(r'\$\(date\s*\+([^)]+)\)', filename)
            if not match:
                break
            
            format_str = match.group(1)  # 例如: %Y%m%d_%H
            # 将 strftime 格式转换为 Python datetime 格式
            # %Y -> %Y, %m -> %m, %d -> %d, %H -> %H, %M -> %M, %S -> %S
            python_format = format_str  # Python 和 strftime 格式相同
            
            time_str = datetime.now().strftime(python_format)
            # 替换整个匹配项
            filename = filename[:match.start()] + time_str + filename[match.end():]
        
        # 2. 替换变量
        filename = filename.replace('%(id)', video_id)
        filename = filename.replace('%(ext)', ext)
        # title 暂时使用 video_id（如果需要 title，需要从任务中获取）
        # 注意：这里 title 可能为空，使用 video_id 作为占位符
        title = video_id  # 简化处理，实际应该从任务中获取
        # 简单的 title 清理（移除特殊字符）
        safe_title = re.sub(r'[^\w\s-]', '_', title).strip('_')
        filename = filename.replace('%(title)', safe_title)
        
        # 3. 移除开头的 /
        filename = filename.lstrip('/')
        
        # 4. 拼接 prefix
        prefix = r2_prefix.rstrip('/') + '/' if r2_prefix else ''
        return prefix + filename
    else:
        # 默认格式: prefix + video_id + "_audio.ext" 或 "_video.ext"
        prefix = r2_prefix.rstrip('/') + '/' if r2_prefix else ''
        suffix = f"_{file_type}.{ext}"
        return prefix + video_id + suffix

def validate_and_get_final_url(format_obj):
    """
    验证格式并获取最终的 CDN URL（与 get_youtube.sh 的 --get-url 行为一致）
    使用 GET 请求处理所有重定向（包括 302），确保获取最终 URL 和完整的签名参数
    关键：使用 Session 和最大重定向次数，确保跟随所有重定向到最终 CDN URL
    """
    if not format_obj:
        return None
    
    url = format_obj.get('url')
    if not url:
        return None
    
    # 验证 URL 格式（与 get_youtube.sh 的过滤逻辑一致）
    # 排除 QUIC
    if url.startswith('quic://') or 'quic://' in url:
        print(f"  ⚠ 跳过 QUIC 协议格式: {format_obj.get('format_id')}")
        return None
    
    # 排除 HLS
    if '.m3u8' in url or format_obj.get('ext') == 'm3u8':
        print(f"  ⚠ 跳过 HLS 格式: {format_obj.get('format_id')}")
        return None
    
    # 必须是 HTTPS
    if not url.startswith('https://'):
        print(f"  ⚠ 跳过非 HTTPS 格式: {format_obj.get('format_id')}, URL: {url[:50]}...")
        return None
    
    # 必须是 googlevideo.com（YouTube CDN）
    if 'googlevideo.com' not in url:
        print(f"  ⚠ 跳过非 googlevideo.com URL: {format_obj.get('format_id')}, URL: {url[:50]}...")
        return None
    
    # 使用 Session 和 GET 请求的 Range 方法获取最终 URL（处理所有重定向，包括 302）
    # 这与 --get-url 的行为一致，确保获取的是最终可访问的 URL，包含完整的签名参数
    # 关键：使用 Session 可以更好地处理重定向链，确保获取最终 URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.youtube.com/',
        'Range': 'bytes=0-0',  # 只请求第一个字节，验证 URL 有效性并获取最终 URL（包含完整签名）
    }
    
    try:
        # 使用 Session 来更好地处理重定向链
        session = requests.Session()
        session.max_redirects = 10  # 允许最多 10 次重定向
        
        # 使用 GET 请求的 Range 方法（类似 --get-url，确保获取完整签名）
        # 使用 stream=True 和 Range 请求，只获取响应头，不下载内容
        # 关键：使用 Range 请求可以触发 CDN 的完整 URL 生成（包含所有签名参数）
        # 并且会自动跟随所有重定向（包括 302）到最终 CDN URL
        response = session.get(url, headers=headers, allow_redirects=True, timeout=15, stream=True)
        final_url = response.url
        
        # 立即关闭连接，不读取内容（只获取响应头）
        response.close()
        session.close()
        
        # 如果最终 URL 与原始 URL 不同，说明有重定向
        if final_url != url:
            print(f"  ℹ 格式 {format_obj.get('format_id')}: URL 重定向")
            print(f"     原始: {url[:80]}...")
            print(f"     最终: {final_url[:80]}...")
        
        # 验证最终 URL 仍然是有效的 CDN URL
        if 'googlevideo.com' not in final_url:
            print(f"  ⚠ 重定向后的 URL 不是 googlevideo.com: {final_url[:50]}...")
            return None
        
        # 返回最终 URL（包含完整的签名参数，已经过所有重定向）
        return final_url
        
    except requests.exceptions.TooManyRedirects as e:
        # 如果重定向次数过多，可能是循环重定向
        print(f"  ⚠ 重定向次数过多: {e}, 使用原始 URL")
        return url
    except requests.exceptions.RequestException as e:
        # 如果请求失败，可能是网络问题，但 URL 本身可能是有效的
        # 在这种情况下，返回原始 URL（让下载服务处理）
        print(f"  ⚠ 获取最终 URL 失败: {e}, 使用原始 URL")
        return url
    except Exception as e:
        print(f"  ⚠ 获取最终 URL 失败: {e}, 使用原始 URL")
        return url

@TASK_DURATION.time()
def process_metadata(task, ydl_opts):
    global parse_rate_limiter
    
    url = task.get('url')
    if not url:
        TASKS_PROCESSED.labels(status="failed").inc()
        return None
    
    # 为当前任务选择要使用的 cookies 文件（支持环境变量优先，其次从后端轮询获取）
    cookies_file_env = os.environ.get('YOUTUBE_COOKIES_FILE')
    cookiefile = None
    if cookies_file_env and os.path.exists(cookies_file_env):
        # 显式指定的本地 cookies 文件（优先，完全跳过后端）
        cookiefile = cookies_file_env
    else:
        # 通过后端获取当前应使用的 cookies（后端可以根据数据库中多条记录实现轮询/负载均衡）
        machine_name_for_cookie = get_machine_name()
        cookie_path = get_cookie_from_backend(machine_name_for_cookie)
        if cookie_path:
            # get_cookie_from_backend 已经把实际文件路径写入 COOKIE_CONFIG['cookie_file']
            cookiefile = COOKIE_CONFIG.get('cookie_file') or cookie_path
    
    # 为当前任务构建独立的 yt-dlp 配置，避免不同任务之间互相覆盖 cookie 配置
    local_ydl_opts = ydl_opts.copy()
    if cookiefile:
        local_ydl_opts['cookiefile'] = cookiefile
    
    # 在处理任务前，检查并更新限速配置（如果距离上次检查时间足够长）
    machine_name = get_machine_name()
    check_and_update_rate_limit(machine_name)
    
    task_id = task.get('id')
    old_video_id = task.get('video_id', '')
    old_audio_url = task.get('audio_url', '')
    old_video_url = task.get('video_url', '')
    
    # 判断是否来自重试队列（通过检查是否有旧的 video_id 或 URL）
    is_retry = (not old_video_id or old_video_id == '') and (not old_audio_url and not old_video_url)
    
    if is_retry:
        print(f"[RETRY] Processing Task {task_id} from retry queue: {url}")
        print(f"  Previous state: video_id='{old_video_id}', has_audio_url={bool(old_audio_url)}, has_video_url={bool(old_video_url)}")
    else:
        print(f"Processing Task {task_id}: {url}")
        if old_video_id:
            print(f"  Previous video_id: {old_video_id}")
    
    job_info = get_job_info(task['job_id'])
    download_mode = job_info.get('download_mode', 'both') # both, audio, video
    video_strategy = job_info.get('video_selection_strategy', 'highest_quality') # highest_quality, hd_priority, ultra_priority, min_1080p
    r2_prefix = job_info.get('r2_prefix', '')

    # 先快速提取 video_id，用于检查 R2 中是否已存在
    # 注意：这里不使用限流器，因为只是快速检查，不涉及 YouTube API 调用
    video_id_for_check = old_video_id
    if not video_id_for_check:
        try:
            # 使用轻量级提取，只获取 video_id
            # 注意：这里需要调用 YouTube API，但因为是快速检查，暂时不计入限流
            # 如果后续需要限流，可以在这里添加限流器调用
            quick_opts = local_ydl_opts.copy()
            quick_opts['quiet'] = True
            quick_opts['no_warnings'] = True
            with yt_dlp.YoutubeDL(quick_opts) as quick_ydl:
                quick_info = quick_ydl.extract_info(url, download=False)
                video_id_for_check = quick_info.get('id', '')
                if video_id_for_check:
                    print(f"  ✓ 快速提取 video_id: {video_id_for_check}")
        except Exception as e:
            print(f"  ⚠ 快速提取 video_id 失败: {e}，将继续正常流程")
            video_id_for_check = None
    
    # 如果获取到了 video_id，检查 R2 中是否已存在
    if video_id_for_check and r2_prefix:
        print(f"  Checking R2 for existing objects (video_id: {video_id_for_check}, prefix: {r2_prefix})...")
        all_exist = True
        
        # 根据 download_mode 检查需要的文件
        if download_mode in ['both', 'audio']:
            audio_key = build_r2_key(r2_prefix, video_id_for_check, download_mode, 'audio', job_info)
            audio_exists = check_r2_object_exists(audio_key)
            if audio_exists:
                print(f"  ✓ Audio 文件已存在: {audio_key}")
            else:
                print(f"  - Audio 文件不存在: {audio_key}")
                all_exist = False
        
        if download_mode in ['both', 'video']:
            video_key = build_r2_key(r2_prefix, video_id_for_check, download_mode, 'video', job_info)
            video_exists = check_r2_object_exists(video_key)
            if video_exists:
                print(f"  ✓ Video 文件已存在: {video_key}")
            else:
                print(f"  - Video 文件不存在: {video_key}")
                all_exist = False
        
        # 如果所有需要的文件都已存在，直接返回 COMPLETED 状态（不调用限流器）
        if all_exist:
            print(f"  ✓ 所有文件已存在于 R2，跳过元信息提取，直接标记为完成（不计入限流）")
            result = {
                "id": task['id'],
                "job_id": task['job_id'],
                "status": "COMPLETED",  # 直接标记为完成
                "worker_id": WORKER_ID,
                "video_id": video_id_for_check,
                "updated_at": format_time_go_compatible(),
                "completed_at": format_time_go_compatible()
            }
            # 保存任务记录到数据库
            try:
                task_record = {
                    "id": result.get("id"),
                    "job_id": result.get("job_id"),
                    "status": result.get("status"),
                    "worker_id": result.get("worker_id"),
                    "video_id": result.get("video_id", ""),
                }
                api_save_task_record(task_record)
            except Exception as e:
                print(f"  ⚠ 保存任务记录到数据库失败: {e}")
            TASKS_PROCESSED.labels(status="skipped").inc()
            return result
    
    # 只有在需要实际处理任务时，才应用解析限流器（在开始处理前等待）
    if parse_rate_limiter:
        parse_rate_limiter.acquire()

    result = {
        "id": task['id'],
        "job_id": task['job_id'],
        "status": "METADATA_FETCHED", # Success state for this worker
        "worker_id": WORKER_ID,
        "updated_at": format_time_go_compatible()  # 使用与 Go 兼容的时间格式
    }
    
    try:
        with yt_dlp.YoutubeDL(local_ydl_opts) as ydl:
            # 使用 extract_info 获取完整信息（类似 --dump-json）
            # 不指定 format，让 yt-dlp 自动选择最佳客户端和格式
            print(f"  Extracting metadata from YouTube...")
            info = ydl.extract_info(url, download=False)
            
            result["title"] = info.get('title', '')
            result["video_id"] = info.get('id', '')
            
            # 记录 video_id 的获取结果
            if result["video_id"]:
                if is_retry:
                    print(f"  ✓ [RETRY SUCCESS] Extracted video_id: {result['video_id']} (was empty before)")
                else:
                    print(f"  ✓ Extracted video_id: {result['video_id']}")
                if result["title"]:
                    print(f"  ✓ Title: {result['title'][:60]}")
            else:
                print(f"  ✗ Failed to extract video_id from URL")
            
            formats = info.get('formats', [])
            
            # 调试：显示格式统计（仅在测试模式）
            if not ydl_opts.get('quiet', True):
                print(f"  找到 {len(formats)} 个格式")
                formats_with_url = [f for f in formats if f.get('url')]
                formats_without_url = [f for f in formats if not f.get('url')]
                print(f"  有 URL 的格式: {len(formats_with_url)}, 无 URL 的格式: {len(formats_without_url)}")
                
                # 显示格式类型统计
                audio_count = len([f for f in formats if f.get('vcodec') == 'none' and f.get('acodec') != 'none'])
                video_count = len([f for f in formats if f.get('vcodec') != 'none' and f.get('acodec') == 'none'])
                mixed_count = len([f for f in formats if f.get('vcodec') != 'none' and f.get('acodec') != 'none'])
                print(f"  音频格式: {audio_count}, 视频格式: {video_count}, 混合格式: {mixed_count}")
            
            # 辅助函数：获取格式的 URL
            def get_format_url(format_obj):
                """获取格式的 URL"""
                return format_obj.get('url')
            
            # Find best audio（先获取所有音频格式，再过滤）
            best_audio = None
            if download_mode in ['both', 'audio']:
                # 第一步：获取所有音频格式（不限制 URL）
                all_audio_formats = [f for f in reversed(formats) 
                                     if f.get('vcodec') == 'none' and f.get('acodec') != 'none']
                
                # 第二步：尝试获取 URL 并过滤
                audio_formats = []
                for f in all_audio_formats:
                    url = get_format_url(f)
                    if url and not url.startswith('quic://') and '.m3u8' not in url and url.startswith('https://') and 'googlevideo.com' in url:
                        # 临时设置 URL 以便后续使用
                        f['url'] = url
                        audio_formats.append(f)
                
                if audio_formats:
                    best_audio = next((f for f in audio_formats if f.get('language') == 'en'), None)
                    if not best_audio:
                        best_audio = audio_formats[0]
            
            # Find best video（先获取所有视频格式，再过滤）
            best_video = None
            if download_mode in ['both', 'video']:
                # 第一步：获取所有视频格式（不限制 URL）
                all_video_candidates = [f for f in reversed(formats) 
                                       if f.get('vcodec') != 'none' and f.get('acodec') == 'none']
                
                # 第二步：尝试获取 URL 并过滤
                video_candidates = []
                skipped_no_url = 0
                skipped_invalid_url = 0
                for f in all_video_candidates:
                    url = get_format_url(f)
                    if not url:
                        skipped_no_url += 1
                        continue
                    if url.startswith('quic://') or '.m3u8' in url or not url.startswith('https://') or 'googlevideo.com' not in url:
                        skipped_invalid_url += 1
                        continue
                    # 临时设置 URL 以便后续使用
                    f['url'] = url
                    video_candidates.append(f)
                
                # 调试日志：显示格式统计
                if video_strategy == 'min_1080p':
                    print(f"  [DEBUG] Video format analysis for min_1080p strategy:")
                    print(f"    Total video formats: {len(all_video_candidates)}")
                    print(f"    Formats with valid URL: {len(video_candidates)}")
                    print(f"    Formats without URL: {skipped_no_url}")
                    print(f"    Formats with invalid URL: {skipped_invalid_url}")
                    if video_candidates:
                        heights = [f.get('height', 0) for f in video_candidates]
                        print(f"    Available heights: {sorted(set(heights), reverse=True)}")
                        min_1080p_count = len([h for h in heights if h >= 1080])
                        print(f"    Formats >= 1080p: {min_1080p_count}")
                    else:
                        print(f"    ⚠ WARNING: No video formats with valid URL found!")
                        # 如果 video_candidates 为空，尝试打印所有格式的信息用于调试
                        if all_video_candidates:
                            print(f"    [DEBUG] All video formats (first 5):")
                            for f in all_video_candidates[:5]:
                                fmt_id = f.get('format_id', 'unknown')
                                height = f.get('height', 'N/A')
                                width = f.get('width', 'N/A')
                                url_preview = get_format_url(f)
                                has_url = 'Yes' if url_preview else 'No'
                                print(f"      Format {fmt_id}: {width}x{height}, URL: {has_url}")
                
                if video_strategy == 'hd_priority':
                    # 优先级策略：1080P > 720P > 1080P+
                    # 1. 检查最高画质是否满足 720P
                    if not any(f.get('height', 0) >= 720 for f in video_candidates):
                        raise Exception("Video quality too low (max height < 720P). Skipping download.")

                    # 策略 A: 优先找 1080P
                    best_video = next((f for f in video_candidates if f.get('height') == 1080), None)
                    
                    # 策略 B: 如果没有 1080P，找 720P
                    if not best_video:
                        best_video = next((f for f in video_candidates if f.get('height') == 720), None)
                    
                    # 策略 C: 如果 1080P 和 720P 都没有，找 > 1080P 的 (例如 2K, 4K)
                    if not best_video:
                        best_video = next((f for f in video_candidates if f.get('height', 0) > 1080), None)

                    # 策略 D (保底): 如果还没选到，选现存最高的 >= 720
                    if not best_video:
                         best_video = next((f for f in video_candidates if f.get('height', 0) >= 720), None)
                elif video_strategy == 'ultra_priority':
                    # 优先级策略：1080P+ > 1080P
                    # 1. 检查是否有 > 1080P 的格式（2K, 4K 等）
                    ultra_formats = [f for f in video_candidates if f.get('height', 0) > 1080]
                    if ultra_formats:
                        # 优先选择 > 1080P 的最高画质
                        best_video = ultra_formats[0]  # Already sorted by height descending
                    else:
                        # 如果没有 > 1080P，选择 1080P
                        best_video = next((f for f in video_candidates if f.get('height') == 1080), None)
                        # 如果连 1080P 都没有，选择最高画质
                        if not best_video and video_candidates:
                            best_video = video_candidates[0]
                elif video_strategy == 'min_1080p':
                    # 优先级策略：最小 >= 1080P，没有就不下载
                    # 1. 检查是否有 >= 1080P 的格式
                    min_1080p_formats = [f for f in video_candidates if f.get('height', 0) >= 1080]
                    if not min_1080p_formats:
                        # 详细错误信息，帮助调试
                        error_msg = "No video format >= 1080P available. Skipping download."
                        if not video_candidates:
                            error_msg += " No video formats with valid URL found. This may be due to YouTube SABR streaming or missing cookies."
                        else:
                            available_heights = sorted(set([f.get('height', 0) for f in video_candidates]), reverse=True)
                            max_height = max([f.get('height', 0) for f in video_candidates]) if video_candidates else 0
                            error_msg += f" Maximum available height: {max_height}p. Available heights: {available_heights}."
                        
                        # 检查所有格式（包括没有 URL 的），看看是否有 1080p 格式但被过滤了
                        all_1080p_formats = [f for f in all_video_candidates if f.get('height', 0) >= 1080]
                        if all_1080p_formats:
                            print(f"  [DEBUG] Found {len(all_1080p_formats)} formats >= 1080p in all formats, but none have valid URL:")
                            for f in all_1080p_formats[:3]:  # 只显示前3个
                                fmt_id = f.get('format_id', 'unknown')
                                height = f.get('height', 'N/A')
                                url_preview = get_format_url(f)
                                print(f"    Format {fmt_id} ({height}p): URL={'Yes' if url_preview else 'No'}")
                        
                        raise Exception(error_msg)
                    
                    # 2. 选择 >= 1080P 的最低画质（最小但满足要求）
                    # 按高度升序排列，选择第一个（最低的 >= 1080P）
                    min_1080p_formats_sorted = sorted(min_1080p_formats, key=lambda f: f.get('height', 0))
                    best_video = min_1080p_formats_sorted[0]
                    print(f"  [DEBUG] Selected video format: {best_video.get('format_id')}, height: {best_video.get('height')}p, width: {best_video.get('width', 'N/A')}")
                else:
                    # Default: highest_quality - 选择最高画质
                    if video_candidates:
                        best_video = video_candidates[0] # Already reversed, so first is highest
            
            # 使用 validate_and_get_final_url 获取最终 CDN URL（与 get_youtube.sh 一致）
            print(f"  Generating download URLs...")
            if best_audio:
                print(f"    Validating audio URL (format {best_audio.get('format_id')})...")
                final_audio_url = validate_and_get_final_url(best_audio)
                if final_audio_url:
                    result["audio_url"] = final_audio_url
                    result["audio_size"] = best_audio.get('filesize') or best_audio.get('filesize_approx')
                    audio_abr = best_audio.get('abr', 'N/A')
                    if is_retry:
                        print(f"  ✓ [RETRY SUCCESS] 音频 URL 已生成: 格式 {best_audio.get('format_id')}, 码率: {audio_abr}kbps, 大小: {result['audio_size']}")
                    else:
                        print(f"  ✓ 音频 URL: 格式 {best_audio.get('format_id')}, 码率: {audio_abr}kbps, 大小: {result['audio_size']}")
                else:
                    print(f"  ✗ 音频格式 {best_audio.get('format_id')} URL 验证失败")
            else:
                if download_mode in ['both', 'audio']:
                    print(f"  ⚠ 未找到可用的音频格式")
            
            if best_video:
                print(f"    Validating video URL (format {best_video.get('format_id')})...")
                final_video_url = validate_and_get_final_url(best_video)
                if final_video_url:
                    result["video_url"] = final_video_url
                    result["video_size"] = best_video.get('filesize') or best_video.get('filesize_approx')
                    video_height = best_video.get('height', 'N/A')
                    video_width = best_video.get('width', 'N/A')
                    if is_retry:
                        print(f"  ✓ [RETRY SUCCESS] 视频 URL 已生成: 格式 {best_video.get('format_id')}, 画质: {video_height}p ({video_width}x{video_height}), 大小: {result['video_size']}")
                    else:
                        print(f"  ✓ 视频 URL: 格式 {best_video.get('format_id')}, 画质: {video_height}p ({video_width}x{video_height}), 大小: {result['video_size']}")
                else:
                    print(f"  ✗ 视频格式 {best_video.get('format_id')} URL 验证失败")
            else:
                if download_mode in ['both', 'video']:
                    print(f"  ⚠ 未找到可用的视频格式")

            # Fallback: 如果没有找到分离的音频/视频格式，尝试使用混合格式
            if not best_audio and not best_video:
                # 查找混合格式（同时包含音频和视频）
                all_mixed_formats = [f for f in reversed(formats)
                                    if f.get('vcodec') != 'none' and f.get('acodec') != 'none']
                
                # 过滤混合格式
                mixed_formats = []
                for f in all_mixed_formats:
                    url = get_format_url(f)
                    if url and not url.startswith('quic://') and '.m3u8' not in url and url.startswith('https://') and 'googlevideo.com' in url:
                        f['url'] = url
                        mixed_formats.append(f)
                
                if mixed_formats:
                    # 优先选择格式 18（360p MP4，音画混合，与 get_youtube.sh 一致）
                    format_18 = next((f for f in mixed_formats if f.get('format_id') == '18'), None)
                    if format_18:
                        best_mixed = format_18
                    else:
                        # 如果没有格式 18，选择第一个混合格式
                        best_mixed = mixed_formats[0]
                    
                    # 混合格式同时作为音频和视频
                    final_mixed_url = validate_and_get_final_url(best_mixed)
                    if final_mixed_url:
                        result["audio_url"] = final_mixed_url
                        result["video_url"] = final_mixed_url
                        result["audio_size"] = best_mixed.get('filesize') or best_mixed.get('filesize_approx')
                        result["video_size"] = best_mixed.get('filesize') or best_mixed.get('filesize_approx')
                        print(f"  ✓ 混合格式 URL: 格式 {best_mixed.get('format_id')}, 大小: {result['audio_size']}")
                    else:
                        print(f"  ✗ 混合格式 {best_mixed.get('format_id')} URL 验证失败")
        
        # 总结处理结果
        has_audio_url = bool(result.get("audio_url"))
        has_video_url = bool(result.get("video_url"))
        
        if not has_audio_url and not has_video_url:
            result["status"] = "FAILED"
            result["error_message"] = "No video or audio URL found"
            result["is_download_fail"] = True
            now_time = format_time_go_compatible()
            result["updated_at"] = now_time  # 更新失败时也更新时间
            result["completed_at"] = now_time  # 失败时设置 completed_at
            if is_retry:
                print(f"  ✗ [RETRY FAILED] Task {task_id}: 未能生成任何下载 URL")
            else:
                print(f"  ✗ Task {task_id}: 未能生成任何下载 URL")
            TASKS_PROCESSED.labels(status="failed").inc()
        else:
            result["updated_at"] = format_time_go_compatible()  # 成功时更新时间（不设置 completed_at，因为状态是 METADATA_FETCHED，不是 COMPLETED）
            if is_retry:
                print(f"  ✓ [RETRY SUCCESS] Task {task_id}: 成功生成下载 URL (audio: {has_audio_url}, video: {has_video_url})")
                print(f"    video_id: '{result.get('video_id')}' -> '{result.get('video_id')}'")
            else:
                print(f"  ✓ Task {task_id}: 成功生成下载 URL (audio: {has_audio_url}, video: {has_video_url})")
            TASKS_PROCESSED.labels(status="success").inc()

    except Exception as e:
        print(f"Error processing {url}: {e}")
        result["status"] = "FAILED"
        result["error_message"] = str(e)
        now_time = format_time_go_compatible()
        result["updated_at"] = now_time  # 异常时也更新时间
        result["completed_at"] = now_time  # 异常失败时设置 completed_at
        if "Sign in to confirm you're not a bot" in str(e):
            result["is_download_fail"] = True
        TASKS_PROCESSED.labels(status="failed").inc()
    
    # 保存任务记录到数据库（使用 job_id + id 作为唯一索引）
    try:
        # 准备要保存的数据（只包含数据库表需要的字段）
        task_record = {
            "id": result.get("id"),
            "job_id": result.get("job_id"),
            "status": result.get("status"),
            "worker_id": result.get("worker_id"),
            "title": result.get("title", ""),
            "video_id": result.get("video_id", ""),
            "audio_url": result.get("audio_url", ""),
            "audio_size": result.get("audio_size", 0),
            "video_url": result.get("video_url", ""),
            "video_size": result.get("video_size", 0),
            "error_message": result.get("error_message", ""),
        }
        api_save_task_record(task_record)
    except Exception as e:
        print(f"  ⚠ 保存任务记录到数据库失败: {e}")
        
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
    
    # Cookies 支持优先级（强制自定义 > 后端下发 > 本地文件 > 浏览器 > 无 cookies）
    cookies_browser = os.environ.get('YOUTUBE_COOKIES_BROWSER')
    cookies_file_env = os.environ.get('YOUTUBE_COOKIES_FILE')
    no_cookies = os.environ.get('YOUTUBE_NO_COOKIES') == '1'
    
    # 1. 明确禁止使用 cookies
    if no_cookies:
        print("不使用 cookies，使用 Android/iOS/Web 客户端模式")
        ydl_opts['extractor_args'] = {
            'youtube': {
                'player_client': ['android', 'ios', 'web']
            }
        }
    # 2. 如果显式指定了 YOUTUBE_COOKIES_FILE，则优先使用该文件，并跳过后端下发的 cookie
    elif cookies_file_env and os.path.exists(cookies_file_env):
        ydl_opts['cookiefile'] = cookies_file_env
        print(f"使用 cookies 文件 (环境变量优先，跳过后端下发): {cookies_file_env}")
        # 不指定 player_client，让 yt-dlp 自动选择最佳客户端（类似 --dump-json 的行为）
    else:
        # 3. 没有显式指定本地 cookie 文件时，才尝试从后端获取 cookie 配置
        machine_name = get_machine_name()
        cookie_file_from_backend = get_cookie_from_backend(machine_name)
        
        # 4. 本地 cookie.txt 路径：当前目录或脚本目录
        cookie_file_paths = [
            "cookie.txt",
            "backend/worker_downloader/cookie.txt",
            os.path.join(os.path.dirname(__file__), "cookie.txt"),
        ]
        cookie_file = None
        if not cookie_file_from_backend:
            for path in cookie_file_paths:
                if os.path.exists(path):
                    cookie_file = path
                    break
        
        if cookie_file_from_backend:
            # 使用从后端获取的 cookie 文件（已在 get_cookie_from_backend 中设置到 COOKIE_CONFIG）
            ydl_opts['cookiefile'] = COOKIE_CONFIG['cookie_file']
            if COOKIE_CONFIG['parse_rate_limit'] > 0:
                print(f"使用从后端获取的 cookies 文件: {COOKIE_CONFIG['cookie_file']} (解析限流: {COOKIE_CONFIG['parse_rate_limit']} requests/min)")
            else:
                print(f"使用从后端获取的 cookies 文件: {COOKIE_CONFIG['cookie_file']}")
            # 不指定 player_client，让 yt-dlp 自动选择最佳客户端（类似 --dump-json 的行为）
        elif cookie_file:
            # 使用本地 cookie.txt 文件
            ydl_opts['cookiefile'] = cookie_file
            print(f"使用 cookies 文件: {cookie_file}")
            # 不指定 player_client，让 yt-dlp 自动选择最佳客户端（类似 --dump-json 的行为）
        elif cookies_browser:
            ydl_opts['cookiesfrombrowser'] = (cookies_browser,)
            print(f"从浏览器读取 cookies: {cookies_browser}")
            # 不指定 player_client，让 yt-dlp 自动选择最佳客户端
        else:
            # 自动尝试从浏览器读取 cookies
            try:
                test_ydl = yt_dlp.YoutubeDL({'cookiesfrombrowser': ('chrome',), 'quiet': True})
                test_ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
                ydl_opts['cookiesfrombrowser'] = ('chrome',)
                print("自动使用 Chrome cookies")
                # 不指定 player_client，让 yt-dlp 自动选择最佳客户端
            except:
                try:
                    test_ydl = yt_dlp.YoutubeDL({'cookiesfrombrowser': ('safari',), 'quiet': True})
                    test_ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
                    ydl_opts['cookiesfrombrowser'] = ('safari',)
                    print("自动使用 Safari cookies")
                    # 不指定 player_client，让 yt-dlp 自动选择最佳客户端
                except:
                    try:
                        test_ydl = yt_dlp.YoutubeDL({'cookiesfrombrowser': ('firefox',), 'quiet': True})
                        test_ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
                        ydl_opts['cookiesfrombrowser'] = ('firefox',)
                        print("自动使用 Firefox cookies")
                        # 不指定 player_client，让 yt-dlp 自动选择最佳客户端
                    except:
                        # 如果没有 cookies，使用 Android/iOS/Web 客户端（无需 cookies）
                        print("未找到浏览器 cookies，使用 Android/iOS/Web 客户端模式（无需 cookies）")
                        ydl_opts['extractor_args'] = {
                            'youtube': {
                                'player_client': ['android', 'ios', 'web']
                            }
                        }

    # 从配置文件或环境变量读取 max_workers，默认值为 2（metadata 任务不需要太多并发）
    max_workers = 2
    worker_config = config.get('worker', {})
    if worker_config.get('metadata_max_threads'):
        max_workers = worker_config['metadata_max_threads']
    elif os.environ.get('METADATA_MAX_THREADS'):
        try:
            max_workers = int(os.environ.get('METADATA_MAX_THREADS'))
        except ValueError:
            print(f"Warning: Invalid METADATA_MAX_THREADS value, using default 2")
    elif os.environ.get('MAX_THREADS'):
        try:
            max_workers = int(os.environ.get('MAX_THREADS'))
        except ValueError:
            print(f"Warning: Invalid MAX_THREADS value, using default 2")
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    print(f"Metadata Worker {WORKER_ID} started with {max_workers} workers.")
    
    futures = set()
    pending_updates = []
    last_flush_time = time.time()
    
    # Threshold to fetch new tasks (avoid spamming acquire for 1 task)
    # For small worker counts, use a lower threshold
    FETCH_THRESHOLD = min(50, max_workers)
    
    # Immediately try to fetch tasks on startup
    print("Attempting to acquire tasks on startup...")
    try:
        initial_tasks = api_acquire_tasks(limit=max_workers)
        if initial_tasks:
            print(f"Acquired {len(initial_tasks)} tasks on startup. (Running: {len(futures)})")
            for t in initial_tasks:
                print(f"  Task {t.get('id')}: {t.get('url', '')[:50]}")
            # 发送完整的任务对象，包含所有字段，避免字段丢失
            # 使用 METADATA_PROCESSING 状态，避免与 download worker 的 RUNNING 状态混淆
            running_updates = []
            for t in initial_tasks:
                update = {
                    "id": t.get('id'),
                    "job_id": t.get('job_id'),
                    "status": "METADATA_PROCESSING",
                    "worker_id": WORKER_ID,
                    # 保留所有现有字段
                    "url": t.get('url', ''),
                    "title": t.get('title', ''),
                    "video_id": t.get('video_id', ''),
                    "audio_url": t.get('audio_url', ''),
                    "audio_size": t.get('audio_size', 0),
                    "video_url": t.get('video_url', ''),
                    "video_size": t.get('video_size', 0),
                    "error_message": t.get('error_message', ''),
                }
                running_updates.append(update)
            api_update_task_batch(running_updates)
            for t in initial_tasks:
                f = executor.submit(process_metadata, t, ydl_opts)
                futures.add(f)
        else:
            print("No tasks available on startup. Worker will continue checking...")
    except Exception as e:
        print(f"Error acquiring tasks on startup: {e}")

    while True:
        try:
            # 0. 定期检查并更新限速配置（在主循环中）
            machine_name = get_machine_name()
            check_and_update_rate_limit(machine_name)
            
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
            # For small worker counts, always fetch when there are available slots
            # For larger worker counts, use threshold to avoid spamming
            should_fetch = (slots >= FETCH_THRESHOLD) or (slots > 0 and max_workers <= 10)
            
            if should_fetch:
                print(f"Fetching tasks: slots={slots}, FETCH_THRESHOLD={FETCH_THRESHOLD}, max_workers={max_workers}")
                tasks = api_acquire_tasks(limit=slots)
                if tasks:
                    print(f"✓ Acquired {len(tasks)} tasks. (Running: {len(futures)}, Slots: {slots})")
                    for t in tasks:
                        task_id = t.get('id')
                        video_id = t.get('video_id', '')
                        has_urls = bool(t.get('audio_url') or t.get('video_url'))
                        print(f"  - Task {task_id}: video_id='{video_id}', has_urls={has_urls}, url={t.get('url', '')[:50]}")
                    
                    # Mark METADATA_PROCESSING - 发送完整的任务对象，包含所有字段，避免字段丢失
                    # 使用 METADATA_PROCESSING 状态，避免与 download worker 的 RUNNING 状态混淆
                    running_updates = []
                    for t in tasks:
                        update = {
                            "id": t.get('id'),
                            "job_id": t.get('job_id'),
                            "status": "METADATA_PROCESSING",
                            "worker_id": WORKER_ID,
                            # 保留所有现有字段
                            "url": t.get('url', ''),
                            "title": t.get('title', ''),
                            "video_id": t.get('video_id', ''),
                            "audio_url": t.get('audio_url', ''),
                            "audio_size": t.get('audio_size', 0),
                            "video_url": t.get('video_url', ''),
                            "video_size": t.get('video_size', 0),
                            "error_message": t.get('error_message', ''),
                        }
                        running_updates.append(update)
                    api_update_task_batch(running_updates)
                    
                    # Submit
                    for t in tasks:
                        f = executor.submit(process_metadata, t, ydl_opts)
                        futures.add(f)
                else:
                    # No tasks available
                    if len(futures) == 0:
                        # Only log occasionally when idle to avoid spam
                        import random
                        if random.random() < 0.1:  # Log 10% of the time
                            print(f"No tasks available (slots: {slots}, running: {len(futures)})")
                        time.sleep(2)
                    else:
                        time.sleep(1)
            elif slots > 0:
                # Log when we have slots but don't fetch (for debugging)
                if len(futures) == 0:
                    # Only log occasionally to avoid spam
                    pass

        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(5)

def test_youtube_url(youtube_url, download_mode='both', video_strategy='highest_quality', job_id=999):
    """
    测试函数：测试 YouTube URL 的元数据提取
    
    参数:
        youtube_url: YouTube 视频 URL
        download_mode: 'both', 'audio', 'video' (默认: 'both')
        video_strategy: 'highest_quality', 'hd_priority', 'ultra_priority', 'min_1080p' (默认: 'highest_quality')
        job_id: 模拟的 job_id (默认: 999)
    """
    print("=" * 80)
    print("YouTube URL 测试")
    print("=" * 80)
    print(f"URL: {youtube_url}")
    print(f"Download Mode: {download_mode}")
    print(f"Video Strategy: {video_strategy}")
    print(f"Job ID: {job_id}")
    print("-" * 80)
    
    # 加载配置
    config = load_config()
    proxy_address = config.get('worker', {}).get('proxy_url')
    
    # 准备 yt-dlp 选项
    ydl_opts = {
        'quiet': False,  # 测试时显示详细信息
        'no_warnings': False,  # 测试时显示警告
        'simulate': True,
        'skip_download': True,
        'nocheckcertificate': True,
    }
    if proxy_address:
        ydl_opts['proxy'] = proxy_address
        print(f"使用代理: {proxy_address}")
    
    # Cookies 支持优先级（与 main 一致：强制自定义 > 后端下发 > 本地文件 > 浏览器 > 无 cookies）
    cookies_browser = os.environ.get('YOUTUBE_COOKIES_BROWSER')
    cookies_file_env = os.environ.get('YOUTUBE_COOKIES_FILE')
    no_cookies = os.environ.get('YOUTUBE_NO_COOKIES') == '1'
    
    if no_cookies:
        print("不使用 cookies，使用 Android/iOS/Web 客户端模式")
        ydl_opts['extractor_args'] = {
            'youtube': {
                'player_client': ['android', 'ios', 'web']
            }
        }
    elif cookies_file_env and os.path.exists(cookies_file_env):
        # 环境变量指定的 cookies 文件（优先，跳过后端）
        ydl_opts['cookiefile'] = cookies_file_env
        print(f"使用 cookies 文件 (环境变量优先，跳过后端下发): {cookies_file_env}")
    else:
        # 没有显式指定本地 cookie 文件时，才尝试从后端获取 cookie 配置
        machine_name = get_machine_name()
        cookie_file_from_backend = get_cookie_from_backend(machine_name)
        
        # 本地 cookie.txt 路径：当前目录或脚本目录
        cookie_file_paths = [
            "cookie.txt",
            "backend/worker_downloader/cookie.txt",
            os.path.join(os.path.dirname(__file__), "cookie.txt"),
        ]
        cookie_file = None
        if not cookie_file_from_backend:
            for path in cookie_file_paths:
                if os.path.exists(path):
                    cookie_file = path
                    break
        
        if cookie_file_from_backend:
            # 使用从后端获取的 cookie 文件（已在 get_cookie_from_backend 中设置到 COOKIE_CONFIG）
            ydl_opts['cookiefile'] = COOKIE_CONFIG['cookie_file']
            if COOKIE_CONFIG['parse_rate_limit'] > 0:
                print(f"使用从后端获取的 cookies 文件: {COOKIE_CONFIG['cookie_file']} (解析限流: {COOKIE_CONFIG['parse_rate_limit']} requests/min)")
            else:
                print(f"使用从后端获取的 cookies 文件: {COOKIE_CONFIG['cookie_file']}")
        elif cookie_file:
            # 使用本地 cookie.txt 文件
            ydl_opts['cookiefile'] = cookie_file
            print(f"使用 cookies 文件: {cookie_file}")
        elif cookies_browser:
            ydl_opts['cookiesfrombrowser'] = (cookies_browser,)
            print(f"从浏览器读取 cookies: {cookies_browser}")
        else:
            # 自动尝试从浏览器读取 cookies
            try:
                test_ydl = yt_dlp.YoutubeDL({'cookiesfrombrowser': ('chrome',), 'quiet': True})
                test_ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
                ydl_opts['cookiesfrombrowser'] = ('chrome',)
                print("自动使用 Chrome cookies")
            except:
                try:
                    test_ydl = yt_dlp.YoutubeDL({'cookiesfrombrowser': ('safari',), 'quiet': True})
                    test_ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
                    ydl_opts['cookiesfrombrowser'] = ('safari',)
                    print("自动使用 Safari cookies")
                except:
                    try:
                        test_ydl = yt_dlp.YoutubeDL({'cookiesfrombrowser': ('firefox',), 'quiet': True})
                        test_ydl.extract_info('https://www.youtube.com/watch?v=dQw4w9WgXcQ', download=False)
                        ydl_opts['cookiesfrombrowser'] = ('firefox',)
                        print("自动使用 Firefox cookies")
                    except:
                        # 如果没有 cookies，使用 Android/iOS/Web 客户端（无需 cookies）
                        print("未找到浏览器 cookies，使用 Android/iOS/Web 客户端模式（无需 cookies）")
                        ydl_opts['extractor_args'] = {
                            'youtube': {
                                'player_client': ['android', 'ios', 'web']
                            }
                        }
    
    print("-" * 80)
    
    # 创建模拟的 task
    task = {
        'id': 999999,
        'job_id': job_id,
        'url': youtube_url
    }
    
    # 创建模拟的 job_info（需要先设置到 JOB_CACHE）
    JOB_CACHE[job_id] = {
        'download_mode': download_mode,
        'video_selection_strategy': video_strategy
    }
    
    # 调用 process_metadata
    print("开始处理...")
    print()
    result = process_metadata(task, ydl_opts)
    
    # 打印结果
    print()
    print("=" * 80)
    print("测试结果")
    print("=" * 80)
    if result:
        print(f"状态: {result.get('status')}")
        print(f"标题: {result.get('title', 'N/A')}")
        print(f"视频 ID: {result.get('video_id', 'N/A')}")
        
        if result.get('audio_url'):
            print(f"\n音频 URL: {result.get('audio_url')[:100]}...")
            print(f"音频大小: {result.get('audio_size', 'N/A')} bytes")
        else:
            print("\n音频 URL: 未找到")
        
        if result.get('video_url'):
            print(f"\n视频 URL: {result.get('video_url')[:100]}...")
            print(f"视频大小: {result.get('video_size', 'N/A')} bytes")
        else:
            print("\n视频 URL: 未找到")
        
        if result.get('error_message'):
            print(f"\n错误信息: {result.get('error_message')}")
        
        # 打印完整的 JSON 结果
        print("\n完整结果 (JSON):")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("处理失败：返回 None")
    
    print("=" * 80)
    return result

if __name__ == "__main__":
    import sys
    
    # 如果提供了命令行参数，使用测试函数
    if len(sys.argv) > 1:
        youtube_url = sys.argv[1]
        # 参数顺序：youtube_url [download_mode] [video_strategy]
        download_mode = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] in ['both', 'audio', 'video'] else 'both'
        # 如果第二个参数不是 download_mode，则可能是 video_strategy
        if len(sys.argv) > 2 and sys.argv[2] not in ['both', 'audio', 'video']:
            video_strategy = sys.argv[2]
        else:
            video_strategy = sys.argv[3] if len(sys.argv) > 3 else 'highest_quality'
        
        print("\n使用方法:")
        print("  python get_yt_metadata.py <youtube_url> [download_mode] [video_strategy]")
        print("\n示例:")
        print("  python get_yt_metadata.py 'https://www.youtube.com/watch?v=VIDEO_ID'")
        print("  python get_yt_metadata.py 'https://www.youtube.com/watch?v=VIDEO_ID' both hd_priority")
        print("  python get_yt_metadata.py 'https://www.youtube.com/watch?v=VIDEO_ID' both min_1080p")
        print("\n参数:")
        print("  download_mode: both, audio, video")
        print("  video_strategy: highest_quality, hd_priority, ultra_priority, min_1080p")
        print()
        
        test_youtube_url(youtube_url, download_mode, video_strategy)
    else:
        # 否则运行正常的 worker
        main()
