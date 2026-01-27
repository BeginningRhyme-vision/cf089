import yt_dlp
import requests
import os
import yaml
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def load_config():
    """加载配置文件，获取代理设置"""
    paths = [
        "../../config_back_local.yaml", 
        "../../config.yaml", 
        "../config_back_local.yaml", 
        "../config.yaml", 
        "config_back_local.yaml", 
        "config.yaml"
    ]
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


def verify_url_accessibility(url: str, proxy: Optional[str] = None, timeout: int = 10) -> Dict:
    """
    验证URL的可访问性
    
    Args:
        url: 要验证的URL
        proxy: 代理地址（可选）
        timeout: 超时时间（秒）
    
    Returns:
        包含验证结果的字典
    """
    result = {
        "url": url,
        "accessible": False,
        "status_code": None,
        "content_length": None,
        "error": None,
        "response_time": None
    }
    
    try:
        proxies = None
        if proxy:
            proxies = {
                'http': proxy,
                'https': proxy
            }
        
        start_time = time.time()
        response = requests.head(
            url, 
            proxies=proxies,
            timeout=timeout,
            allow_redirects=True,
            verify=False
        )
        response_time = time.time() - start_time
        
        result["accessible"] = response.status_code == 200
        result["status_code"] = response.status_code
        result["content_length"] = response.headers.get('Content-Length')
        result["response_time"] = round(response_time, 3)
        
        if response.status_code != 200:
            result["error"] = f"HTTP {response.status_code}"
            
    except requests.exceptions.Timeout:
        result["error"] = "Timeout"
    except requests.exceptions.ConnectionError as e:
        result["error"] = f"Connection error: {str(e)}"
    except Exception as e:
        result["error"] = f"Error: {str(e)}"
    
    return result


def get_format_quality_info(format_info: Dict) -> Dict:
    """
    从格式信息中提取画质信息
    
    Args:
        format_info: yt-dlp格式信息字典
    
    Returns:
        包含画质信息的字典
    """
    quality = {
        "format_id": format_info.get('format_id', ''),
        "format_note": format_info.get('format_note', ''),
        "resolution": None,
        "width": format_info.get('width'),
        "height": format_info.get('height'),
        "fps": format_info.get('fps'),
        "vcodec": format_info.get('vcodec', ''),
        "acodec": format_info.get('acodec', ''),
        "abr": format_info.get('abr'),  # 音频码率 (kbps)
        "vbr": format_info.get('vbr'),  # 视频码率 (kbps)
        "tbr": format_info.get('tbr'),  # 总码率 (kbps)
        "filesize": format_info.get('filesize') or format_info.get('filesize_approx'),
        "ext": format_info.get('ext', ''),
        "language": format_info.get('language', ''),
    }
    
    # 构建分辨率字符串
    if quality["width"] and quality["height"]:
        quality["resolution"] = f"{quality['width']}x{quality['height']}"
        if quality["height"]:
            if quality["height"] >= 4320:
                quality["quality_label"] = "8K"
            elif quality["height"] >= 2160:
                quality["quality_label"] = "4K"
            elif quality["height"] >= 1440:
                quality["quality_label"] = "1440p"
            elif quality["height"] >= 1080:
                quality["quality_label"] = "1080p"
            elif quality["height"] >= 720:
                quality["quality_label"] = "720p"
            elif quality["height"] >= 480:
                quality["quality_label"] = "480p"
            elif quality["height"] >= 360:
                quality["quality_label"] = "360p"
            elif quality["height"] >= 240:
                quality["quality_label"] = "240p"
            else:
                quality["quality_label"] = "144p"
    else:
        quality["quality_label"] = "Unknown"
    
    # 文件大小格式化
    if quality["filesize"]:
        size_mb = quality["filesize"] / (1024 * 1024)
        quality["filesize_mb"] = round(size_mb, 2)
    else:
        quality["filesize_mb"] = None
    
    return quality


def list_youtube_formats(
    url: str, 
    proxy: Optional[str] = None,
    verify_accessibility: bool = True,
    max_workers: int = 10
) -> Dict:
    """
    列出YouTube视频的所有可下载格式，并验证可访问性
    
    Args:
        url: YouTube视频URL
        proxy: 代理地址（可选）
        verify_accessibility: 是否验证链接可访问性
        max_workers: 并发验证的最大线程数
    
    Returns:
        包含所有格式信息的字典
    """
    result = {
        "url": url,
        "title": None,
        "video_id": None,
        "duration": None,
        "audio_formats": [],
        "video_formats": [],
        "mixed_formats": [],
        "error": None
    }
    
    # 配置yt-dlp选项
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'simulate': True,
        'skip_download': True,
        'nocheckcertificate': True,
    }
    if proxy:
        ydl_opts['proxy'] = proxy
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
            result["title"] = info.get('title', '')
            result["video_id"] = info.get('id', '')
            result["duration"] = info.get('duration')
            
            formats = info.get('formats', [])
            
            # 分类格式
            audio_formats = []
            video_formats = []
            mixed_formats = []
            
            for fmt in formats:
                vcodec = fmt.get('vcodec', 'none')
                acodec = fmt.get('acodec', 'none')
                url = fmt.get('url')
                
                if not url:
                    continue
                
                format_data = {
                    "format_info": get_format_quality_info(fmt),
                    "url": url,
                    "accessibility": None
                }
                
                # 分类
                if vcodec != 'none' and acodec != 'none':
                    # 混合格式（视频+音频）
                    mixed_formats.append(format_data)
                elif vcodec != 'none' and acodec == 'none':
                    # 纯视频格式
                    video_formats.append(format_data)
                elif vcodec == 'none' and acodec != 'none':
                    # 纯音频格式
                    audio_formats.append(format_data)
            
            # 按画质排序
            def sort_key(fmt):
                quality = fmt["format_info"]
                height = quality.get("height", 0)
                abr = quality.get("abr", 0) or 0
                return (height, abr)
            
            video_formats.sort(key=sort_key, reverse=True)
            audio_formats.sort(key=sort_key, reverse=True)
            mixed_formats.sort(key=sort_key, reverse=True)
            
            result["audio_formats"] = audio_formats
            result["video_formats"] = video_formats
            result["mixed_formats"] = mixed_formats
            
            # 验证可访问性
            if verify_accessibility:
                print(f"验证 {len(audio_formats)} 个音频格式和 {len(video_formats)} 个视频格式的可访问性...")
                
                all_urls = []
                url_to_format = {}
                
                for fmt_list, fmt_type in [(audio_formats, "audio"), (video_formats, "video"), (mixed_formats, "mixed")]:
                    for fmt in fmt_list:
                        all_urls.append(fmt["url"])
                        url_to_format[fmt["url"]] = (fmt, fmt_type)
                
                # 并发验证
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_url = {
                        executor.submit(verify_url_accessibility, url, proxy): url 
                        for url in all_urls
                    }
                    
                    completed = 0
                    for future in as_completed(future_to_url):
                        completed += 1
                        url = future_to_url[future]
                        verification_result = future.result()
                        
                        fmt, fmt_type = url_to_format[url]
                        fmt["accessibility"] = verification_result
                        
                        if completed % 10 == 0:
                            print(f"已验证 {completed}/{len(all_urls)} 个链接...")
    
    except Exception as e:
        result["error"] = str(e)
        print(f"Error processing {url}: {e}")
    
    return result


def print_formats_summary(result: Dict):
    """打印格式摘要信息"""
    print("\n" + "="*80)
    print(f"视频标题: {result.get('title', 'N/A')}")
    print(f"视频ID: {result.get('video_id', 'N/A')}")
    if result.get('duration'):
        minutes = result['duration'] // 60
        seconds = result['duration'] % 60
        print(f"时长: {minutes}分{seconds}秒")
    print("="*80)
    
    if result.get('error'):
        print(f"错误: {result['error']}")
        return
    
    # 音频格式
    audio_formats = result.get('audio_formats', [])
    print(f"\n音频格式 ({len(audio_formats)} 个):")
    print("-" * 80)
    for i, fmt in enumerate(audio_formats, 1):
        quality = fmt["format_info"]
        acc = fmt.get("accessibility", {})
        accessible = "✓" if acc.get("accessible") else "✗"
        print(f"{i}. [{accessible}] {quality.get('quality_label', 'N/A')} | "
              f"码率: {quality.get('abr', 'N/A')} kbps | "
              f"格式: {quality.get('ext', 'N/A')} | "
              f"大小: {quality.get('filesize_mb', 'N/A')} MB")
        if acc.get("error"):
            print(f"   错误: {acc['error']}")
    
    # 视频格式
    video_formats = result.get('video_formats', [])
    print(f"\n视频格式 ({len(video_formats)} 个):")
    print("-" * 80)
    for i, fmt in enumerate(video_formats, 1):
        quality = fmt["format_info"]
        acc = fmt.get("accessibility", {})
        accessible = "✓" if acc.get("accessible") else "✗"
        print(f"{i}. [{accessible}] {quality.get('quality_label', 'N/A')} "
              f"({quality.get('resolution', 'N/A')}) | "
              f"FPS: {quality.get('fps', 'N/A')} | "
              f"码率: {quality.get('vbr', quality.get('tbr', 'N/A'))} kbps | "
              f"格式: {quality.get('ext', 'N/A')} | "
              f"大小: {quality.get('filesize_mb', 'N/A')} MB")
        if acc.get("error"):
            print(f"   错误: {acc['error']}")
    
    # 混合格式
    mixed_formats = result.get('mixed_formats', [])
    if mixed_formats:
        print(f"\n混合格式 (视频+音频) ({len(mixed_formats)} 个):")
        print("-" * 80)
        for i, fmt in enumerate(mixed_formats, 1):
            quality = fmt["format_info"]
            acc = fmt.get("accessibility", {})
            accessible = "✓" if acc.get("accessible") else "✗"
            print(f"{i}. [{accessible}] {quality.get('quality_label', 'N/A')} "
                  f"({quality.get('resolution', 'N/A')}) | "
                  f"FPS: {quality.get('fps', 'N/A')} | "
                  f"格式: {quality.get('ext', 'N/A')} | "
                  f"大小: {quality.get('filesize_mb', 'N/A')} MB")
            if acc.get("error"):
                print(f"   错误: {acc['error']}")


def test_list_youtube_formats(url: Optional[str] = None, save_json: bool = True):
    """
    测试函数
    
    Args:
        url: 要测试的YouTube URL，如果为None则使用默认测试URL
        save_json: 是否保存JSON结果文件
    """
    import sys
    
    # 如果没有提供URL，使用命令行参数或默认URL
    if not url:
        if len(sys.argv) > 1:
            url = sys.argv[1]
        else:
            url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # 默认测试视频
    
    test_urls = [url] if url else []
    
    # 加载配置获取代理
    config = load_config()
    proxy = config.get('worker', {}).get('proxy_url') if config else None
    
    if proxy:
        print(f"使用代理: {proxy}")
    else:
        print("未配置代理")
    
    for test_url in test_urls:
        print(f"\n正在处理: {test_url}")
        result = list_youtube_formats(
            url=test_url,
            proxy=proxy,
            verify_accessibility=True,
            max_workers=10
        )
        
        print_formats_summary(result)
        
        # 保存详细结果到JSON文件（可选）
        if save_json:
            import json
            video_id = result.get('video_id', 'unknown')
            output_file = f"youtube_formats_{video_id}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"\n详细结果已保存到: {output_file}")


if __name__ == "__main__":
    import sys
    
    # 支持命令行参数
    # 用法: python list_youtube_formats.py [youtube_url] [--no-json]
    url = None
    save_json = True
    
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg == "--no-json":
                save_json = False
            elif not arg.startswith("--"):
                url = arg
    
    test_list_youtube_formats(url=url, save_json=save_json)
