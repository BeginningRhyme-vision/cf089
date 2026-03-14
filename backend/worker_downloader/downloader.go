package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

// --- Configuration ---

type Config struct {
	Storage StorageConfig `yaml:"storage"`
	Worker  WorkerConfig  `yaml:"worker"`
}

type WorkerConfig struct {
	ProxyURL string `yaml:"proxy_url"`
}

type StorageConfig struct {
	Src                SrcConfig `yaml:"src"`
	DownloadServiceURL string    `yaml:"download_service_url"`
}

type SrcConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

var (
	cfg             *Config
	s3Client        *s3.Client
	s3PresignClient *s3.PresignClient
	bucketName      string
	apiBaseURL      string
	jobCache        sync.Map // map[int64]JobInfo (JobID -> JobInfo)
	workerID        string
	machineName     string
	internalClient  *http.Client
	externalClient  *http.Client

	// Global rate limiter for download requests (chunks per minute)
	globalRateLimiter *RateLimiter

	// Global semaphore to limit concurrent ListParts used for ETag lookup
	listPartsSem = make(chan struct{}, 3)

	// Metrics
	TasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_downloader_tasks_processed_total",
		Help: "The total number of processed tasks",
	}, []string{"status"})

	DownloadDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "worker_downloader_duration_seconds",
		Help: "Duration of downloads",
	})
)

// RateLimiter 速率限制器：控制每分钟的请求数
type RateLimiter struct {
	requestsPerMinute int
	requestTimes      []time.Time
	mu                sync.Mutex
}

// NewRateLimiter 创建新的速率限制器
func NewRateLimiter(requestsPerMinute int) *RateLimiter {
	return &RateLimiter{
		requestsPerMinute: requestsPerMinute,
		requestTimes:      make([]time.Time, 0),
	}
}

// Acquire 获取许可，如果超过限制则等待
func (rl *RateLimiter) Acquire() {
	if rl.requestsPerMinute <= 0 {
		return // 无限制
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	oneMinuteAgo := now.Add(-1 * time.Minute)

	// 移除一分钟之前的请求记录
	validTimes := make([]time.Time, 0)
	for _, t := range rl.requestTimes {
		if t.After(oneMinuteAgo) {
			validTimes = append(validTimes, t)
		}
	}
	rl.requestTimes = validTimes

	currentCount := len(rl.requestTimes)

	// 如果当前分钟内的请求数已达到限制，等待
	if currentCount >= rl.requestsPerMinute {
		// 计算需要等待的时间（直到最早的请求超过1分钟）
		oldestTime := rl.requestTimes[0]
		waitTime := time.Minute - now.Sub(oldestTime) + 100*time.Millisecond // 加100ms缓冲
		if waitTime > 0 {
			log.Printf("[RateLimiter] Rate limit reached (%d/%d requests/min), waiting %.2f seconds",
				currentCount, rl.requestsPerMinute, waitTime.Seconds())
			rl.mu.Unlock()
			time.Sleep(waitTime)
			rl.mu.Lock()
			// 等待后再次清理过期记录
			now = time.Now()
			oneMinuteAgo = now.Add(-1 * time.Minute)
			validTimes = make([]time.Time, 0)
			for _, t := range rl.requestTimes {
				if t.After(oneMinuteAgo) {
					validTimes = append(validTimes, t)
				}
			}
			rl.requestTimes = validTimes
		}
	}

	// 记录本次请求时间
	rl.requestTimes = append(rl.requestTimes, time.Now())
}

func initClients() {
	// Internal Client - No Proxy
	internalTransport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}
	internalClient = &http.Client{
		Transport: internalTransport,
		Timeout:   30 * time.Second,
	}

	// External Client - With Proxy
	externalTransport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}

	if os.Getenv("USE_PROXY") == "true" {
		if cfg.Worker.ProxyURL != "" {
			proxyURL, err := url.Parse(cfg.Worker.ProxyURL)
			if err != nil {
				log.Printf("Invalid proxy URL: %v", err)
			} else {
				externalTransport.Proxy = http.ProxyURL(proxyURL)
			}
		}
	}
	// No proxy
	// Use longer timeout for upload operations (10 minutes for large chunks)
	// Upload service may need time to download from YouTube CDN and upload to R2
	// 增加超时时间以支持更大的chunk size和更高的并发
	externalClient = &http.Client{
		Transport: externalTransport,
		Timeout:   10 * time.Minute, // 从5分钟增加到10分钟
	}
}

const (
// 默认值：6MB (注意原注释写的是32MB但实际是6MB，这里保持实际值不变)
// 可通过环境变量 DOWNLOAD_CHUNK_SIZE 覆盖
// 可通过环境变量 DOWNLOAD_MAX_CONCURRENT_WORKERS 覆盖
// 可通过环境变量 DOWNLOAD_TASK_BUFFER_SIZE 覆盖
)

var (
	ChunkSize               int64 = getDefaultChunkSize()
	MaxConcurrentWorkers          = getDefaultMaxConcurrentWorkers()
	TaskBufferSize                = getDefaultTaskBufferSize()
	ConcurrentChunksPerFile       = getDefaultConcurrentChunksPerFile() // 每个文件内部的并发chunk数量
	GlobalRequestsPerMinute       = getDefaultGlobalRequestsPerMinute() // 全局每分钟请求数限制
)

func getDefaultChunkSize() int64 {
	chunkSizeStr := os.Getenv("DOWNLOAD_CHUNK_SIZE")
	if chunkSizeStr != "" {
		if chunkSize, err := strconv.ParseInt(chunkSizeStr, 10, 64); err == nil {
			return chunkSize
		}
	}
	return 6 * 1024 * 1024 // 默认 32MB（从6MB增加到32MB以提高速度）
}

func getDefaultMaxConcurrentWorkers() int {
	workersStr := os.Getenv("DOWNLOAD_MAX_CONCURRENT_WORKERS")
	if workersStr != "" {
		if workers, err := strconv.Atoi(workersStr); err == nil {
			return workers
		}
	}
	// 默认并发 worker 数，从 200 降到 20（降速 10 倍）
	return 20
}

// getDefaultConcurrentChunksPerFile 获取每个文件内部的并发chunk数量
func getDefaultConcurrentChunksPerFile() int {
	chunksStr := os.Getenv("DOWNLOAD_CONCURRENT_CHUNKS_PER_FILE")
	if chunksStr != "" {
		if chunks, err := strconv.Atoi(chunksStr); err == nil {
			return chunks
		}
	}
	// 默认每个文件内部并发 chunk 数，从 200 降到 20（降速 10 倍）
	return 5
}

func getDefaultGlobalRequestsPerMinute() int {
	rateStr := os.Getenv("DOWNLOAD_REQUESTS_PER_MINUTE")
	if rateStr != "" {
		if rate, err := strconv.Atoi(rateStr); err == nil {
			return rate
		}
	}
	// 默认全局每分钟请求数，从 1000 降到 100（降速约 10 倍）
	return 100
}

func getDefaultTaskBufferSize() int {
	bufferStr := os.Getenv("DOWNLOAD_TASK_BUFFER_SIZE")
	if bufferStr != "" {
		if buffer, err := strconv.Atoi(bufferStr); err == nil {
			return buffer
		}
	}
	return 40 // 默认值
}

// --- Models ---

type YoutubeTask struct {
	ID        int64  `json:"id"`
	JobID     int64  `json:"job_id"`
	URL       string `json:"url"`
	AudioURL  string `json:"audio_url"`
	AudioSize int64  `json:"audio_size"`
	VideoURL  string `json:"video_url"`
	VideoSize int64  `json:"video_size"`
	Title     string `json:"title"`
	VideoID   string `json:"video_id"`
	Status    string `json:"status"`
}

type JobInfo struct {
	ID               uint   `json:"id"`
	R2Prefix         string `json:"r2_prefix"`
	AudioExtension   string `json:"audio_extension"`
	VideoExtension   string `json:"video_extension"`
	FilenameTemplate string `json:"filename_template"`
}

type UpdateTaskRequest struct {
	ID             int64  `json:"id"`
	Status         string `json:"status"`
	ErrorMessage   string `json:"error_message,omitempty"`
	IsDownloadFail bool   `json:"is_download_fail,omitempty"`
}

// --- Main ---

func main() {
	loadConfig()
	initClients()
	initS3()

	// Start Metrics Server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9091")
		http.ListenAndServe(":9091", nil)
	}()

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}

	// Initialize WorkerID
	workerID = os.Getenv("WORKER_ID")
	if workerID == "" {
		// Seed random generator (Go 1.20+ seeds global random automatically, but explicit seeding is safe for older versions)
		rand.Seed(time.Now().UnixNano())
		workerID = fmt.Sprintf("go-downloader-%04d", rand.Intn(10000))
	}

	// Initialize MachineName
	machineName = getMachineName()

	// Initialize global rate limiter
	globalRateLimiter = NewRateLimiter(GlobalRequestsPerMinute)

	log.Printf("=== Go Downloader Worker Started ===")
	log.Printf("Worker ID: %s", workerID)
	log.Printf("Machine Name: %s", machineName)
	log.Printf("Backend API URL: %s", apiBaseURL)
	log.Printf("R2 Storage Endpoint: %s", cfg.Storage.Src.Endpoint)
	log.Printf("R2 Bucket: %s", bucketName)
	log.Printf("Download Service URL: %s", cfg.Storage.DownloadServiceURL)
	log.Printf("Max Concurrent Workers: %d", MaxConcurrentWorkers)
	log.Printf("Chunk Size: %d bytes (%.2f MB)", ChunkSize, float64(ChunkSize)/(1024*1024))
	log.Printf("Concurrent Chunks Per File: %d", ConcurrentChunksPerFile)
	log.Printf("Global Requests Per Minute: %d", GlobalRequestsPerMinute)
	log.Printf("Note: This worker connects to backend API, not database directly")
	log.Printf("=====================================")

	taskChan := make(chan YoutubeTask, TaskBufferSize)

	// Start Fetcher
	go func() {
		log.Printf("Task fetcher started, polling for tasks from: %s/tasks/acquire", apiBaseURL)
		log.Printf("Worker will continue running and waiting for new tasks. Press Ctrl+C to stop.")
		pollCount := 0
		lastTaskTime := time.Now()
		for {
			pollCount++
			// Backpressure: if channel is mostly full, wait a bit
			// 当 channel 使用率达到 80% 时才等待（避免整数除法问题）
			threshold := int(float64(TaskBufferSize) * 0.8)
			if threshold == 0 {
				threshold = 1 // 至少为 1，避免总是等待
			}
			if len(taskChan) >= threshold {
				if pollCount%50 == 0 { // Log every 50 iterations to avoid spam
					log.Printf("Task channel nearly full (%d/%d), waiting...", len(taskChan), TaskBufferSize)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if pollCount == 1 || pollCount%10 == 0 { // Log first attempt and every 10th attempt
				log.Printf("[Poll #%d] Attempting to acquire tasks from backend...", pollCount)
			}

			tasks, err := acquireTasks()
			if err != nil {
				log.Printf("[Poll #%d] Error acquiring tasks: %v", pollCount, err)
				time.Sleep(5 * time.Second)
				continue
			}

			if len(tasks) == 0 {
				// 减少无任务时的日志输出频率，每 50 次轮询输出一次，并显示等待时间
				if pollCount%50 == 0 {
					timeSinceLastTask := time.Since(lastTaskTime)
					log.Printf("[Poll #%d] No tasks available (waiting for %v). Worker is running normally, waiting for new tasks...",
						pollCount, timeSinceLastTask.Round(time.Second))
				}
				time.Sleep(2 * time.Second)
				continue
			}
			lastTaskTime = time.Now()
			log.Printf("[Poll #%d] ✓ Acquired %d tasks from backend", pollCount, len(tasks))
			for _, t := range tasks {
				taskChan <- t
			}
		}
	}()

	// Start Workers
	var wg sync.WaitGroup
	for i := 0; i < MaxConcurrentWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskChan {
				processTask(t)
			}
		}()
	}
	wg.Wait()
}

// --- Logic ---

func loadConfig() {
	var configPath string

	// Check if config file is specified via environment variable
	if envPath := os.Getenv("CONFIG_FILE"); envPath != "" {
		if _, err := os.Stat(envPath); err == nil {
			configPath = envPath
		} else {
			log.Fatalf("Config file specified in CONFIG_FILE not found: %s", envPath)
		}
	} else {
		// Locate config.yaml (assuming run from backend/worker_downloader)
		paths := []string{"../../config_back_local.yaml", "../../config.yaml", "../config_back_local.yaml", "../config.yaml", "config_back_local.yaml", "config.yaml"}
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				configPath = p
				break
			}
		}
		if configPath == "" {
			log.Fatal("Could not find config.yaml or config_back_local.yaml")
		}
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	log.Printf("Loaded config from: %s", configPath)

	cfg = &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
}

func initS3() {
	// Parse Endpoint to get bucket?
	// The python code did: parsed_url.path.strip('/') -> bucket
	// endpoint -> https://...

	// We need a custom resolver for R2/S3
	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: cfg.Storage.Src.Endpoint,
		}, nil
	})

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithEndpointResolverWithOptions(r2Resolver),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Storage.Src.AccessKey,
			cfg.Storage.Src.SecretKey,
			"",
		)),
		awsconfig.WithRegion("auto"),
	)
	if err != nil {
		log.Fatalf("Failed to load aws config: %v", err)
	}

	s3Client = s3.NewFromConfig(c, func(o *s3.Options) {
		o.UsePathStyle = true // R2/Minio usually need this or virtual host. Python used virtual.
	})

	// Extract bucket from endpoint URL logic as per Python script
	// Python: parsed_url.path.strip('/')
	// E.g. https://<account>.r2.cloudflarestorage.com/<bucket> ??
	// Or https://s3.us-east-1.amazonaws.com/bucket
	// If endpoint is https://<account>.r2.cloudflarestorage.com, where is the bucket?
	// Python script: R2_BUCKET_NAME = parsed_url.path.strip('/')
	// This implies the endpoint URL in config includes the bucket name in the path?
	// Let's assume so.

	// Simple parsing
	u, _ := http.NewRequest("GET", cfg.Storage.Src.Endpoint, nil)
	path := u.URL.Path
	bucketName = strings.Trim(path, "/")
	if bucketName == "" {
		// Fallback or error? Python script exited.
		log.Fatal("Bucket name could not be derived from endpoint path")
	}

	// Adjust endpoint for SDK?
	// If endpoint has bucket path, SDK might append it again if we use path style.
	// Usually endpoint should be the domain.
	// But let's respect the python logic:
	// R2_ENDPOINT_URL = scheme://netloc
	// So we should strip path from endpoint used in SDK.

	sdkEndpoint := fmt.Sprintf("%s://%s", u.URL.Scheme, u.URL.Host)

	// Re-init with correct base endpoint
	r2Resolver2 := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: sdkEndpoint,
		}, nil
	})
	c2, _ := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithEndpointResolverWithOptions(r2Resolver2),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Storage.Src.AccessKey,
			cfg.Storage.Src.SecretKey,
			"",
		)),
		awsconfig.WithRegion("auto"),
	)
	s3Client = s3.NewFromConfig(c2, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Initialize presign client for generating presigned URLs
	s3PresignClient = s3.NewPresignClient(s3Client)

	log.Printf("S3/R2 client initialized - Endpoint: %s, Bucket: %s", sdkEndpoint, bucketName)
}

// getMachineName 获取机器名，优先使用 Kubernetes 节点名，然后是环境变量，最后使用主机名
func getMachineName() string {
	// 优先使用 Kubernetes 节点名（通过 Downward API 注入）
	if name := os.Getenv("NODE_NAME"); name != "" {
		return name
	}
	// 其次使用手动设置的机器名
	if name := os.Getenv("MACHINE_NAME"); name != "" {
		return name
	}
	// 最后使用主机名（在容器中通常是 pod 名，不是宿主机名）
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func acquireTasks() ([]YoutubeTask, error) {
	url := apiBaseURL + "/tasks/acquire"
	reqBody, _ := json.Marshal(map[string]interface{}{
		"worker_id":    workerID,
		"machine_name": machineName,
		"stage":        "download",
		"limit":        TaskBufferSize,
	})

	log.Printf("  → Requesting tasks from: %s (worker_id: %s, machine_name: %s, stage: download, limit: %d)", url, workerID, machineName, TaskBufferSize)

	resp, err := internalClient.Post(url, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("  ✗ HTTP request failed: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	log.Printf("  ← Response status: %d %s", resp.StatusCode, resp.Status)

	if resp.StatusCode != 200 {
		log.Printf("  ✗ Unexpected status code: %d", resp.StatusCode)
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []YoutubeTask
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		log.Printf("  ✗ Failed to decode response: %v", err)
		return nil, err
	}

	if len(tasks) > 0 {
		log.Printf("  ✓ Successfully decoded %d tasks", len(tasks))
		// 记录每个任务的详细信息，用于调试
		for i, task := range tasks {
			log.Printf("  Task[%d]: ID=%d, VideoID=%s, AudioURL=%v (size=%d), VideoURL=%v (size=%d)",
				i, task.ID, task.VideoID, task.AudioURL != "", task.AudioSize, task.VideoURL != "", task.VideoSize)
		}
	} else {
		log.Printf("  ℹ No tasks in response (empty array)")
	}

	return tasks, nil
}

func sanitizeTitle(title string) string {
	// Simple sanitization: replace non-alphanumeric chars with underscore, keep dots and dashes
	// Or just remove very bad chars.
	// Let's replace anything that is not letter, number, dot, dash, underscore with underscore.
	// For simplicity, just replacing / with _ and trimming is usually enough for S3/R2 keys unless restricted.
	// But to be safe:
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, title)
	return strings.Trim(safe, "_")
}

func strftimeToGo(format string) string {
	// Basic mapping of strftime to Go layout
	mapping := map[string]string{
		"%Y": "2006",
		"%m": "01",
		"%d": "02",
		"%H": "15",
		"%M": "04",
		"%S": "05",
	}
	for k, v := range mapping {
		format = strings.ReplaceAll(format, k, v)
	}
	return format
}

func generateFilename(template, prefix, videoID, title, ext string) string {
	if template == "" {
		// Default behavior
		// Ensure trailing slash on prefix
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		// If audio/video differentiation is needed, the caller usually appends suffix.
		// But here we are generating the base path.
		// Wait, existing logic was: prefix + videoID + "_audio." + ext
		// The template replaces that entirely.
		// So if no template, we return empty string to signal "use default logic" or we handle default logic here?
		// To be compatible with previous logic which distinguished audio/video suffixes,
		// we should probably let the caller handle default if this returns empty,
		// OR we pass a "type" (audio/video) to this function?
		// The prompt says: "支持自定义文件的路径名称... 比如 $(date ...)/%(id).%(ext)"
		// If template is provided, we use it. If not, we fall back.
		return ""
	}

	filename := template

	// 1. Date replacement: $(date +FORMAT)
	// Find all occurrences
	for {
		start := strings.Index(filename, "$(date +")
		if start == -1 {
			break
		}
		end := strings.Index(filename[start:], ")")
		if end == -1 {
			break
		}
		end += start

		fullToken := filename[start : end+1] // $(date +...)
		formatStr := filename[start+8 : end] // The part after "+", e.g. %Y%m%d_%H

		goLayout := strftimeToGo(formatStr)
		timeStr := time.Now().Format(goLayout)

		filename = strings.Replace(filename, fullToken, timeStr, 1)
	}

	// 2. Variables replacement
	filename = strings.ReplaceAll(filename, "%(id)", videoID)
	filename = strings.ReplaceAll(filename, "%(title)", sanitizeTitle(title))
	filename = strings.ReplaceAll(filename, "%(ext)", ext)

	// Prepend prefix if not absolute (usually we just prepend R2Prefix)
	// The user said: "拼接 R2Prefix + 自定义文件名"
	// So we always prepend Prefix.

	// Ensure prefix has slash if it exists
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// If the template starts with /, remove it to avoid double slash with prefix?
	// Usually keys don't start with /
	filename = strings.TrimPrefix(filename, "/")

	return prefix + filename
}

func getJobInfo(jobID int64) (JobInfo, error) {
	if val, ok := jobCache.Load(jobID); ok {
		return val.(JobInfo), nil
	}

	resp, err := internalClient.Get(fmt.Sprintf("%s/youtube-jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return JobInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return JobInfo{}, fmt.Errorf("job not found")
	}

	var job JobInfo
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return JobInfo{}, err
	}

	jobCache.Store(jobID, job)
	return job, nil
}

func processTask(t YoutubeTask) {
	start := time.Now()
	defer func() {
		DownloadDuration.Observe(time.Since(start).Seconds())
	}()

	updateTaskStatusWithTask(t, "RUNNING")
	log.Printf("Task %d (%s) RUNNING", t.ID, t.VideoID)

	jobInfo, err := getJobInfo(t.JobID)
	if err != nil {
		reportErrorWithTask(t, "Failed to get job info: "+err.Error())
		return
	}

	prefix := jobInfo.R2Prefix
	// Ensure trailing slash
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	// 记录任务信息，用于调试
	log.Printf("Task %d: AudioURL=%v (size=%d), VideoURL=%v (size=%d)",
		t.ID, t.AudioURL != "", t.AudioSize, t.VideoURL != "", t.VideoSize)
	if t.AudioURL == "" && t.VideoURL == "" {
		reportErrorWithTask(t, "both audio_url and video_url are empty, task is not downloadable")
		return
	}

	if t.AudioURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ext := jobInfo.AudioExtension
			if ext == "" {
				ext = "m4a"
			}

			var key string
			customKey := generateFilename(jobInfo.FilenameTemplate, jobInfo.R2Prefix, t.VideoID, t.Title, ext)
			if customKey != "" {
				key = customKey
			} else {
				key = fmt.Sprintf("%s%s_audio.%s", prefix, t.VideoID, ext)
			}

			log.Printf("Task %d: Starting audio download to %s (size=%d)", t.ID, key, t.AudioSize)
			if err := transferFile(t.AudioURL, key, t.AudioSize); err != nil {
				log.Printf("Task %d: Audio download failed: %v", t.ID, err)
				errChan <- fmt.Errorf("audio failed: %w", err)
			} else {
				log.Printf("Task %d: Audio download completed successfully", t.ID)
			}
		}()
	} else {
		log.Printf("Task %d: No AudioURL, skipping audio download", t.ID)
	}

	if t.VideoURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ext := jobInfo.VideoExtension
			if ext == "" {
				ext = "mp4"
			}

			var key string
			customKey := generateFilename(jobInfo.FilenameTemplate, jobInfo.R2Prefix, t.VideoID, t.Title, ext)
			if customKey != "" {
				key = customKey
			} else {
				key = fmt.Sprintf("%s%s_video.%s", prefix, t.VideoID, ext)
			}

			log.Printf("Task %d: Starting video download to %s (size=%d)", t.ID, key, t.VideoSize)
			if err := transferFile(t.VideoURL, key, t.VideoSize); err != nil {
				log.Printf("Task %d: Video download failed: %v", t.ID, err)
				errChan <- fmt.Errorf("video failed: %w", err)
			} else {
				log.Printf("Task %d: Video download completed successfully", t.ID)
			}
		}()
	} else {
		log.Printf("Task %d: No VideoURL, skipping video download", t.ID)
	}

	log.Printf("Task %d: Waiting for downloads to complete...", t.ID)
	wg.Wait()
	close(errChan)
	log.Printf("Task %d: All downloads completed, checking errors...", t.ID)

	var errs []string
	for e := range errChan {
		errs = append(errs, e.Error())
	}

	if len(errs) > 0 {
		reportErrorWithTask(t, strings.Join(errs, "; "))
	} else {
		updateTaskStatusWithTask(t, "COMPLETED")
		TasksProcessed.WithLabelValues("success").Inc()
		log.Printf("Task %d (%s) COMPLETED", t.ID, t.VideoID)
	}
}

// getContentTypeFromURL 从 URL 参数中提取 MIME 类型（例如 YouTube CDN URL 中的 mime=video/mp4）
func getContentTypeFromURL(sourceURL string) string {
	parsedURL, err := url.Parse(sourceURL)
	if err != nil {
		return ""
	}

	// 检查 URL 查询参数中的 mime 参数
	mime := parsedURL.Query().Get("mime")
	if mime != "" {
		return mime
	}

	return ""
}

// getContentTypeFromKey 根据文件扩展名获取 Content-Type
func getContentTypeFromKey(key string) string {
	lastDot := strings.LastIndex(key, ".")
	if lastDot == -1 || lastDot == len(key)-1 {
		// 没有扩展名或扩展名为空，返回默认值
		return "application/octet-stream"
	}

	ext := strings.ToLower(key[lastDot+1:])
	contentTypes := map[string]string{
		"mp4":  "video/mp4",
		"webm": "video/webm",
		"mkv":  "video/x-matroska",
		"avi":  "video/x-msvideo",
		"mov":  "video/quicktime",
		"m4a":  "audio/mp4",
		"mp3":  "audio/mpeg",
		"opus": "audio/opus",
		"ogg":  "audio/ogg",
		"wav":  "audio/wav",
		"flac": "audio/flac",
		"aac":  "audio/aac",
	}
	if ct, ok := contentTypes[ext]; ok {
		return ct
	}
	// 默认返回 binary
	return "application/octet-stream"
}

// getContentType 智能获取 Content-Type：优先从 URL 参数，其次从文件扩展名
func getContentType(sourceURL, key string) string {
	// 1. 优先尝试从 URL 参数获取（例如 YouTube CDN URL 中的 mime=video/mp4）
	if contentType := getContentTypeFromURL(sourceURL); contentType != "" {
		return contentType
	}

	// 2. 从文件扩展名获取
	return getContentTypeFromKey(key)
}

func transferFile(sourceURL, key string, providedSize int64) error {
	size := providedSize

	if size <= 0 {
		return fmt.Errorf("invalid content length: %d", size)
	}

	// 智能获取 Content-Type：优先从 URL 参数（如 mime=video/mp4），其次从文件扩展名
	contentType := getContentType(sourceURL, key)

	// 2. Initiate Multipart
	createOut, err := s3Client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return err
	}
	uploadID := *createOut.UploadId

	// 3. Upload Parts
	numParts := int(math.Ceil(float64(size) / float64(ChunkSize)))
	log.Printf("Starting multipart upload: key=%s, size=%d, chunkSize=%d, numParts=%d", key, size, ChunkSize, numParts)

	var completedParts []types.CompletedPart
	var partsMu sync.Mutex
	var wg sync.WaitGroup
	errAbort := make(chan error, 1)
	missingEtagParts := make(map[int32]struct{})
	var missingMu sync.Mutex

	// 使用可配置的并发chunk数量（默认30，可通过环境变量 DOWNLOAD_CONCURRENT_CHUNKS_PER_FILE 配置）
	sem := make(chan struct{}, ConcurrentChunksPerFile)

	for i := 0; i < numParts; i++ {
		start := int64(i) * ChunkSize
		end := start + ChunkSize - 1
		if end >= size {
			end = size - 1
		}
		partNum := int32(i + 1)
		partSize := end - start + 1

		// Validate part size
		if partSize <= 0 {
			return fmt.Errorf("invalid part size for part %d: start=%d, end=%d, size=%d", partNum, start, end, partSize)
		}
		if partSize > ChunkSize {
			return fmt.Errorf("part size exceeds chunk size for part %d: partSize=%d, chunkSize=%d", partNum, partSize, ChunkSize)
		}

		wg.Add(1)
		go func(pNum int32, s, e int64) {
			defer wg.Done()

			// 1. 先获取文件内部的并发限制（每个文件的并发chunk数）
			sem <- struct{}{}
			defer func() { <-sem }()

			// Check if aborted
			select {
			case <-errAbort:
				return
			default:
			}

			// 2. 获取全局速率限制许可（限制总的 chunk 上传请求频率）
			// 注意：单元测试可能不会初始化 globalRateLimiter，因此需要 nil 保护
			if globalRateLimiter != nil {
				globalRateLimiter.Acquire()
			}

			// Log chunk details for debugging
			chunkSize := e - s + 1
			log.Printf("[CHUNK DEBUG] Part %d: start=%d, end=%d, size=%d (expected total=%d, chunkSize=%d)",
				pNum, s, e, chunkSize, size, ChunkSize)

			etag, err := uploadChunkExternal(sourceURL, key, uploadID, pNum, s, e)
			if err != nil {
				if isMissingEtagRecoverable(err) {
					missingMu.Lock()
					missingEtagParts[pNum] = struct{}{}
					missingMu.Unlock()
					log.Printf("[CHUNK RECOVERABLE] Part %d missing etag, defer to final recovery: %v", pNum, err)
					return
				}
				log.Printf("[CHUNK ERROR] Part %d failed: %v", pNum, err)
				select {
				case errAbort <- err:
				default:
				}
				return
			}

			log.Printf("[CHUNK SUCCESS] Part %d completed: etag=%s, range=bytes %d-%d/%d",
				pNum, etag, s, e, size)

			partsMu.Lock()
			completedParts = append(completedParts, types.CompletedPart{
				ETag:       aws.String(etag),
				PartNumber: aws.Int32(pNum),
			})
			partsMu.Unlock()
		}(partNum, start, end)
	}

	wg.Wait()

	select {
	case err := <-errAbort:
		s3Client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		return err
	default:
	}

	if len(completedParts) != numParts {
		recoveredParts, recoverErr := waitAndCollectCompletedParts(bucketName, key, uploadID, numParts, 2*time.Minute)
		if recoverErr == nil {
			completedParts = recoveredParts
			log.Printf("[UPLOAD RECOVERY] recovered all parts via ListParts: %d/%d", len(completedParts), numParts)
		} else {
			missingMu.Lock()
			missingCount := len(missingEtagParts)
			missingMu.Unlock()
			log.Printf("[UPLOAD RECOVERY] failed to recover all parts (current=%d expected=%d missing_marked=%d): %v", len(completedParts), numParts, missingCount, recoverErr)
		}
	}

	// Verify all parts were uploaded
	if len(completedParts) != numParts {
		s3Client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		return fmt.Errorf("incomplete upload: expected %d parts, got %d parts", numParts, len(completedParts))
	}

	// Sort parts by part number
	for i := 0; i < len(completedParts); i++ {
		for j := i + 1; j < len(completedParts); j++ {
			if *completedParts[i].PartNumber > *completedParts[j].PartNumber {
				completedParts[i], completedParts[j] = completedParts[j], completedParts[i]
			}
		}
	}

	// Verify all parts are present and in order
	log.Printf("[UPLOAD VERIFY] Verifying %d parts for upload completion...", len(completedParts))
	for i, part := range completedParts {
		partIdx := int(*part.PartNumber) - 1
		expectedPartNum := int32(i + 1)
		if *part.PartNumber != expectedPartNum {
			return fmt.Errorf("part number mismatch: expected %d, got %d at index %d", expectedPartNum, *part.PartNumber, i)
		}

		// Calculate expected range for this part
		partStart := int64(partIdx) * ChunkSize
		partEnd := partStart + ChunkSize - 1
		if partEnd >= size {
			partEnd = size - 1
		}
		expectedSize := partEnd - partStart + 1
		log.Printf("[UPLOAD VERIFY] Part %d: expected range bytes %d-%d (size=%d), etag=%s",
			*part.PartNumber, partStart, partEnd, expectedSize, *part.ETag)
	}

	// Verify total size of all parts matches expected size
	totalPartsSize := int64(0)
	for _, part := range completedParts {
		partIdx := int(*part.PartNumber) - 1
		if partIdx < numParts {
			partStart := int64(partIdx) * ChunkSize
			partEnd := partStart + ChunkSize - 1
			if partEnd >= size {
				partEnd = size - 1
			}
			partSize := partEnd - partStart + 1
			totalPartsSize += partSize
		}
	}

	log.Printf("[UPLOAD VERIFY] Total calculated size: %d bytes, expected: %d bytes", totalPartsSize, size)
	if totalPartsSize != size {
		return fmt.Errorf("total parts size mismatch: expected %d bytes, calculated %d bytes (difference: %d)",
			size, totalPartsSize, totalPartsSize-size)
	}

	_, err = s3Client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		// If CompleteMultipartUpload fails, abort the upload to clean up
		s3Client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	log.Printf("Successfully uploaded: %s (expected size: %d bytes, content-type: %s)", key, size, contentType)

	// For MP4/M4A files, warn about potential moov block corruption
	// The moov block is typically at byte 32 and is in the first chunk
	if (strings.HasSuffix(strings.ToLower(key), ".mp4") || strings.HasSuffix(strings.ToLower(key), ".m4a")) &&
		ChunkSize > 32768 {
		log.Printf("[MP4 WARNING] MP4 file uploaded. Moov block (typically at byte 32) is in first chunk.")
		log.Printf("[MP4 WARNING] If playback shows incorrect duration, verify download service correctly handles Range requests.")
		log.Printf("[MP4 WARNING] Repair command: ffmpeg -i %s -c copy -movflags +faststart fixed.mp4", key)
	}

	return nil
}

func isMissingEtagRecoverable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "2xx but missing etag")
}

func waitAndCollectCompletedParts(bucket, key, uploadID string, expectedParts int, timeout time.Duration) ([]types.CompletedPart, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		paginator := s3.NewListPartsPaginator(s3Client, &s3.ListPartsInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		partETagMap := make(map[int32]string, expectedParts)
		var pageErr error
		for paginator.HasMorePages() {
			out, err := paginator.NextPage(ctx)
			if err != nil {
				pageErr = err
				break
			}
			for _, part := range out.Parts {
				if part.PartNumber != nil && part.ETag != nil {
					etag := strings.Trim(*part.ETag, `"`)
					if etag != "" {
						partETagMap[*part.PartNumber] = etag
					}
				}
			}
		}
		cancel()
		if pageErr == nil {
			if len(partETagMap) >= expectedParts {
				result := make([]types.CompletedPart, 0, expectedParts)
				ok := true
				for i := 1; i <= expectedParts; i++ {
					pn := int32(i)
					etag, exists := partETagMap[pn]
					if !exists {
						ok = false
						break
					}
					result = append(result, types.CompletedPart{
						ETag:       aws.String(etag),
						PartNumber: aws.Int32(pn),
					})
				}
				if ok {
					return result, nil
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("collect parts timeout: expected=%d", expectedParts)
}

// listIncompleteMultipartUploads 列出所有未完成的多部分上传
func listIncompleteMultipartUploads(prefix string) ([]types.MultipartUpload, error) {
	ctx := context.Background()
	var uploads []types.MultipartUpload

	paginator := s3.NewListMultipartUploadsPaginator(s3Client, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, output.Uploads...)
	}

	return uploads, nil
}

// abortIncompleteMultipartUpload 中止一个未完成的多部分上传
func abortIncompleteMultipartUpload(key string, uploadID string) error {
	_, err := s3Client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

// cleanupIncompleteMultipartUploads 清理指定前缀的所有未完成的多部分上传
func cleanupIncompleteMultipartUploads(prefix string) error {
	uploads, err := listIncompleteMultipartUploads(prefix)
	if err != nil {
		return fmt.Errorf("failed to list incomplete uploads: %w", err)
	}

	if len(uploads) == 0 {
		log.Printf("No incomplete multipart uploads found with prefix: %s", prefix)
		return nil
	}

	log.Printf("Found %d incomplete multipart upload(s) with prefix: %s", len(uploads), prefix)
	for _, upload := range uploads {
		err := abortIncompleteMultipartUpload(*upload.Key, *upload.UploadId)
		if err != nil {
			log.Printf("Failed to abort upload %s (ID: %s): %v", *upload.Key, *upload.UploadId, err)
		} else {
			log.Printf("Aborted incomplete upload: %s (ID: %s, Initiated: %v)",
				*upload.Key, *upload.UploadId, *upload.Initiated)
		}
	}

	return nil
}

func uploadChunkExternal(srcURL, key, uploadID string, partNum int32, start, end int64) (string, error) {
	// Generate presigned URL for UploadPart
	ctx := context.TODO()
	presignRequest, err := s3PresignClient.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		PartNumber: aws.Int32(partNum),
		UploadId:   aws.String(uploadID),
	}, func(opts *s3.PresignOptions) {
		// Set expiration time (AWS SigV4 presign commonly supports up to 7 days; we use 1 day here)
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL for part %d: %w", partNum, err)
	}

	// If source is a private R2 object URL (no X-Amz-*), presign it (GET) so upload worker can fetch it via plain fetch().
	// This matches “方式 A”：调用方传入带 X-Amz-* 的预签名 GET URL，避免 Worker fetch 私有对象 401/403。
	if maybeNeedsPresignedGet(srcURL) {
		if presignedSrc, perr := presignR2GetObjectURL(ctx, srcURL); perr == nil && presignedSrc != "" {
			log.Printf("[PRESIGNED SRC] Part %d: using presigned GET source URL (len=%d)", partNum, len(presignedSrc))
			srcURL = presignedSrc
		} else if perr != nil {
			log.Printf("[PRESIGNED SRC] Part %d: failed to presign source URL, using original srcURL: %v", partNum, perr)
		}
	}

	// Get the presigned URL
	presignedURL := presignRequest.URL
	// Also forward any required signed headers (some S3-compatible providers require these)
	signedHeaders := map[string]string{}
	for k, vals := range presignRequest.SignedHeader {
		if len(vals) > 0 {
			signedHeaders[k] = vals[0]
		}
	}

	// Log presigned URL details for debugging
	log.Printf("[PRESIGNED URL] Part %d: URL length=%d, signedHeaders count=%d", partNum, len(presignedURL), len(signedHeaders))
	if len(signedHeaders) > 0 {
		log.Printf("[PRESIGNED URL] Part %d signedHeaders: %+v", partNum, signedHeaders)
	}

	chunkSize := end - start + 1
	payload := map[string]interface{}{
		"fileUrl":    srcURL,
		"offset":     start,
		"size":       chunkSize,
		"r2Key":      presignedURL, // Send presigned URL instead of plain URL
		"headers":    signedHeaders,
		"partNumber": partNum,
		"uploadId":   uploadID,
		// 明确指定 HTTP 方法（UploadPart 必须使用 PUT）
		"method": "PUT",
	}

	// Validate chunk size before sending
	if chunkSize <= 0 {
		return "", fmt.Errorf("invalid chunk size for part %d: start=%d, end=%d, size=%d", partNum, start, end, chunkSize)
	}
	if chunkSize > ChunkSize {
		return "", fmt.Errorf("chunk size exceeds limit for part %d: size=%d, limit=%d", partNum, chunkSize, ChunkSize)
	}

	// Critical: Validate Range calculation for download service
	// The download service should construct: Range: bytes=start-end
	// Where end = start + size - 1 (NOT start + size)
	expectedRangeEnd := start + chunkSize - 1
	if expectedRangeEnd != end {
		return "", fmt.Errorf("range calculation error for part %d: expected end=%d, calculated end=%d (start=%d, size=%d)",
			partNum, end, expectedRangeEnd, start, chunkSize)
	}

	// Warn if this is the first chunk and it contains moov block (typically at byte 32)
	// MP4 moov block is usually in the first few KB, so if ChunkSize > 32KB, moov is in first chunk
	if partNum == 1 && start == 0 && ChunkSize > 32768 {
		log.Printf("[CRITICAL] Part 1 contains MP4 moov block (typically at byte 32). Ensure download service correctly handles Range: bytes=%d-%d",
			start, end)
	}

	body, _ := json.Marshal(payload)

	// Log request details in JSON format
	requestLog := map[string]interface{}{
		"url":    cfg.Storage.DownloadServiceURL,
		"method": "POST",
		"headers": map[string]string{
			"Content-Type": "application/json",
		},
		"body": payload,
	}
	requestLogJSON, _ := json.MarshalIndent(requestLog, "", "  ")
	log.Printf("[REQUEST DEBUG] Chunk %d - Request details:\n%s", partNum, string(requestLogJSON))

	var (
		resp      *http.Response
		reqErr    error
		bodyBytes []byte
	)
	for attempt := 1; attempt <= 3; attempt++ {
		resp, reqErr = externalClient.Post(cfg.Storage.DownloadServiceURL, "application/json", bytes.NewBuffer(body))
		if reqErr != nil {
			if attempt == 3 {
				return "", fmt.Errorf("failed to upload chunk %d: HTTP request failed after %d attempts: %v", partNum, attempt, reqErr)
			}
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		bodyBytes, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			if attempt == 3 {
				break
			}
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		break
	}

	etag := ""
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// 首先尝试从响应头获取 ETag
		if etagHeader := resp.Header.Get("ETag"); etagHeader != "" {
			// 移除引号（如果存在）
			etag = strings.Trim(etagHeader, `"`)
			log.Printf("[CHUNK RESPONSE] Part %d: got ETag from response header: %s", partNum, etag)
		}

		// 如果响应头没有，尝试从响应体获取
		if etag == "" && len(bodyBytes) > 0 {
			var resMap map[string]interface{}
			if err := json.Unmarshal(bodyBytes, &resMap); err == nil {
				// 尝试多种可能的字段名（不区分大小写）
				if val, ok := resMap["etag"].(string); ok && val != "" {
					etag = val
				} else if val, ok := resMap["ETag"].(string); ok && val != "" {
					etag = val
				} else if val, ok := resMap["Etag"].(string); ok && val != "" {
					etag = val
				}
				// Log response details for debugging
				log.Printf("[CHUNK RESPONSE] Part %d response: status=%d, etag=%s, body=%+v",
					partNum, resp.StatusCode, etag, resMap)
			} else {
				log.Printf("[CHUNK WARNING] Part %d: failed to decode response body: %v (body: %s)",
					partNum, err, string(bodyBytes))
				// 即使解析失败，也尝试从响应头获取
				if etagHeader := resp.Header.Get("ETag"); etagHeader != "" {
					etag = strings.Trim(etagHeader, `"`)
					log.Printf("[CHUNK RESPONSE] Part %d: got ETag from response header after decode failure: %s", partNum, etag)
				}
			}
		}
		
		// 如果 2xx 但没有 ETag，等待后使用 ListParts
		if etag == "" {
			log.Printf("[CHUNK WAIT] Part %d: 2xx response but missing ETag (status=%d), will use ListParts to retrieve ETag", partNum, resp.StatusCode)
			
			// 先等待至少1秒，避免立即查询
			log.Printf("[CHUNK WAIT] Part %d: waiting 1s before ListParts attempt", partNum)
			time.Sleep(1 * time.Second)

			lookupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			etag2, lerr := fetchPartETag(lookupCtx, bucketName, key, uploadID, partNum)
			cancel()
			if lerr == nil && etag2 != "" {
				log.Printf("[CHUNK RESPONSE] Part %d: recovered ETag via ListParts: %s", partNum, etag2)
				return etag2, nil
			} else {
				// 带上 body，便于定位 upload service 是否"成功但未上传"或"成功但未返回 etag"
				return "", fmt.Errorf("2xx but missing etag (status=%d) and ListParts lookup failed: %v. body=%s", resp.StatusCode, lerr, string(bodyBytes))
			}
		}
		
		// 如果成功获取到 ETag，直接返回
		if etag != "" {
			return etag, nil
		}
	} else {
		// 非 2xx 响应，直接返回错误
		log.Printf("[CHUNK ERROR] Part %d: HTTP status %d", partNum, resp.StatusCode)
		if len(bodyBytes) > 0 {
			log.Printf("[CHUNK ERROR] Part %d response body: %s", partNum, string(bodyBytes))
		}
		if resp.StatusCode == 403 {
			return "", fmt.Errorf("403 Forbidden: presigned URL authentication failed - check if upload service uses PUT method and correct headers. Response: %s", string(bodyBytes))
		} else {
			return "", fmt.Errorf("HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}
	}
	
	return "", fmt.Errorf("failed to upload chunk %d,Can't get Etag", partNum)
}

// fetchPartETag queries R2/S3 multipart state to find the ETag for a specific uploaded part.
func fetchPartETag(ctx context.Context, bucket, key, uploadID string, partNum int32) (string, error) {
	// Some upload services may return 200 before the part is actually committed in R2.
	// 为避免对 R2 造成过高压力，这里做一个"温和"的短轮询：
	// - 全局并发上限：3（listPartsSem）
	// - 每个 part 最多 3 次尝试，退避时间从1秒起步（1s, 2s, 3s），总等待时间约 <= 10 秒
	maxAttempts := 8
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 全局并发控制
		select {
		case listPartsSem <- struct{}{}:
		case <-ctx.Done():
			return "", fmt.Errorf("part %d not found in ListParts (attempts=%d): %w", partNum, attempt-1, ctx.Err())
		}

		found := false
		etag := ""

		// 单次 ListParts，只要找到对应 part 就返回
		p := s3.NewListPartsPaginator(s3Client, &s3.ListPartsInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		for p.HasMorePages() {
			out, err := p.NextPage(ctx)
			if err != nil {
				lastErr = err
				break
			}
			for _, part := range out.Parts {
				if part.PartNumber != nil && *part.PartNumber == partNum {
					if part.ETag != nil && *part.ETag != "" {
						etag = strings.Trim(*part.ETag, `"`)
						found = true
					} else {
						lastErr = fmt.Errorf("part %d found but ETag empty", partNum)
					}
					break
				}
			}
			if found || lastErr != nil {
				break
			}
		}

		<-listPartsSem // 释放并发令牌

		if found && etag != "" {
			return etag, nil
		}

		if attempt == maxAttempts {
			if lastErr != nil {
				return "", fmt.Errorf("part %d not found in ListParts after %d attempts: %w", partNum, attempt, lastErr)
			}
			return "", fmt.Errorf("part %d not found in ListParts after %d attempts", partNum, attempt)
		}

		// 退避时间：1s, 2s, 3s，之后固定3s
		sleep := 1 * time.Second
		if attempt == 2 {
			sleep = 2 * time.Second
		} else if attempt >= 3 {
			sleep = 3 * time.Second
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("part %d not found in ListParts (attempts=%d): %w", partNum, attempt, ctx.Err())
		case <-time.After(sleep):
		}
	}

	return "", fmt.Errorf("part %d not found in ListParts", partNum)
}

// maybeNeedsPresignedGet returns true if srcURL looks like an R2 object URL without presigned query params.
func maybeNeedsPresignedGet(srcURL string) bool {
	u, err := url.Parse(srcURL)
	if err != nil {
		return false
	}
	// Already presigned?
	q := u.Query()
	if q.Has("X-Amz-Signature") || q.Has("X-Amz-Credential") || q.Has("X-Amz-Algorithm") {
		return false
	}
	host := strings.ToLower(u.Host)
	// Heuristic: R2 object URLs are on *.r2.cloudflarestorage.com
	return strings.Contains(host, "r2.cloudflarestorage.com")
}

// presignR2GetObjectURL derives (bucket,key) from an R2 path-style object URL and generates a presigned GET URL.
// Expected srcURL form: https://<account>.r2.cloudflarestorage.com/<bucket>/<key...>
func presignR2GetObjectURL(ctx context.Context, srcURL string) (string, error) {
	u, err := url.Parse(srcURL)
	if err != nil {
		return "", err
	}
	path := strings.TrimPrefix(u.Path, "/")
	if path == "" {
		return "", fmt.Errorf("empty path in srcURL")
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("cannot derive bucket/key from srcURL path: %s", u.Path)
	}
	bkt := parts[0]
	objKey := parts[1]

	req, err := s3PresignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bkt),
		Key:    aws.String(objKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func reportError(id int64, msg string) {
	log.Printf("Task %d FAILED: %s", id, msg)
	TasksProcessed.WithLabelValues("failed").Inc()
	updateTask(UpdateTaskRequest{
		ID:             id,
		Status:         "FAILED",
		ErrorMessage:   msg,
		IsDownloadFail: true,
	})
}

// reportErrorWithTask 报告错误并更新数据库（需要完整的任务信息）
func reportErrorWithTask(t YoutubeTask, msg string) {
	reportError(t.ID, msg)
	// 更新数据库记录
	updateTaskRecordInDB(t, "FAILED", msg)
}

func updateTaskStatus(id int64, status string) {
	updateTask(UpdateTaskRequest{
		ID:     id,
		Status: status,
	})
}

// updateTaskStatusWithTask 更新任务状态并更新数据库（需要完整的任务信息）
func updateTaskStatusWithTask(t YoutubeTask, status string) {
	updateTaskStatus(t.ID, status)
	// 更新数据库记录
	updateTaskRecordInDB(t, status, "")
}

func updateTask(req UpdateTaskRequest) {
	// We should probably batch this, but for now single update
	wrapper := []UpdateTaskRequest{req}
	body, err := json.Marshal(wrapper)
	if err != nil {
		log.Printf("Failed to marshal task update request: %v", err)
		return
	}

	resp, err := internalClient.Post(apiBaseURL+"/tasks/update", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to update task %d status to %s: %v", req.ID, req.Status, err)
		return
	}
	defer resp.Body.Close()

	// Read response body for debugging
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to update task %d status to %s: HTTP %d, response: %s",
			req.ID, req.Status, resp.StatusCode, string(respBody))
		return
	}

	log.Printf("Successfully updated task %d status to %s", req.ID, req.Status)
}

// updateTaskRecordInDB 更新数据库中的任务记录（youtube_task_records 表）
func updateTaskRecordInDB(t YoutubeTask, status, errorMessage string) {
	// 准备更新数据库的数据
	taskRecord := map[string]interface{}{
		"id":            t.ID,
		"job_id":        t.JobID,
		"status":        status,
		"error_message": errorMessage,
		"worker_id":     workerID,
	}

	// 如果任务有其他信息，也一并更新
	if t.Title != "" {
		taskRecord["title"] = t.Title
	}
	if t.VideoID != "" {
		taskRecord["video_id"] = t.VideoID
	}
	if t.AudioURL != "" {
		taskRecord["audio_url"] = t.AudioURL
	}
	if t.AudioSize > 0 {
		taskRecord["audio_size"] = t.AudioSize
	}
	if t.VideoURL != "" {
		taskRecord["video_url"] = t.VideoURL
	}
	if t.VideoSize > 0 {
		taskRecord["video_size"] = t.VideoSize
	}

	body, err := json.Marshal(taskRecord)
	if err != nil {
		log.Printf("Failed to marshal task record for DB update (task %d): %v", t.ID, err)
		return
	}

	// 调用后端 API 更新数据库记录
	resp, err := internalClient.Post(apiBaseURL+"/youtube-tasks/update", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to update task record in DB (task %d): %v", t.ID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("Failed to update task record in DB (task %d): HTTP %d, response: %s",
			t.ID, resp.StatusCode, string(respBody))
		return
	}

	log.Printf("Successfully updated task record in DB (task %d, status: %s)", t.ID, status)
}

// getFileSizeFromURL 通过 HTTP HEAD 请求获取文件大小
// 如果无法获取，返回错误
func getFileSizeFromURL(url string) (int64, error) {
	// 对于 YouTube CDN URL，直接尝试 GET Range 请求（HEAD 通常不支持）
	// 添加必要的 headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// 添加 User-Agent 和其他 headers（YouTube CDN 需要）
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "identity")         // 禁用压缩，确保 Content-Length 准确
	req.Header.Set("Referer", "https://www.youtube.com/") // YouTube CDN 可能需要 Referer
	req.Header.Set("Range", "bytes=0-0")                  // 只请求第一个字节，用于获取 Content-Length

	// 使用 externalClient（带代理）来获取文件大小
	// Go 的 http.Client 默认会跟随重定向（最多 10 次）
	resp, err := externalClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get file size: %w", err)
	}
	defer resp.Body.Close()

	// 处理 206 Partial Content（Range 请求的响应）
	if resp.StatusCode == http.StatusPartialContent {
		contentRange := resp.Header.Get("Content-Range")
		if contentRange != "" {
			// Content-Range: bytes 0-0/24134379 或 bytes */24134379
			// 使用 strings 包更可靠地解析
			// 格式：bytes start-end/total 或 bytes */total
			if strings.HasPrefix(contentRange, "bytes ") {
				parts := strings.Split(contentRange, "/")
				if len(parts) == 2 {
					var total int64
					if _, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &total); err == nil && total > 0 {
						return total, nil
					}
				}
			}
			// 如果解析失败，记录详细信息
			return 0, fmt.Errorf("failed to parse Content-Range header: %q (status: %d)", contentRange, resp.StatusCode)
		}
		// 对于 Range 请求，如果没有 Content-Range header，无法确定总大小
		// ContentLength 只表示返回的字节数（通常是 1），不是总文件大小
		return 0, fmt.Errorf("206 Partial Content response missing Content-Range header (status: %d, Content-Length: %d)", resp.StatusCode, resp.ContentLength)
	}

	// 处理 200 OK（某些服务器可能不支持 Range，直接返回完整文件）
	// 注意：如果发送了 Range 请求但收到 200 OK，通常意味着服务器不支持 Range 或返回了完整文件
	if resp.StatusCode == http.StatusOK {
		// 优先尝试从 Content-Range 获取（某些服务器即使返回 200 也会包含 Content-Range）
		contentRange := resp.Header.Get("Content-Range")
		if contentRange != "" {
			// 使用 strings 包解析
			if strings.HasPrefix(contentRange, "bytes ") {
				parts := strings.Split(contentRange, "/")
				if len(parts) == 2 {
					var total int64
					if _, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &total); err == nil && total > 0 {
						return total, nil
					}
				}
			}
		}
		// 如果发送了 Range 请求但收到 200 OK，且 ContentLength 很小（如 1 字节），
		// 这可能是错误响应，不应该信任 ContentLength
		if resp.ContentLength > 0 {
			// 如果 ContentLength 异常小（小于 100 字节），可能是错误响应
			if resp.ContentLength < 100 {
				return 0, fmt.Errorf("received 200 OK with suspiciously small Content-Length: %d bytes (URL may have expired or be invalid)", resp.ContentLength)
			}
			return resp.ContentLength, nil
		}
		return 0, fmt.Errorf("200 OK response missing Content-Length header")
	}

	// 处理 302/301 重定向（虽然 client.CheckRedirect 应该已经处理了，但以防万一）
	if resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
		location := resp.Header.Get("Location")
		if location != "" {
			// 递归调用，但限制深度
			return getFileSizeFromURL(location)
		}
	}

	return 0, fmt.Errorf("unexpected status code: %d (URL may require authentication or specific headers, or may have expired)", resp.StatusCode)
}
