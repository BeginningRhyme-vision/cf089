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
	cfg            *Config
	s3Client       *s3.Client
	s3PresignClient *s3.PresignClient
	bucketName     string
	apiBaseURL     string
	jobCache       sync.Map // map[int64]JobInfo (JobID -> JobInfo)
	workerID       string
	internalClient *http.Client
	externalClient *http.Client

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
	externalClient = &http.Client{
		Transport: externalTransport,
		Timeout:   30 * time.Second,
	}
}

const (
	ChunkSize            = 16 * 1024 * 1024 // 32MB
	MaxConcurrentWorkers = 200
	TaskBufferSize       = 200
)

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

	log.Printf("Go Downloader Worker Started as %s", workerID)

	taskChan := make(chan YoutubeTask, TaskBufferSize)

	// Start Fetcher
	go func() {
		for {
			// Backpressure: if channel is mostly full, wait a bit
			if len(taskChan) >= TaskBufferSize-20 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			tasks, err := acquireTasks()
			if err != nil {
				log.Printf("Error acquiring tasks: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if len(tasks) == 0 {
				time.Sleep(2 * time.Second)
				continue
			}

			log.Printf("Acquired %d tasks", len(tasks))
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
	// Locate config.yaml (assuming run from backend/worker_downloader)
	paths := []string{"../../config.yaml", "../config.yaml", "config.yaml"}
	var data []byte
	var err error
	for _, p := range paths {
		data, err = os.ReadFile(p)
		if err == nil {
			break
		}
	}
	if data == nil {
		log.Fatal("Could not find config.yaml")
	}

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
}

func acquireTasks() ([]YoutubeTask, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"worker_id": workerID,
		"stage":     "download",
		"limit":     TaskBufferSize,
	})

	resp, err := internalClient.Post(apiBaseURL+"/tasks/acquire", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []YoutubeTask
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
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
		return val.(string), nil
	}

	resp, err := internalClient.Get(fmt.Sprintf("%s/youtube-jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("job not found")
	}

	var job JobInfo
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return "", err
	}

	jobCache.Store(jobID, job.R2Prefix)
	return job.R2Prefix, nil
}

func processTask(t YoutubeTask) {
	start := time.Now()
	defer func() {
		DownloadDuration.Observe(time.Since(start).Seconds())
	}()

	updateTaskStatus(t.ID, "RUNNING")
	log.Printf("Task %d (%s) RUNNING", t.ID, t.VideoID)

	prefix, err := getJobPrefix(t.JobID)
	if err != nil {
		reportError(t.ID, "Failed to get job info: "+err.Error())
		return
	}

	// Ensure trailing slash
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	if t.AudioURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ext := jobInfo.AudioExtension

			var key string
			customKey := generateFilename(jobInfo.FilenameTemplate, jobInfo.R2Prefix, t.VideoID, t.Title, ext)
			if customKey != "" {
				key = customKey
			} else {
				key = fmt.Sprintf("%s%s_audio.%s", prefix, t.VideoID, ext)
			}

			if err := transferFile(t.AudioURL, key, t.AudioSize); err != nil {
				errChan <- fmt.Errorf("audio failed: %w", err)
			}
		}()
	}

	if t.VideoURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ext := jobInfo.VideoExtension

			var key string
			customKey := generateFilename(jobInfo.FilenameTemplate, jobInfo.R2Prefix, t.VideoID, t.Title, ext)
			if customKey != "" {
				key = customKey
			} else {
				key = fmt.Sprintf("%s%s_video.%s", prefix, t.VideoID, ext)
			}
			key := fmt.Sprintf("%s%s_video.%s", prefix, t.VideoID, ext)
			if err := transferFile(t.VideoURL, key, t.VideoSize); err != nil {
				errChan <- fmt.Errorf("video failed: %w", err)
			}
		}()
	}

	wg.Wait()
	close(errChan)

	var errs []string
	for e := range errChan {
		errs = append(errs, e.Error())
	}

	if len(errs) > 0 {
		reportError(t.ID, strings.Join(errs, "; "))
	} else {
		updateTaskStatus(t.ID, "COMPLETED")
		TasksProcessed.WithLabelValues("success").Inc()
		log.Printf("Task %d (%s) COMPLETED", t.ID, t.VideoID)
	}
}

func transferFile(sourceURL, key string, providedSize int64) error {
	size := providedSize

	if size <= 0 {
		return fmt.Errorf("invalid content length: %d", size)
	}

	// 2. Initiate Multipart
	createOut, err := s3Client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	uploadID := *createOut.UploadId

	// 3. Upload Parts
	numParts := int(math.Ceil(float64(size) / float64(ChunkSize)))
	var completedParts []types.CompletedPart
	var partsMu sync.Mutex
	var wg sync.WaitGroup
	errAbort := make(chan error, 1)

	sem := make(chan struct{}, 5) // Limit concurrency per file

	for i := 0; i < numParts; i++ {
		start := int64(i) * ChunkSize
		end := start + ChunkSize - 1
		if end >= size {
			end = size - 1
		}
		partNum := int32(i + 1)

		wg.Add(1)
		go func(pNum int32, s, e int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Check if aborted
			select {
			case <-errAbort:
				return
			default:
			}
			time.Sleep(100 * time.Millisecond)
			etag, err := uploadChunkExternal(sourceURL, key, uploadID, pNum, s, e)
			if err != nil {
				select {
				case errAbort <- err:
				default:
				}
				return
			}

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

	// Verify all parts were uploaded
	if len(completedParts) != numParts {
		s3Client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		return fmt.Errorf("incomplete upload: expected %d parts, got %d parts", numParts, len(completedParts))
	}

	// Sort parts
	for i := 0; i < len(completedParts); i++ {
		for j := i + 1; j < len(completedParts); j++ {
			if *completedParts[i].PartNumber > *completedParts[j].PartNumber {
				completedParts[i], completedParts[j] = completedParts[j], completedParts[i]
			}
		}
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
	log.Printf("Successfully uploaded: %s", key)
	return nil
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
		// Set expiration time (default is 15 minutes, we'll use 1 hour for safety)
		opts.Expires = time.Duration(1 * time.Hour)
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL for part %d: %w", partNum, err)
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

	payload := map[string]interface{}{
		"fileUrl":    srcURL,
		"offset":     start,
		"size":       end - start + 1,
		"r2Key":      presignedURL, // Send presigned URL instead of plain URL
		"headers":    signedHeaders,
		"partNumber": partNum,
		"uploadId":   uploadID,
	}

	body, _ := json.Marshal(payload)

	// Log request details in JSON format
	requestLog := map[string]interface{}{
		"url":     cfg.Storage.DownloadServiceURL,
		"method":  "POST",
		"headers": map[string]string{
			"Content-Type": "application/json",
		},
		"body": payload,
	}
	requestLogJSON, _ := json.MarshalIndent(requestLog, "", "  ")
	log.Printf("[REQUEST DEBUG] Chunk %d - Request details:\n%s", partNum, string(requestLogJSON))

	// Retry logic
	var lastErr error
	for i := 0; i < 12; i++ {
		resp, err := externalClient.Post(cfg.Storage.DownloadServiceURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			lastErr = err
			log.Printf("Chunk %d retry %d error: %v", partNum, i+1, err)
			time.Sleep(1 * time.Second)
			continue
		}

		etag := ""
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var resMap map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&resMap); err == nil {
				if val, ok := resMap["etag"].(string); ok {
					etag = val
				}
			}
		} else {
			lastErr = fmt.Errorf("HTTP status %d", resp.StatusCode)
			log.Printf("Chunk %d retry %d status: %d", partNum, i+1, resp.StatusCode)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if etag != "" {
			return etag, nil
		}
		time.Sleep(1 * time.Second)
	}
	if lastErr != nil {
		return "", fmt.Errorf("failed to upload chunk %d: %v", partNum, lastErr)
	}
	return "", fmt.Errorf("failed to upload chunk %d,Can't get Etag", partNum)
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

func updateTaskStatus(id int64, status string) {
	updateTask(UpdateTaskRequest{
		ID:     id,
		Status: status,
	})
}

func updateTask(req UpdateTaskRequest) {
	// We should probably batch this, but for now single update
	wrapper := []UpdateTaskRequest{req}
	body, _ := json.Marshal(wrapper)
	resp, err := internalClient.Post(apiBaseURL+"/tasks/update", "application/json", bytes.NewBuffer(body))
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
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
