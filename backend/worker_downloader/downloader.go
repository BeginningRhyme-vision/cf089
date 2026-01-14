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
	bucketName     string
	apiBaseURL     string
	jobCache       sync.Map // map[int64]string (JobID -> R2Prefix)
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
	if cfg.Worker.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.Worker.ProxyURL)
		if err != nil {
			log.Printf("Invalid proxy URL: %v", err)
		} else {
			externalTransport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	externalClient = &http.Client{
		Transport: externalTransport,
		Timeout:   30 * time.Second,
	}
}

const (
	ChunkSize            = 32 * 1024 * 1024 // 32MB
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
	ID       uint   `json:"id"`
	R2Prefix string `json:"r2_prefix"`
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

func getJobPrefix(jobID int64) (string, error) {
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
			ext := "m4a" // Default
			if strings.Contains(t.AudioURL, ".webm") {
				ext = "webm"
			}
			key := fmt.Sprintf("%s%s_audio.%s", prefix, t.VideoID, ext)
			if err := transferFile(t.AudioURL, key, t.AudioSize); err != nil {
				errChan <- fmt.Errorf("audio failed: %w", err)
			}
		}()
	}

	if t.VideoURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ext := "mp4"
			if strings.Contains(t.VideoURL, ".webm") {
				ext = "webm"
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
	if err == nil {
		log.Printf("Successfully uploaded: %s", key)
	}

	return err
}

func uploadChunkExternal(srcURL, key, uploadID string, partNum int32, start, end int64) (string, error) {
	// Construct Presigned URL for the destination?
	// The Python script: presigned_url = f"{parsed_url.scheme}://{R2_BUCKET_NAME}.{parsed_url.netloc}/{key}"
	// This format implies R2 virtual host style.
	// We need to match what the External Service expects.
	// Let's replicate the python logic exactly.

	u, _ := http.NewRequest("GET", cfg.Storage.Src.Endpoint, nil)
	// host: <account>.r2.cloudflarestorage.com
	// scheme: https

	destURL := fmt.Sprintf("%s://%s.%s/%s", u.URL.Scheme, bucketName, u.URL.Host, key)

	payload := map[string]interface{}{
		"fileUrl":    srcURL,
		"offset":     start,
		"size":       end - start + 1,
		"r2Key":      destURL, // Python sent destinationUrl here
		"partNumber": partNum,
		"uploadId":   uploadID,
	}

	body, _ := json.Marshal(payload)

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
	return "", fmt.Errorf("failed to upload chunk %d", partNum)
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
