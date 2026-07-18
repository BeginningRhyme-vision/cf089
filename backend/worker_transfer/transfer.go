package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
)

// Transfer Worker
// 1. Acquire Tasks (Transfer)
// 2. Transfer using external service

type TransferTask struct {
	ID           int64  `json:"id"`
	JobID        int64  `json:"job_id"`
	Src          string `json:"src"`
	Size         int64  `json:"size"`
	RunToken     string `json:"run_token"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message"`
	WorkerID     string `json:"worker_id"`
}

type JobInfo struct {
	JobID        uint             `json:"job_id"`
	Metadata     TransferMetadata `json:"metadata"`
	DstDir       string           `json:"dst_dir"`
	SrcDir       string           `json:"src_dir"`
	DeleteSource bool             `json:"delete_source"`
}

type cachedJob struct {
	info   JobInfo
	expiry time.Time
}

type JobStatsDelta struct {
	Success int
	Failed  int
}

type transferServiceError struct {
	statusCode int
	message    string
	body       string
	code       string
	stage      string
	retryable  bool
}

func (e *transferServiceError) Error() string {
	if e == nil {
		return ""
	}
	if e.statusCode > 0 {
		if e.body != "" {
			return fmt.Sprintf("transfer service status %d: %s", e.statusCode, e.body)
		}
		return fmt.Sprintf("transfer service status %d: %s", e.statusCode, e.message)
	}
	if e.body != "" {
		return e.body
	}
	return e.message
}

var (
	jobCache              sync.Map // JobID -> cachedJob
	httpClient            *http.Client
	transferClient        *http.Client
	transferProtoLog      sync.Once
	s3ProtoLog            sync.Once
	workerID              string
	workerCount           int
	taskBufferSize        int
	partConcurrency       int
	resumeFailStreakLimit int
	listPartsTimeout      time.Duration
	listPartsRetryCount   int
	multipartThreshold    int64
	minPartSize           int64

	s3Clients sync.Map // Endpoint -> *s3.Client (Cache for Destinations)
	srcClient *s3.Client

	statsBuffer = make(map[int64]*JobStatsDelta)
	statsMutex  sync.Mutex

	// Metrics
	BytesTransferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "transfer_bytes_transferred_total",
		Help: "Total bytes transferred",
	})

	TasksTransferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "transfer_tasks_transferred_total",
		Help: "Total tasks processed",
	}, []string{"status"})

	TransferDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "transfer_duration_seconds",
		Help: "Duration of transfers",
	})
)

const (
	WorkerID                     = "go-transfer-1"
	DefaultConcurrentWorkers     = 64
	DefaultTaskBufferSize        = 128
	DefaultPartConcurrency       = 16
	DefaultMultipartThresholdMB  = 8
	DefaultMinPartSizeMB         = 5
	DefaultTransferMaxConns      = 384
	DefaultTransferTimeoutSec    = 120
	DefaultTransferTLSHandshake  = 10
	DefaultResumeFailStreakLimit = 5
	DefaultListPartsTimeoutSec   = 60
	DefaultListPartsRetryCount   = 2
	ListPartsRetryBackoffBase    = 3
	WorkerHeartbeatInterval      = 30 * time.Second
	ActiveTouchInterval          = 5 * time.Second
	TransferAttemptLimit         = 2
)

func buildWorkerID() string {
	base := strings.TrimSpace(os.Getenv("TRANSFER_WORKER_ID"))
	if base == "" {
		base = WorkerID
	}

	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown-host"
	}

	return fmt.Sprintf("%s@%s-%d", base, host, time.Now().UTC().UnixNano())
}

func runTransfer() {
	loadConfig()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9094")
		http.ListenAndServe(":9094", nil)
	}()

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}
	workerID = buildWorkerID()
	workerCount = getEnvInt("TRANSFER_MAX_WORKERS", DefaultConcurrentWorkers)
	taskBufferSize = getEnvInt("TRANSFER_TASK_BUFFER", DefaultTaskBufferSize)
	partConcurrency = getEnvInt("TRANSFER_PART_CONCURRENCY", DefaultPartConcurrency)
	resumeFailStreakLimit = getEnvInt("TRANSFER_RESUME_FAIL_STREAK_LIMIT", DefaultResumeFailStreakLimit)
	listPartsTimeout = time.Duration(getEnvInt("TRANSFER_LIST_PARTS_TIMEOUT_SECONDS", DefaultListPartsTimeoutSec)) * time.Second
	listPartsRetryCount = getEnvInt("TRANSFER_LIST_PARTS_RETRY_COUNT", DefaultListPartsRetryCount)
	if listPartsRetryCount < 0 {
		listPartsRetryCount = 0
	}
	multipartThreshold = int64(getEnvInt("TRANSFER_MULTIPART_THRESHOLD_MB", DefaultMultipartThresholdMB)) * 1024 * 1024
	minPartSize = int64(getEnvInt("TRANSFER_MIN_PART_SIZE_MB", DefaultMinPartSizeMB)) * 1024 * 1024
	httpClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
	transferTimeout := time.Duration(getEnvInt("TRANSFER_SERVICE_TIMEOUT_SECONDS", DefaultTransferTimeoutSec)) * time.Second
	transferMaxConnsPerHost := getEnvInt("TRANSFER_MAX_CONNS_PER_HOST", DefaultTransferMaxConns)
	transferForceHTTP2 := getEnvBool("TRANSFER_FORCE_HTTP2", false)
	transferTransport := &http.Transport{
		MaxIdleConns:        384,
		MaxIdleConnsPerHost: 256,
		MaxConnsPerHost:     transferMaxConnsPerHost,
		ForceAttemptHTTP2:   transferForceHTTP2,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: DefaultTransferTLSHandshake * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	transferClient = &http.Client{
		Timeout:   transferTimeout,
		Transport: &protocolLoggingRoundTripper{base: transferTransport, once: &transferProtoLog, label: "Transfer service"},
	}
	log.Printf("Transfer client config: force_http2=%t max_conns_per_host=%d timeout=%s", transferForceHTTP2, transferMaxConnsPerHost, transferTimeout)
	log.Printf("ListParts config: timeout=%s retry_count=%d backoff_base=%d", listPartsTimeout, listPartsRetryCount, ListPartsRetryBackoffBase)

	initSourceClient()
	initStatsFlusher()
	initWorkerHeartbeat()

	log.Printf("Transfer Worker Started (worker_id=%s)", workerID)

	taskChan := make(chan TransferTask, taskBufferSize)

	// Start Fetcher
	go func() {
		for {
			// Backpressure: if channel is mostly full, wait a bit
			if len(taskChan) >= taskBufferSize-20 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			tasks, err := acquireTasks()
			if err != nil {
				log.Printf("Error acquiring tasks: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if len(tasks) == 0 {
				time.Sleep(1 * time.Second)
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
	for i := 0; i < workerCount; i++ {
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

func initStatsFlusher() {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			flushStats()
		}
	}()
}

func initWorkerHeartbeat() {
	go func() {
		ticker := time.NewTicker(WorkerHeartbeatInterval)
		defer ticker.Stop()
		for {
			if err := reportTransferWorkerHeartbeat(); err != nil {
				log.Printf("Failed to report transfer worker heartbeat for %s: %v", workerID, err)
			}
			<-ticker.C
		}
	}()
}

func flushStats() {
	statsMutex.Lock()
	if len(statsBuffer) == 0 {
		statsMutex.Unlock()
		return
	}

	snapshot := make(map[int64]JobStatsDelta)
	for k, v := range statsBuffer {
		if v.Success > 0 || v.Failed > 0 {
			snapshot[k] = *v
		}
	}
	// Reset buffer
	statsBuffer = make(map[int64]*JobStatsDelta)
	statsMutex.Unlock()

	for jobID, delta := range snapshot {
		sendJobStatsUpdate(jobID, delta.Success, delta.Failed)
	}
}

func updateJobStats(jobID int64, incSuccess, incFailed int) {
	// Transfer job counters are now driven by task status transitions in backend.
	_ = jobID
	_ = incSuccess
	_ = incFailed
}

type UpdateJobStatusRequest struct {
	Status        string     `json:"status,omitempty"`
	LastScanTime  *time.Time `json:"last_scan_time,omitempty"`
	ResultMessage string     `json:"result_message,omitempty"`
	IncSuccess    int        `json:"inc_success,omitempty"`
	IncFailed     int        `json:"inc_failed,omitempty"`
}

func sendJobStatsUpdate(jobID int64, incSuccess, incFailed int) {
	reqPayload := UpdateJobStatusRequest{
		IncSuccess: incSuccess,
		IncFailed:  incFailed,
	}
	data, _ := json.Marshal(reqPayload)

	req, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to update job stats for job %d: %v", jobID, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Printf("Failed to update job stats for job %d: status %d", jobID, resp.StatusCode)
		return
	}
	drainAndCloseResponseBody(resp)
}

func drainAndCloseResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	resp.Body = http.NoBody
}

func processTask(t TransferTask) {
	start := time.Now()

	job, err := getJobInfo(t.JobID)
	if err != nil {
		log.Printf("Failed to get job info for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", err.Error())
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	srcDir := strings.TrimSpace(job.SrcDir)
	dstDir := strings.TrimSpace(job.DstDir)

	if err := markTaskActive(t); err != nil {
		log.Printf("Failed to activate task %d/%d: %v", t.JobID, t.ID, err)
	}
	stopTouch := startActiveTaskToucher(t)
	defer stopTouch()

	log.Printf("Processing Task %d (Job %d): %s -> %s", t.ID, t.JobID, t.Src, dstDir)

	// 1. Resolve Dst Client
	sk := job.Metadata.SKEncrypted
	if strings.HasPrefix(sk, "enc_") {
		sk = strings.TrimPrefix(sk, "enc_")
	}

	dstClient, err := createDestS3Client(job.Metadata.Endpoint, job.Metadata.AK, sk)
	if err != nil {
		log.Printf("Dst client init failed for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", "Dst client init failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	// 2. Resolve Paths
	srcKey := t.Src
	relKey := buildRelativeKey(srcDir, srcKey)

	dstKey := relKey
	if dstDir != "" {
		if dstKey == "" {
			dstKey = dstDir
		} else {
			dstKey = strings.TrimSuffix(dstDir, "/") + "/" + dstKey
		}
	}

	// 3. Get Object Info (Size) from Src
	srcBucket := getBucketFromEndpoint(cfg.Storage.Src.Endpoint)
	dstBucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	head, err := srcClient.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(srcKey),
	})
	if err != nil {
		log.Printf("HeadObject failed for %s/%s: %v", srcBucket, srcKey, err)
		errMsg := "HeadObject failed: " + err.Error()
		if isSourceHeadNotFound(err) {
			log.Printf("Task %d/%d source object missing at %s/%s; cleaning multipart artifacts before marking FAILED", t.JobID, t.ID, srcBucket, srcKey)
			if cleanupErr := cleanupMultipartArtifactsForTask(t, dstClient, dstBucket, dstKey); cleanupErr != nil {
				errMsg = "SourceMissing cleanup failed: " + cleanupErr.Error()
			} else {
				errMsg = "SourceNotFound: source head returned 404"
			}
		}
		updateTaskStatus(t, "FAILED", errMsg)
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}
	size := aws.ToInt64(head.ContentLength)
	sourceETag := aws.ToString(head.ETag)

	// 4. Construct Public/Virtual-Hosted URLs for Transfer Service (Matches r2s3.go logic)
	srcUrl, err := constructVirtualHostURL(cfg.Storage.Src.Endpoint, srcBucket, srcKey)
	if err != nil {
		log.Printf("Failed to construct Src URL for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", "Construct Src URL failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	log.Printf("Task %d: Transferring %d bytes to bucket '%s' key '%s'", t.ID, size, dstBucket, dstKey)

	// 5. Transfer Loop
	err = transferFile(t, srcUrl, dstClient, dstBucket, dstKey, size, job.Metadata.Endpoint, sourceETag)
	if err != nil {
		log.Printf("Transfer failed for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", err.Error())
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
	} else {
		log.Printf("Task %d completed successfully", t.ID)

		if job.DeleteSource {
			_, err := srcClient.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(srcBucket),
				Key:    aws.String(srcKey),
			})
			if err != nil {
				log.Printf("Failed to delete source object %s for task %d: %v", srcKey, t.ID, err)
			} else {
				log.Printf("Deleted source object %s for task %d", srcKey, t.ID)
			}
		}

		if err := updateTaskStatus(t, "COMPLETED", ""); err != nil {
			log.Printf("Failed to update COMPLETED status for task %d/%d: %v", t.JobID, t.ID, err)
			if reportErr := reportCompletionCompensation(t, size, dstBucket, dstKey, "final_status_update_failed"); reportErr != nil {
				log.Printf("Failed to report completion compensation for task %d/%d: %v", t.JobID, t.ID, reportErr)
			}
		}
		updateJobStats(t.JobID, 1, 0)

		// Metrics
		duration := time.Since(start).Seconds()
		TransferDuration.Observe(duration)
		BytesTransferred.Add(float64(size))
		TasksTransferred.WithLabelValues("success").Inc()
	}
}

func buildRelativeKey(srcDir, srcKey string) string {
	srcDir = strings.Trim(strings.TrimSpace(srcDir), "/")
	srcKey = strings.TrimPrefix(srcKey, "/")

	if srcDir == "" {
		return srcKey
	}
	if srcKey == srcDir {
		return ""
	}

	dirPrefix := srcDir + "/"
	if strings.HasPrefix(srcKey, dirPrefix) {
		return strings.TrimPrefix(srcKey, dirPrefix)
	}

	return srcKey
}

type multipartCheckpoint struct {
	JobID                  int64     `json:"job_id"`
	TaskID                 int64     `json:"task_id"`
	Src                    string    `json:"src"`
	Size                   int64     `json:"size"`
	SourceETag             string    `json:"source_etag"`
	SrcIdentity            string    `json:"src_identity"`
	DstBucket              string    `json:"dst_bucket"`
	DstKey                 string    `json:"dst_key"`
	UploadID               string    `json:"upload_id"`
	PartSize               int64     `json:"part_size"`
	NumParts               int       `json:"num_parts"`
	AttemptCount           int       `json:"attempt_count"`
	LastRunToken           string    `json:"last_run_token"`
	LastError              string    `json:"last_error"`
	ResumeFailStreak       int       `json:"resume_fail_streak"`
	LastKnownUploadedParts int       `json:"last_known_uploaded_parts"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

func buildSrcIdentity(srcURL string, size int64, sourceETag string) string {
	sourceETag = strings.Trim(strings.TrimSpace(sourceETag), `"`)
	if sourceETag == "" {
		return fmt.Sprintf("%s|%d", srcURL, size)
	}
	return fmt.Sprintf("%s|%d|%s", srcURL, size, sourceETag)
}

func loadMultipartCheckpoint(jobID, taskID int64) (*multipartCheckpoint, error) {
	payload := map[string]int64{
		"job_id":  jobID,
		"task_id": taskID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint load payload: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/checkpoint/load", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("post checkpoint load: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("checkpoint load returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result struct {
		Found      bool                `json:"found"`
		Checkpoint multipartCheckpoint `json:"checkpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode checkpoint load response: %w", err)
	}
	if !result.Found {
		return nil, nil
	}
	return &result.Checkpoint, nil
}

func saveMultipartCheckpoint(ckpt *multipartCheckpoint) error {
	if ckpt == nil {
		return nil
	}
	data, err := json.Marshal(ckpt)
	if err != nil {
		return fmt.Errorf("marshal checkpoint save payload: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/checkpoint/save", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post checkpoint save: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("checkpoint save returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	drainAndCloseResponseBody(resp)
	return nil
}

func clearMultipartCheckpoint(t TransferTask) error {
	payload := map[string]any{
		"job_id":    t.JobID,
		"task_id":   t.ID,
		"run_token": t.RunToken,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal checkpoint clear payload: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/checkpoint/clear", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post checkpoint clear: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("checkpoint clear returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	drainAndCloseResponseBody(resp)
	return nil
}

func shouldReuseMultipartCheckpoint(ckpt *multipartCheckpoint, srcIdentity, dstBucket, dstKey string, size, partSize int64, numParts int) bool {
	if ckpt == nil {
		return false
	}
	if strings.TrimSpace(ckpt.UploadID) == "" {
		return false
	}
	if ckpt.SrcIdentity != srcIdentity {
		return false
	}
	if ckpt.Size != size {
		return false
	}
	if ckpt.DstBucket != dstBucket || ckpt.DstKey != dstKey {
		return false
	}
	if ckpt.PartSize != partSize || ckpt.NumParts != numParts {
		return false
	}
	return true
}

func listUploadedParts(ctx context.Context, dstClient *s3.Client, dstBucket, dstKey, uploadID string) (map[int32]types.CompletedPart, error) {
	result := make(map[int32]types.CompletedPart)
	paginator := s3.NewListPartsPaginator(dstClient, &s3.ListPartsInput{
		Bucket:   aws.String(dstBucket),
		Key:      aws.String(dstKey),
		UploadId: aws.String(uploadID),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, part := range page.Parts {
			if part.PartNumber == nil || part.ETag == nil {
				continue
			}
			result[*part.PartNumber] = types.CompletedPart{
				ETag:       part.ETag,
				PartNumber: part.PartNumber,
			}
		}
	}

	return result, nil
}

func countCompletedParts(parts []types.CompletedPart) int {
	count := 0
	for _, part := range parts {
		if part.PartNumber != nil && part.ETag != nil && strings.TrimSpace(*part.ETag) != "" {
			count++
		}
	}
	return count
}

func abortMultipartUpload(dstClient *s3.Client, dstBucket, dstKey, uploadID string) error {
	if strings.TrimSpace(uploadID) == "" {
		return nil
	}
	abortCtx, abortCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer abortCancel()
	if _, err := dstClient.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(dstBucket),
		Key:      aws.String(dstKey),
		UploadId: aws.String(uploadID),
	}); err != nil {
		return err
	}
	return nil
}

func abortMultipartUploadBestEffort(dstClient *s3.Client, dstBucket, dstKey, uploadID string) {
	if err := abortMultipartUpload(dstClient, dstBucket, dstKey, uploadID); err != nil {
		log.Printf("AbortMultipartUpload failed for %s/%s upload=%s: %v", dstBucket, dstKey, uploadID, err)
	}
}

func abandonMultipartCheckpoint(t TransferTask, dstClient *s3.Client, dstBucket, dstKey, uploadID, reason string) error {
	log.Printf("Abandoning multipart upload for task %d/%d reason=%s upload_id=%s bucket=%s key=%s",
		t.JobID, t.ID, reason, uploadID, dstBucket, dstKey)
	if err := abortMultipartUpload(dstClient, dstBucket, dstKey, uploadID); err != nil {
		log.Printf("Failed to abort multipart upload for task %d/%d reason=%s upload_id=%s; preserving checkpoint for inspection/retry: %v",
			t.JobID, t.ID, reason, uploadID, err)
		return fmt.Errorf("abort multipart upload_id=%s reason=%s: %w", uploadID, reason, err)
	}
	log.Printf("Aborted multipart upload for task %d/%d reason=%s upload_id=%s", t.JobID, t.ID, reason, uploadID)
	if clearErr := clearMultipartCheckpoint(t.JobID, t.ID); clearErr != nil {
		log.Printf("Aborted multipart upload for task %d/%d upload_id=%s but failed to clear checkpoint: %v",
			t.JobID, t.ID, uploadID, clearErr)
		return nil
	}
	log.Printf("Cleared multipart checkpoint for task %d/%d reason=%s upload_id=%s", t.JobID, t.ID, reason, uploadID)
	return nil
}

func cleanupMultipartArtifactsForTask(t TransferTask, dstClient *s3.Client, fallbackBucket, fallbackKey string) error {
	ckpt, err := loadMultipartCheckpoint(t.JobID, t.ID)
	if err != nil {
		log.Printf("Failed to load multipart checkpoint for cleanup task %d/%d: %v", t.JobID, t.ID, err)
		return err
	}
	if ckpt == nil {
		return nil
	}

	dstBucket := fallbackBucket
	if strings.TrimSpace(ckpt.DstBucket) != "" {
		dstBucket = ckpt.DstBucket
	}
	dstKey := fallbackKey
	if strings.TrimSpace(ckpt.DstKey) != "" {
		dstKey = ckpt.DstKey
	}

	if err := abandonMultipartCheckpoint(t, dstClient, dstBucket, dstKey, ckpt.UploadID, "source_head_not_found"); err != nil {
		log.Printf("Multipart cleanup deferred for task %d/%d after source disappearance: %v", t.JobID, t.ID, err)
		return err
	}
	return nil
}

func isMultipartUploadNotFound(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "nosuchupload") ||
		strings.Contains(lower, "no such upload") ||
		strings.Contains(lower, "status code: 404") ||
		strings.Contains(lower, "statuscode: 404") ||
		strings.Contains(lower, "statuscode:404")
}

func isRetryableListPartsError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		type temporary interface {
			Temporary() bool
		}
		var tempErr temporary
		if errors.As(err, &tempErr) && tempErr.Temporary() {
			return true
		}
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "context deadline exceeded"),
		strings.Contains(lower, "timeout"),
		strings.Contains(lower, "i/o timeout"),
		strings.Contains(lower, "connection reset"),
		strings.Contains(lower, "connection refused"),
		strings.Contains(lower, "connection aborted"),
		strings.Contains(lower, "broken pipe"),
		strings.Contains(lower, "unexpected eof"),
		strings.Contains(lower, "server closed idle connection"),
		strings.Contains(lower, "tls handshake timeout"),
		strings.Contains(lower, "status code: 500"),
		strings.Contains(lower, "status code: 502"),
		strings.Contains(lower, "status code: 503"),
		strings.Contains(lower, "status code: 504"),
		strings.Contains(lower, "statuscode: 500"),
		strings.Contains(lower, "statuscode: 502"),
		strings.Contains(lower, "statuscode: 503"),
		strings.Contains(lower, "statuscode: 504"),
		strings.Contains(lower, "statuscode:500"),
		strings.Contains(lower, "statuscode:502"),
		strings.Contains(lower, "statuscode:503"),
		strings.Contains(lower, "statuscode:504"):
		return true
	default:
		return false
	}
}

func listPartsRetryBackoff(retryNumber int) time.Duration {
	if retryNumber < 1 {
		retryNumber = 1
	}
	backoffSeconds := 1
	for i := 0; i < retryNumber; i++ {
		backoffSeconds *= ListPartsRetryBackoffBase
	}
	return time.Duration(backoffSeconds) * time.Second
}

func waitForListPartsRetry(backoff time.Duration) {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	<-timer.C
}

func listUploadedPartsWithRetry(t TransferTask, dstClient *s3.Client, dstBucket, dstKey, uploadID string) (map[int32]types.CompletedPart, error) {
	var lastErr error
	maxAttempts := listPartsRetryCount + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		listCtx, listCancel := context.WithTimeout(context.Background(), listPartsTimeout)
		parts, err := listUploadedParts(listCtx, dstClient, dstBucket, dstKey, uploadID)
		listCancel()
		if err == nil {
			return parts, nil
		}

		lastErr = err
		if isMultipartUploadNotFound(err) || !isRetryableListPartsError(err) || attempt == maxAttempts {
			return nil, lastErr
		}

		backoff := listPartsRetryBackoff(attempt)
		log.Printf("Retrying ListParts for task %d/%d upload_id=%s after retryable error on attempt=%d/%d backoff=%s: %v",
			t.JobID, t.ID, uploadID, attempt, maxAttempts, backoff, err)
		waitForListPartsRetry(backoff)
	}

	return nil, lastErr
}

func isSourceHeadNotFound(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "nosuchkey") ||
		strings.Contains(lower, "no such key") ||
		strings.Contains(lower, "not found") ||
		strings.Contains(lower, "status code: 404") ||
		strings.Contains(lower, "statuscode: 404") ||
		strings.Contains(lower, "statuscode:404")
}

func transferFile(t TransferTask, srcURL string, dstClient *s3.Client, dstBucket, dstKey string, size int64, dstEndpoint string, sourceETag string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstUrl, err := constructVirtualHostURL(dstEndpoint, dstBucket, dstKey)
	if err != nil {
		return err
	}

	var (
		ckpt             *multipartCheckpoint
		uploadID         string
		uploadedParts    map[int32]types.CompletedPart
		reusedCheckpoint bool
	)

	if loaded, loadErr := loadMultipartCheckpoint(t.JobID, t.ID); loadErr != nil {
		log.Printf("Failed to load multipart checkpoint for task %d/%d: %v", t.JobID, t.ID, loadErr)
	} else {
		ckpt = loaded
	}

	if size < multipartThreshold {
		if ckpt != nil {
			log.Printf("Task %d/%d switched to single-part upload because size=%d is below multipart threshold=%d, cleaning stale multipart upload_id=%s",
				t.JobID, t.ID, size, multipartThreshold, ckpt.UploadID)
			if err := abandonMultipartCheckpoint(t, dstClient, ckpt.DstBucket, ckpt.DstKey, ckpt.UploadID, "single_part_fallback_threshold"); err != nil {
				return err
			}
		}
		_, err = uploadTransferPartWithRetry(ctx, srcURL, dstUrl, size, 0, "", -1)
		return err
	}

	// Multipart
	partSize := calculatePartSize(size)
	numParts := int((size-1)/partSize) + 1
	srcIdentity := buildSrcIdentity(srcURL, size, sourceETag)

	if shouldReuseMultipartCheckpoint(ckpt, srcIdentity, dstBucket, dstKey, size, partSize, numParts) {
		parts, listErr := listUploadedPartsWithRetry(t, dstClient, dstBucket, dstKey, ckpt.UploadID)
		switch {
		case listErr == nil:
			uploadID = ckpt.UploadID
			uploadedParts = parts
			reusedCheckpoint = true
			ckpt.AttemptCount++
			ckpt.LastRunToken = t.RunToken
			ckpt.LastKnownUploadedParts = len(uploadedParts)
			log.Printf("Resuming multipart upload for task %d/%d upload_id=%s uploaded_parts=%d part_size=%d num_parts=%d attempt=%d",
				t.JobID, t.ID, ckpt.UploadID, len(uploadedParts), ckpt.PartSize, ckpt.NumParts, ckpt.AttemptCount)
			if saveErr := saveMultipartCheckpoint(ckpt); saveErr != nil {
				log.Printf("Failed to refresh multipart checkpoint for task %d/%d: %v", t.JobID, t.ID, saveErr)
			}
		case isMultipartUploadNotFound(listErr):
			log.Printf("Multipart checkpoint upload missing for task %d/%d, clearing stale upload_id=%s", t.JobID, t.ID, ckpt.UploadID)
			if clearErr := clearMultipartCheckpoint(t); clearErr != nil {
				log.Printf("Failed to clear stale multipart checkpoint for task %d/%d: %v", t.JobID, t.ID, clearErr)
			}
			ckpt = nil
		default:
			log.Printf("Failed to list multipart parts for task %d/%d upload_id=%s before fresh upload: %v", t.JobID, t.ID, ckpt.UploadID, listErr)
			if err := abandonMultipartCheckpoint(t, dstClient, ckpt.DstBucket, ckpt.DstKey, ckpt.UploadID, "list_parts_error"); err != nil {
				return err
			}
			ckpt = nil
		}
	} else if ckpt != nil {
		log.Printf("Multipart checkpoint mismatch for task %d/%d, abandoning old upload_id=%s", t.JobID, t.ID, ckpt.UploadID)
		if err := abandonMultipartCheckpoint(t, dstClient, ckpt.DstBucket, ckpt.DstKey, ckpt.UploadID, "checkpoint_mismatch"); err != nil {
			return err
		}
		ckpt = nil
	}

	if uploadID == "" {
		createOut, createErr := dstClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(dstBucket),
			Key:    aws.String(dstKey),
		})
		if createErr != nil {
			return createErr
		}
		uploadID = *createOut.UploadId
		log.Printf("Created multipart upload for task %d/%d upload_id=%s bucket=%s key=%s part_size=%d num_parts=%d",
			t.JobID, t.ID, uploadID, dstBucket, dstKey, partSize, numParts)
		uploadedParts = make(map[int32]types.CompletedPart)
		ckpt = &multipartCheckpoint{
			JobID:                  t.JobID,
			TaskID:                 t.ID,
			Src:                    srcURL,
			Size:                   size,
			SourceETag:             strings.Trim(strings.TrimSpace(sourceETag), `"`),
			SrcIdentity:            srcIdentity,
			DstBucket:              dstBucket,
			DstKey:                 dstKey,
			UploadID:               uploadID,
			PartSize:               partSize,
			NumParts:               numParts,
			AttemptCount:           1,
			LastRunToken:           t.RunToken,
			ResumeFailStreak:       0,
			LastKnownUploadedParts: 0,
			CreatedAt:              time.Now().UTC(),
			UpdatedAt:              time.Now().UTC(),
		}
		if saveErr := saveMultipartCheckpoint(ckpt); saveErr != nil {
			log.Printf("Failed to save initial multipart checkpoint for task %d/%d upload_id=%s; aborting fresh multipart upload: %v", t.JobID, t.ID, uploadID, saveErr)
			abortMultipartUploadBestEffort(dstClient, dstBucket, dstKey, uploadID)
			return fmt.Errorf("save initial multipart checkpoint for task %d/%d upload_id=%s: %w", t.JobID, t.ID, uploadID, saveErr)
		}
		log.Printf("Saved initial multipart checkpoint for task %d/%d upload_id=%s", t.JobID, t.ID, uploadID)
	}

	completedParts := make([]types.CompletedPart, numParts)
	for partNumber, part := range uploadedParts {
		idx := int(partNumber) - 1
		if idx >= 0 && idx < len(completedParts) {
			completedParts[idx] = part
		}
	}
	beforeCount := countCompletedParts(completedParts)
	var wg sync.WaitGroup
	var firstErr error
	var failOnce sync.Once

	sem := make(chan struct{}, partConcurrency)

	for i := 0; i < numParts; i++ {
		start := int64(i) * partSize
		end := start + partSize - 1
		if end >= size {
			end = size - 1
		}
		partNum := int32(i + 1)
		if existing := completedParts[i]; existing.PartNumber != nil && existing.ETag != nil {
			continue
		}

		wg.Add(1)
		go func(idx int, pNum int32, s, e int64) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			etag, err := uploadTransferPartWithRetry(ctx, srcURL, dstUrl, e-s+1, s, uploadID, int(pNum))
			if err != nil {
				failOnce.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}

			completedParts[idx] = types.CompletedPart{
				ETag:       aws.String(etag),
				PartNumber: aws.Int32(pNum),
			}
		}(i, partNum, start, end)
	}

	wg.Wait()

	if firstErr != nil {
		if ckpt != nil {
			afterCount := countCompletedParts(completedParts)
			ckpt.LastRunToken = t.RunToken
			ckpt.LastError = firstErr.Error()
			if afterCount > ckpt.LastKnownUploadedParts {
				ckpt.LastKnownUploadedParts = afterCount
			}
			if reusedCheckpoint {
				if afterCount > beforeCount {
					ckpt.ResumeFailStreak = 0
				} else {
					ckpt.ResumeFailStreak++
				}
			} else {
				ckpt.ResumeFailStreak = 0
			}
			if ckpt.ResumeFailStreak >= resumeFailStreakLimit {
				log.Printf("Abandoning multipart checkpoint for task %d/%d after %d no-progress resume failures", t.JobID, t.ID, ckpt.ResumeFailStreak)
				if err := abandonMultipartCheckpoint(t, dstClient, dstBucket, dstKey, uploadID, "resume_fail_streak_exceeded"); err != nil {
					log.Printf("Preserving multipart checkpoint for task %d/%d after abort failure on resume_fail_streak_exceeded: %v", t.JobID, t.ID, err)
					if saveErr := saveMultipartCheckpoint(ckpt); saveErr != nil {
						log.Printf("Failed to persist multipart checkpoint after abort failure for task %d/%d: %v", t.JobID, t.ID, saveErr)
					}
				}
			} else if saveErr := saveMultipartCheckpoint(ckpt); saveErr != nil {
				log.Printf("Failed to persist multipart checkpoint after transfer failure for task %d/%d: %v", t.JobID, t.ID, saveErr)
			}
		}
		return firstErr
	}

	_, err = dstClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err == nil {
		log.Printf("Completed multipart upload for task %d/%d upload_id=%s completed_parts=%d", t.JobID, t.ID, uploadID, countCompletedParts(completedParts))
		if clearErr := clearMultipartCheckpoint(t); clearErr != nil {
			log.Printf("Failed to clear multipart checkpoint after completion for task %d/%d: %v", t.JobID, t.ID, clearErr)
		}
	}
	return err
}

func callTransferService(ctx context.Context, srcUrl, dstUrl string, size, offset int64, uploadID string, partNum int) (string, error) {
	payload := map[string]interface{}{
		"r2Key":      srcUrl,
		"s3Url":      dstUrl,
		"size":       size,
		"offset":     offset,
		"uploadId":   uploadID,
		"partNumber": partNum,
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Storage.TransferServiceURL, bytes.NewBuffer(body))
	if err != nil {
		return "", classifyTransferTransportError(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := transferClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", classifyTransferTransportError(err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", classifyTransferResponseError(resp.StatusCode, string(respBody))
	}

	var res map[string]interface{}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return "", &transferServiceError{
			message:   fmt.Sprintf("decode response failed: %v", err),
			body:      strings.TrimSpace(string(respBody)),
			retryable: false,
		}
	}
	if etag, ok := res["etag"].(string); ok && strings.TrimSpace(etag) != "" {
		return etag, nil
	}

	return "", &transferServiceError{
		message:   "etag missing in response",
		body:      strings.TrimSpace(string(respBody)),
		retryable: false,
	}
}

func uploadTransferPartWithRetry(ctx context.Context, srcUrl, dstUrl string, size, offset int64, uploadID string, partNum int) (string, error) {
	var lastErr error

	for attempt := 1; attempt <= TransferAttemptLimit; attempt++ {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		etag, err := callTransferService(ctx, srcUrl, dstUrl, size, offset, uploadID, partNum)
		if err == nil {
			return etag, nil
		}

		lastErr = err
		if !isRetryableTransferError(err) || attempt == TransferAttemptLimit {
			return "", lastErr
		}

		if err := waitForTransferRetry(ctx, attempt); err != nil {
			return "", err
		}
	}

	return "", lastErr
}

func waitForTransferRetry(ctx context.Context, attempt int) error {
	backoff := time.Duration(attempt) * time.Second
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetryableTransferError(err error) bool {
	if err == nil {
		return false
	}

	var svcErr *transferServiceError
	if errors.As(err, &svcErr) {
		return svcErr.retryable
	}

	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

func classifyTransferTransportError(err error) error {
	if err == nil {
		return nil
	}

	retryable := true
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "unsupported protocol scheme"),
		strings.Contains(lower, "no host in request url"),
		strings.Contains(lower, "invalid control character"),
		strings.Contains(lower, "invalid url escape"),
		strings.Contains(lower, "missing port in address"):
		retryable = false
	case errors.Is(err, context.Canceled):
		retryable = false
	}

	return &transferServiceError{
		message:   fmt.Sprintf("transfer service request failed: %v", err),
		retryable: retryable,
	}
}

func classifyTransferResponseError(statusCode int, body string) error {
	body = strings.TrimSpace(body)
	if parsed, ok := parseStructuredTransferError(body); ok {
		retryable := parsed.Error.Retryable
		if hasFatalTransferBody(parsed.Error.Message) || hasFatalTransferBody(parsed.Error.Code) {
			retryable = false
		}
		return &transferServiceError{
			statusCode: statusCode,
			message:    parsed.Error.Message,
			body:       body,
			code:       parsed.Error.Code,
			stage:      parsed.Error.Stage,
			retryable:  retryable,
		}
	}

	retryable := isRetryableTransferStatus(statusCode)
	if hasFatalTransferBody(body) {
		retryable = false
	}

	return &transferServiceError{
		statusCode: statusCode,
		body:       body,
		retryable:  retryable,
	}
}

func isRetryableTransferStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func hasFatalTransferBody(body string) bool {
	if strings.TrimSpace(body) == "" {
		return false
	}

	lower := strings.ToLower(body)
	fatalMarkers := []string{
		"missing required environment variables",
		"missing required parameters",
		"missing required parameter",
		"signaturedoesnotmatch",
		"invalidaccesskeyid",
		"accessdenied",
		"access denied",
		"forbidden",
		"authorizationheadermalformed",
		"invalidargument",
		"invalidrequest",
		"invalid token",
		"expiredtoken",
		"request has expired",
		"nosuchbucket",
		"no such bucket",
		"nosuchkey",
		"no such key",
		"nosuchupload",
		"entitytoosmall",
		"invalidpart",
		"invalidpartorder",
		"etag missing",
		"no etag found",
	}

	for _, marker := range fatalMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}

	return false
}

type transferErrorEnvelope struct {
	Error struct {
		Code      string `json:"code"`
		Stage     string `json:"stage"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	} `json:"error"`
}

func parseStructuredTransferError(body string) (transferErrorEnvelope, bool) {
	var envelope transferErrorEnvelope
	if strings.TrimSpace(body) == "" {
		return envelope, false
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		return envelope, false
	}
	if strings.TrimSpace(envelope.Error.Code) == "" &&
		strings.TrimSpace(envelope.Error.Stage) == "" &&
		strings.TrimSpace(envelope.Error.Message) == "" {
		return envelope, false
	}
	return envelope, true
}

type protocolLoggingRoundTripper struct {
	base  http.RoundTripper
	once  *sync.Once
	label string
}

func (p *protocolLoggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := p.base.RoundTrip(req)
	if err == nil && resp != nil {
		p.once.Do(func() {
			log.Printf("%s protocol negotiated: %s host=%s", p.label, resp.Proto, req.URL.Host)
		})
	}
	return resp, err
}

func constructVirtualHostURL(endpointStr, bucket, key string) (string, error) {
	// Mimic r2s3.go logic for creating scheme://bucket.host/key

	// Normalize just to parse host/scheme cleanly if needed, but r2s3 uses simple parsing
	normalized := endpointStr
	isS3 := strings.HasPrefix(endpointStr, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(endpointStr, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}

	host := u.Host
	if isS3 {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}

	// r2s3 style: fmt.Sprintf("%s://%s.%s/%s", u.Scheme, srcCfg.Bucket, u.Host, obj.Key)
	return fmt.Sprintf("%s://%s.%s/%s", u.Scheme, bucket, host, key), nil
}

func initSourceClient() {
	c, err := createSourceS3Client(cfg.Storage.Src.Endpoint, cfg.Storage.Src.AccessKey, cfg.Storage.Src.SecretKey)
	if err != nil {
		log.Fatalf("Failed to init source client: %v", err)
	}
	srcClient = c
}

func createSourceS3Client(endpoint, ak, sk string) (*s3.Client, error) {
	return createS3Client(endpoint, ak, sk, 0)
}

func createDestS3Client(endpoint, ak, sk string) (*s3.Client, error) {
	timeoutSeconds := getEnvInt("TRANSFER_DEST_S3_TIMEOUT_SECONDS", 60)
	return createS3Client(endpoint, ak, sk, time.Duration(timeoutSeconds)*time.Second)
}

func createS3Client(endpoint, ak, sk string, requestTimeout time.Duration) (*s3.Client, error) {
	normalized := endpoint
	isS3 := strings.HasPrefix(normalized, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(normalized, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return nil, err
	}

	host := u.Host
	if isS3 {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}

	baseEndpoint := fmt.Sprintf("%s://%s", u.Scheme, host)

	forceHTTP2 := getEnvBool("TRANSFER_FORCE_HTTP2", false)
	s3Transport := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		ForceAttemptHTTP2:   forceHTTP2,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: DefaultTransferTLSHandshake * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	s3HTTPClient := &http.Client{
		Transport: &protocolLoggingRoundTripper{base: s3Transport, once: &s3ProtoLog, label: "S3 client"},
	}
	if requestTimeout > 0 {
		s3HTTPClient.Timeout = requestTimeout
	}

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		awsconfig.WithRegion("auto"),
		awsconfig.WithHTTPClient(s3HTTPClient),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(c, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = false
	}), nil
}

func acquireTasks() ([]TransferTask, error) {
	payload := map[string]interface{}{
		"worker_id": workerID,
		"limit":     taskBufferSize,
	}
	data, _ := json.Marshal(payload)

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/acquire", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []TransferTask
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func getJobInfo(jobID int64) (*JobInfo, error) {
	// Check cache
	if val, ok := jobCache.Load(jobID); ok {
		cj := val.(cachedJob)
		if time.Now().Before(cj.expiry) {
			return &cj.info, nil
		}
	}

	resp, err := httpClient.Get(fmt.Sprintf("%s/jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var job TransferJobStruct
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}

	// Map backend model to JobInfo
	info := JobInfo{
		JobID:        job.JobID,
		Metadata:     job.Metadata,
		DstDir:       job.DstDir,
		SrcDir:       job.SrcDir,
		DeleteSource: job.DeleteSource,
	}

	jobCache.Store(jobID, cachedJob{info: info, expiry: time.Now().Add(1 * time.Minute)})
	return &info, nil
}

// Temporary struct to match backend response for getJobInfo
type TransferJobStruct struct {
	JobID        uint             `json:"job_id"`
	Metadata     TransferMetadata `json:"metadata"`
	DstDir       string           `json:"dst_dir"`
	SrcDir       string           `json:"src_dir"`
	DeleteSource bool             `json:"delete_source"`
}

func updateTaskStatus(t TransferTask, status string, msg string) error {
	t.Status = status
	t.ErrorMessage = msg

	payload := []TransferTask{t}
	data, _ := json.Marshal(payload)

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/update", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post transfer task update: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("transfer task update returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	drainAndCloseResponseBody(resp)
	return nil
}

func markTaskActive(t TransferTask) error {
	type activateRequest struct {
		JobID    int64  `json:"job_id"`
		TaskID   int64  `json:"task_id"`
		RunToken string `json:"run_token"`
		WorkerID string `json:"worker_id"`
	}
	type activateResponse struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}

	payload := activateRequest{
		JobID:    t.JobID,
		TaskID:   t.ID,
		RunToken: t.RunToken,
		WorkerID: workerID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal activate request: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/activate", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post activate request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("activate returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result activateResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode activate response: %w", err)
	}
	if result.Status != "activated" {
		return fmt.Errorf("activate not applied: status=%s reason=%s", result.Status, result.Reason)
	}
	return nil
}

func touchActiveTask(t TransferTask) error {
	payload := map[string]interface{}{
		"job_id":    t.JobID,
		"task_id":   t.ID,
		"run_token": t.RunToken,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal transfer progress payload: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/progress", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post transfer progress: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("transfer progress returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	drainAndCloseResponseBody(resp)
	return nil
}

func startActiveTaskToucher(t TransferTask) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(ActiveTouchInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := touchActiveTask(t); err != nil {
					log.Printf("Failed to touch active task %d/%d: %v", t.JobID, t.ID, err)
				}
			}
		}
	}()
	return func() {
		close(done)
	}
}

func reportTransferWorkerHeartbeat() error {
	payload := map[string]string{
		"worker_id": workerID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal worker heartbeat payload: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/heartbeat", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post worker heartbeat: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("worker heartbeat returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	drainAndCloseResponseBody(resp)
	return nil
}

func reportCompletionCompensation(t TransferTask, size int64, dstBucket, dstKey, reason string) error {
	type completionCompensationRequest struct {
		JobID     int64  `json:"job_id"`
		TaskID    int64  `json:"task_id"`
		RunToken  string `json:"run_token"`
		Src       string `json:"src"`
		WorkerID  string `json:"worker_id"`
		Size      int64  `json:"size"`
		DstBucket string `json:"dst_bucket"`
		DstKey    string `json:"dst_key"`
		Reason    string `json:"reason"`
	}

	payload := completionCompensationRequest{
		JobID:     t.JobID,
		TaskID:    t.ID,
		RunToken:  t.RunToken,
		Src:       t.Src,
		WorkerID:  workerID,
		Size:      size,
		DstBucket: dstBucket,
		DstKey:    dstKey,
		Reason:    reason,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal completion compensation payload: %w", err)
	}

	resp, err := httpClient.Post(apiBaseURL+"/transfer-tasks/compensations", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("post completion compensation: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("completion compensation returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	drainAndCloseResponseBody(resp)
	return nil
}

func calculatePartSize(size int64) int64 {
	// Max parts: 10000
	partSize := size / 10000
	if partSize < minPartSize {
		partSize = minPartSize
	}
	return partSize
}

func getEnvInt(key string, defaultValue int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultValue
	}
	return n
}

func getEnvBool(key string, defaultValue bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}

	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}
