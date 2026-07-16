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
	jobCache           sync.Map // JobID -> cachedJob
	httpClient         *http.Client
	transferClient     *http.Client
	transferProtoLog   sync.Once
	s3ProtoLog         sync.Once
	workerID           string
	workerCount        int
	taskBufferSize     int
	partConcurrency    int
	multipartThreshold int64
	minPartSize        int64

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
	WorkerID                    = "go-transfer-1"
	DefaultConcurrentWorkers    = 64
	DefaultTaskBufferSize       = 128
	DefaultPartConcurrency      = 16
	DefaultMultipartThresholdMB = 8
	DefaultMinPartSizeMB        = 5
	DefaultTransferMaxConns     = 384
	DefaultTransferTimeoutSec   = 120
	DefaultTransferTLSHandshake = 10
	WorkerHeartbeatInterval     = 30 * time.Second
	ActiveTouchInterval         = 5 * time.Second
	TransferAttemptLimit        = 2
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
	}
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
	size := t.Size

	if size == 0 {
		head, err := srcClient.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(srcKey),
		})
		if err != nil {
			log.Printf("HeadObject failed for %s/%s: %v", srcBucket, srcKey, err)
			updateTaskStatus(t, "FAILED", "HeadObject failed: "+err.Error())
			updateJobStats(t.JobID, 0, 1)
			TasksTransferred.WithLabelValues("failed").Inc()
			return
		}
		size = *head.ContentLength
	}

	// 4. Construct Public/Virtual-Hosted URLs for Transfer Service (Matches r2s3.go logic)
	srcUrl, err := constructVirtualHostURL(cfg.Storage.Src.Endpoint, srcBucket, srcKey)
	if err != nil {
		log.Printf("Failed to construct Src URL for task %d: %v", t.ID, err)
		updateTaskStatus(t, "FAILED", "Construct Src URL failed")
		updateJobStats(t.JobID, 0, 1)
		TasksTransferred.WithLabelValues("failed").Inc()
		return
	}

	dstBucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	log.Printf("Task %d: Transferring %d bytes to bucket '%s' key '%s'", t.ID, size, dstBucket, dstKey)

	// 5. Transfer Loop
	err = transferFile(srcUrl, dstClient, dstBucket, dstKey, size, job.Metadata.Endpoint)
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

func transferFile(srcURL string, dstClient *s3.Client, dstBucket, dstKey string, size int64, dstEndpoint string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstUrl, err := constructVirtualHostURL(dstEndpoint, dstBucket, dstKey)
	if err != nil {
		return err
	}

	if size < multipartThreshold {
		_, err = uploadTransferPartWithRetry(ctx, srcURL, dstUrl, size, 0, "", -1)
		return err
	}

	// Multipart
	createOut, err := dstClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(dstBucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		return err
	}
	uploadID := *createOut.UploadId

	partSize := calculatePartSize(size)
	numParts := int((size-1)/partSize) + 1

	completedParts := make([]types.CompletedPart, numParts)
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
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer abortCancel()
		_, _ = dstClient.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		})
		return firstErr
	}

	_, err = dstClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
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
