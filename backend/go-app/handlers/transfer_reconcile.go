package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const (
	transferCompletionReconcileInterval  = 30 * time.Second
	transferCompletionReconcileBatchSize = 100
)

type transferTaskCompensationDetail struct {
	JobID     int64     `json:"job_id"`
	TaskID    int64     `json:"task_id"`
	RunToken  string    `json:"run_token"`
	Src       string    `json:"src"`
	WorkerID  string    `json:"worker_id"`
	Size      int64     `json:"size"`
	DstBucket string    `json:"dst_bucket"`
	DstKey    string    `json:"dst_key"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}

func transferCompletionPendingKey() string {
	return "tx:completion:pending"
}

func transferCompletionCompensationKey(jobID, taskID int64, runToken string) string {
	return fmt.Sprintf("tx:completion:comp:%d:%d:%s", jobID, taskID, runToken)
}

func transferCompletionPendingMember(jobID, taskID int64, runToken string) string {
	return fmt.Sprintf("%d:%d:%s", jobID, taskID, runToken)
}

func parseTransferCompletionPendingMember(member string) (int64, int64, string, error) {
	parts := strings.SplitN(member, ":", 3)
	if len(parts) != 3 || strings.TrimSpace(parts[2]) == "" {
		return 0, 0, "", fmt.Errorf("invalid transfer completion pending member %q", member)
	}
	var jobID, taskID int64
	if _, err := fmt.Sscanf(parts[0], "%d", &jobID); err != nil {
		return 0, 0, "", fmt.Errorf("invalid transfer completion pending member %q: %w", member, err)
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &taskID); err != nil {
		return 0, 0, "", fmt.Errorf("invalid transfer completion pending member %q: %w", member, err)
	}
	if jobID <= 0 || taskID <= 0 {
		return 0, 0, "", fmt.Errorf("invalid transfer completion pending member %q", member)
	}
	return jobID, taskID, parts[2], nil
}

func RecordTransferTaskCompensation(c *gin.Context) {
	type request struct {
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

	var req request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if req.JobID <= 0 || req.TaskID <= 0 || req.RunToken == "" || req.DstBucket == "" || req.DstKey == "" {
		c.JSON(400, gin.H{"error": "job_id, task_id, run_token, dst_bucket and dst_key are required"})
		return
	}

	ctx := context.Background()
	current, _, err := getCurrentTransferTask(ctx, req.JobID, req.TaskID)
	if err == redis.Nil {
		c.JSON(404, gin.H{"error": "transfer task not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": "load transfer task: " + err.Error()})
		return
	}
	if current.RunToken == "" || current.RunToken != req.RunToken {
		c.JSON(409, gin.H{"error": "run token mismatch"})
		return
	}

	detail := transferTaskCompensationDetail{
		JobID:     req.JobID,
		TaskID:    req.TaskID,
		RunToken:  req.RunToken,
		Src:       req.Src,
		WorkerID:  req.WorkerID,
		Size:      req.Size,
		DstBucket: req.DstBucket,
		DstKey:    req.DstKey,
		Reason:    req.Reason,
		CreatedAt: time.Now().UTC(),
	}

	data, err := json.Marshal(detail)
	if err != nil {
		c.JSON(500, gin.H{"error": "marshal compensation detail: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()
	pipe.Set(ctx, transferCompletionCompensationKey(req.JobID, req.TaskID, req.RunToken), data, 0)
	pipe.ZAdd(ctx, transferCompletionPendingKey(), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: transferCompletionPendingMember(req.JobID, req.TaskID, req.RunToken),
	})
	if _, err := pipe.Exec(ctx); err != nil {
		c.JSON(500, gin.H{"error": "store compensation detail: " + err.Error()})
		return
	}

	c.JSON(200, gin.H{"status": "recorded"})
}

func StartTransferCompletionReconciler() {
	go func() {
		ticker := time.NewTicker(transferCompletionReconcileInterval)
		defer ticker.Stop()
		for range ticker.C {
			ctx := context.Background()
			runTransferCompletionReconcileBatch(ctx)
			runTransferClaimedStaleBatch(ctx)
			runTransferStaleRunningBatch(ctx)
		}
	}()
}

func runTransferCompletionReconcileBatch(ctx context.Context) {
	members, err := database.RDB.ZRange(ctx, transferCompletionPendingKey(), 0, transferCompletionReconcileBatchSize-1).Result()
	if err != nil {
		log.Printf("[TransferReconcile] failed to load completion pending members: %v", err)
		return
	}

	for _, member := range members {
		if err := reconcileTransferCompletionPendingMember(ctx, member); err != nil {
			log.Printf("[TransferReconcile] failed to reconcile pending member %s: %v", member, err)
		}
	}
}

func reconcileTransferCompletionPendingMember(ctx context.Context, member string) error {
	jobID, taskID, runToken, err := parseTransferCompletionPendingMember(member)
	if err != nil {
		database.RDB.ZRem(ctx, transferCompletionPendingKey(), member)
		return err
	}

	detailJSON, err := database.RDB.Get(ctx, transferCompletionCompensationKey(jobID, taskID, runToken)).Result()
	if err == redis.Nil {
		database.RDB.ZRem(ctx, transferCompletionPendingKey(), member)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load compensation detail: %w", err)
	}

	var detail transferTaskCompensationDetail
	if err := json.Unmarshal([]byte(detailJSON), &detail); err != nil {
		database.RDB.Del(ctx, transferCompletionCompensationKey(jobID, taskID, runToken))
		database.RDB.ZRem(ctx, transferCompletionPendingKey(), member)
		return fmt.Errorf("decode compensation detail: %w", err)
	}

	taskKey := fmt.Sprintf("tx:task:%d:%d", detail.JobID, detail.TaskID)
	taskJSON, err := database.RDB.Get(ctx, taskKey).Result()
	if err == redis.Nil {
		database.RDB.Del(ctx, transferCompletionCompensationKey(jobID, taskID, runToken))
		database.RDB.ZRem(ctx, transferCompletionPendingKey(), member)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load task state: %w", err)
	}

	var task models.TransferTask
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return fmt.Errorf("decode task state: %w", err)
	}
	if task.RunToken == "" || task.RunToken != runToken {
		cleanupTransferCompletionPending(ctx, member, detail.JobID, detail.TaskID, runToken)
		return nil
	}
	if task.Status == "COMPLETED" {
		cleanupTransferCompletionPending(ctx, member, detail.JobID, detail.TaskID, runToken)
		return nil
	}

	var job models.TransferJob
	if err := database.DB.Preload("Metadata").Where("job_id = ?", detail.JobID).First(&job).Error; err != nil {
		return fmt.Errorf("load transfer job %d: %w", detail.JobID, err)
	}

	exists, err := transferCompletionEvidenceExists(ctx, job, detail)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	if err := applyTransferTaskTerminalUpdate(ctx, task, "COMPLETED", "", detail.Size); err != nil {
		return err
	}

	cleanupTransferCompletionPending(ctx, member, detail.JobID, detail.TaskID, runToken)
	log.Printf("[TransferReconcile] Reconciled completion compensation job=%d task=%d run_token=%s to COMPLETED", detail.JobID, detail.TaskID, runToken)
	return nil
}

func cleanupTransferCompletionPending(ctx context.Context, member string, jobID, taskID int64, runToken string) {
	pipe := database.RDB.Pipeline()
	pipe.Del(ctx, transferCompletionCompensationKey(jobID, taskID, runToken))
	pipe.ZRem(ctx, transferCompletionPendingKey(), member)
	_, _ = pipe.Exec(ctx)
}

func transferCompletionEvidenceExists(ctx context.Context, job models.TransferJob, detail transferTaskCompensationDetail) (bool, error) {
	client, err := createTransferReconcileS3Client(job.Metadata.Endpoint, job.Metadata.AK, job.Metadata.SKEncrypted)
	if err != nil {
		return false, fmt.Errorf("create destination s3 client: %w", err)
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(detail.DstBucket),
		Key:    aws.String(detail.DstKey),
	})
	if err != nil {
		if isTransferReconcileNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("head destination object %s/%s: %w", detail.DstBucket, detail.DstKey, err)
	}

	if detail.Size > 0 && head.ContentLength != nil && *head.ContentLength != detail.Size {
		return false, nil
	}
	return true, nil
}

func createTransferReconcileS3Client(endpoint, ak, skEncrypted string) (*s3.Client, error) {
	sk := strings.TrimPrefix(skEncrypted, "enc_")
	baseEndpoint, err := buildTransferReconcileBaseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("auto"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = false
	}), nil
}

func buildTransferReconcileBaseEndpoint(endpoint string) (string, error) {
	normalized := strings.TrimSpace(endpoint)
	isS3 := strings.HasPrefix(normalized, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(normalized, "s3://")
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

	return fmt.Sprintf("%s://%s", u.Scheme, host), nil
}

func isTransferReconcileNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "no such key") ||
		strings.Contains(msg, "status code: 404") ||
		strings.Contains(msg, "statuscode: 404") ||
		strings.Contains(msg, "statuscode:404")
}

func applyTransferTaskTerminalUpdate(ctx context.Context, task models.TransferTask, terminalStatus, errorMessage string, completionSize int64) error {
	if terminalStatus != "COMPLETED" && terminalStatus != "FAILED" {
		return fmt.Errorf("invalid terminal status %q", terminalStatus)
	}

	size := task.Size
	if size <= 0 {
		size = completionSize
	}
	successBytes := int64(0)
	if task.Status != "COMPLETED" && terminalStatus == "COMPLETED" && size > 0 {
		successBytes = size
	}

	if task.Status != terminalStatus {
		if err := applyTransferTaskDBStatusTransition(task.JobID, task.Status, terminalStatus, successBytes); err != nil {
			return err
		}
	}

	now := time.Now().UTC()
	task.Status = terminalStatus
	task.ErrorMessage = errorMessage
	task.UpdatedAt = now
	if terminalStatus == "COMPLETED" || terminalStatus == "FAILED" {
		task.CompletedAt = now
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal transfer task terminal state: %w", err)
	}

	pipe := database.RDB.Pipeline()
	pipe.Set(ctx, fmt.Sprintf("tx:task:%d:%d", task.JobID, task.ID), data, 0)
	if task.RunToken != "" {
		pipe.ZRem(ctx, transferClaimedRunningKey(), transferClaimedRunningMember(task.JobID, task.ID, task.RunToken))
		pipe.ZRem(ctx, transferRunningLastSeenKey(), transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken))
		pipe.Del(ctx, transferCompletionCompensationKey(task.JobID, task.ID, task.RunToken))
		pipe.ZRem(ctx, transferCompletionPendingKey(), transferCompletionPendingMember(task.JobID, task.ID, task.RunToken))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store transfer task terminal state: %w", err)
	}
	return nil
}

func runTransferClaimedStaleBatch(ctx context.Context) {
	cutoff := float64(time.Now().Add(-TransferClaimedStaleAfter).Unix())
	members, err := database.RDB.ZRangeByScore(ctx, transferClaimedRunningKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", cutoff),
		Count: transferCompletionReconcileBatchSize,
	}).Result()
	if err != nil {
		log.Printf("[TransferReconcile] failed to load claimed stale members: %v", err)
		return
	}

	for _, member := range members {
		if err := reconcileClaimedTransferTask(ctx, member); err != nil {
			log.Printf("[TransferReconcile] failed to reconcile claimed member %s: %v", member, err)
		}
	}
}

func runTransferStaleRunningBatch(ctx context.Context) {
	cutoff := float64(time.Now().Add(-TransferRunningStaleAfter).Unix())
	members, err := database.RDB.ZRangeByScore(ctx, transferRunningLastSeenKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", cutoff),
		Count: transferCompletionReconcileBatchSize,
	}).Result()
	if err != nil {
		log.Printf("[TransferReconcile] failed to load active stale members: %v", err)
		return
	}

	for _, member := range members {
		if err := reconcileStaleRunningTransferTask(ctx, member); err != nil {
			log.Printf("[TransferReconcile] failed to reconcile stale running member %s: %v", member, err)
		}
	}
}

func reconcileClaimedTransferTask(ctx context.Context, member string) error {
	jobID, taskID, runToken, err := parseTransferRuntimeMember(member)
	if err != nil {
		database.RDB.ZRem(ctx, transferClaimedRunningKey(), member)
		return err
	}

	task, _, err := getCurrentTransferTask(ctx, jobID, taskID)
	if err == redis.Nil {
		database.RDB.ZRem(ctx, transferClaimedRunningKey(), member)
		return nil
	}
	if err != nil {
		return err
	}
	if task.RunToken != runToken || task.Status != "RUNNING" {
		database.RDB.ZRem(ctx, transferClaimedRunningKey(), member)
		return nil
	}
	if transferWorkerAlive(ctx, task.WorkerID) {
		return nil
	}

	var job models.TransferJob
	if err := database.DB.Preload("Metadata").Where("job_id = ?", jobID).First(&job).Error; err != nil {
		return err
	}

	detail := transferTaskCompensationDetail{
		JobID:     task.JobID,
		TaskID:    task.ID,
		Size:      task.Size,
		DstBucket: getBucketFromEndpoint(job.Metadata.Endpoint),
		DstKey:    buildTransferDestinationKey(job.DstDir, job.SrcDir, task.Src),
	}
	exists, err := transferCompletionEvidenceExists(ctx, job, detail)
	if err != nil {
		return err
	}
	if exists {
		if err := applyTransferTaskTerminalUpdate(ctx, task, "COMPLETED", "", detail.Size); err != nil {
			return err
		}
		log.Printf("[TransferReconcile] Reconciled claimed RUNNING task job=%d task=%d run_token=%s to COMPLETED", task.JobID, task.ID, task.RunToken)
		return nil
	}

	if err := applyTransferTaskTerminalUpdate(ctx, task, "FAILED", "reconciler marked claimed RUNNING task as FAILED: worker heartbeat expired before activation", 0); err != nil {
		return err
	}
	log.Printf("[TransferReconcile] Reconciled claimed RUNNING task job=%d task=%d run_token=%s to FAILED", task.JobID, task.ID, task.RunToken)
	return nil
}

func reconcileStaleRunningTransferTask(ctx context.Context, member string) error {
	jobID, taskID, runToken, err := parseTransferRuntimeMember(member)
	if err != nil {
		database.RDB.ZRem(ctx, transferRunningLastSeenKey(), member)
		return err
	}

	task, _, err := getCurrentTransferTask(ctx, jobID, taskID)
	if err == redis.Nil {
		database.RDB.ZRem(ctx, transferRunningLastSeenKey(), member)
		return nil
	}
	if err != nil {
		return err
	}
	if task.RunToken != runToken || task.Status != "RUNNING" {
		database.RDB.ZRem(ctx, transferRunningLastSeenKey(), member)
		return nil
	}

	var job models.TransferJob
	if err := database.DB.Preload("Metadata").Where("job_id = ?", jobID).First(&job).Error; err != nil {
		return err
	}

	detail := transferTaskCompensationDetail{
		JobID:     task.JobID,
		TaskID:    task.ID,
		Size:      task.Size,
		DstBucket: getBucketFromEndpoint(job.Metadata.Endpoint),
		DstKey:    buildTransferDestinationKey(job.DstDir, job.SrcDir, task.Src),
	}
	exists, err := transferCompletionEvidenceExists(ctx, job, detail)
	if err != nil {
		return err
	}
	if exists {
		if err := applyTransferTaskTerminalUpdate(ctx, task, "COMPLETED", "", detail.Size); err != nil {
			return err
		}
		log.Printf("[TransferReconcile] Reconciled stale RUNNING task job=%d task=%d run_token=%s to COMPLETED", task.JobID, task.ID, task.RunToken)
		return nil
	}

	if err := applyTransferTaskTerminalUpdate(ctx, task, "FAILED", "reconciler marked stale RUNNING task as FAILED: no completion evidence found", 0); err != nil {
		return err
	}
	log.Printf("[TransferReconcile] Reconciled stale RUNNING task job=%d task=%d run_token=%s to FAILED", task.JobID, task.ID, task.RunToken)
	return nil
}

func buildTransferDestinationKey(dstDir, srcDir, srcKey string) string {
	relKey := buildRelativeKeyForReconcile(srcDir, srcKey)
	dstDir = strings.Trim(strings.TrimSpace(dstDir), "/")
	if dstDir == "" {
		return relKey
	}
	if relKey == "" {
		return dstDir
	}
	return dstDir + "/" + relKey
}

func buildRelativeKeyForReconcile(srcDir, srcKey string) string {
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

func getBucketFromEndpoint(endpoint string) string {
	if strings.HasPrefix(endpoint, "s3://") {
		host := strings.TrimPrefix(endpoint, "s3://")
		parts := strings.Split(host, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}

	return strings.Trim(u.Path, "/")
}
