package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	TransferWorkerHeartbeatTTL            = 2 * time.Minute
	TransferClaimedStaleAfter             = 20 * time.Minute
	TransferRunningStaleAfter             = 15 * time.Minute
	TransferHeartbeatInterval             = 30 * time.Second
	TransferActiveTouchInterval           = 5 * time.Second
	DefaultTransferMultipartCheckpointTTL = 72 * time.Hour
)

func transferClaimedRunningKey() string {
	return "tx:running:claimed"
}

func transferRunningLastSeenKey() string {
	return "tx:running:last_seen"
}

func transferWorkerHeartbeatKey(workerID string) string {
	return fmt.Sprintf("tx:worker:heartbeat:%s", workerID)
}

func transferMultipartCheckpointKey(jobID, taskID int64) string {
	return fmt.Sprintf("tx:mpu:ckpt:%d:%d", jobID, taskID)
}

func transferClaimedRunningMember(jobID, taskID int64, runToken string) string {
	return fmt.Sprintf("%d:%d:%s", jobID, taskID, runToken)
}

func transferRunningLastSeenMember(jobID, taskID int64, runToken string) string {
	return fmt.Sprintf("%d:%d:%s", jobID, taskID, runToken)
}

func parseTransferRuntimeMember(member string) (int64, int64, string, error) {
	parts := strings.SplitN(member, ":", 3)
	if len(parts) != 3 {
		return 0, 0, "", fmt.Errorf("invalid transfer runtime member %q", member)
	}

	var jobID, taskID int64
	if _, err := fmt.Sscanf(parts[0], "%d", &jobID); err != nil {
		return 0, 0, "", fmt.Errorf("parse job id from %q: %w", member, err)
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &taskID); err != nil {
		return 0, 0, "", fmt.Errorf("parse task id from %q: %w", member, err)
	}
	return jobID, taskID, parts[2], nil
}

func newTransferRunToken(workerID string, now time.Time) string {
	return fmt.Sprintf("%d:%s", now.UTC().UnixNano(), workerID)
}

func getCurrentTransferTask(ctx context.Context, jobID, taskID int64) (models.TransferTask, string, error) {
	taskKey := fmt.Sprintf("tx:task:%d:%d", jobID, taskID)
	raw, err := database.RDB.Get(ctx, taskKey).Result()
	if err != nil {
		return models.TransferTask{}, taskKey, err
	}

	var task models.TransferTask
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return models.TransferTask{}, taskKey, err
	}
	return task, taskKey, nil
}

func claimTransferTask(ctx context.Context, task models.TransferTask, workerID string) (models.TransferTask, bool, error) {
	current, taskKey, err := getCurrentTransferTask(ctx, task.JobID, task.ID)
	if err != nil {
		if err == redis.Nil {
			return models.TransferTask{}, false, nil
		}
		return models.TransferTask{}, false, err
	}
	if current.Status != "PENDING" {
		return models.TransferTask{}, false, nil
	}

	now := time.Now().UTC()
	current.Status = "RUNNING"
	current.WorkerID = workerID
	current.RunToken = newTransferRunToken(workerID, now)
	if current.StartedAt.IsZero() {
		current.StartedAt = now
	}
	current.UpdatedAt = now

	data, err := json.Marshal(current)
	if err != nil {
		return models.TransferTask{}, false, err
	}

	pipe := database.RDB.Pipeline()
	pipe.Set(ctx, taskKey, data, 0)
	pipe.ZAdd(ctx, transferClaimedRunningKey(), redis.Z{
		Score:  float64(now.Unix()),
		Member: transferClaimedRunningMember(current.JobID, current.ID, current.RunToken),
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return models.TransferTask{}, false, err
	}

	query := `
		UPDATE transfer_jobs
		SET
			pending_count = GREATEST(0, pending_count - 1),
			running_count = running_count + 1,
			status = CASE
				WHEN status = 'PENDING' THEN 'RUNNING'
				ELSE status
			END,
			updated_at = NOW()
		WHERE job_id = ?
	`
	if err := database.DB.Exec(query, current.JobID).Error; err != nil {
		return models.TransferTask{}, false, err
	}

	return current, true, nil
}

func touchTransferRunningLastSeen(pipe redis.Pipeliner, ctx context.Context, task models.TransferTask) {
	if pipe == nil || task.JobID <= 0 || task.ID <= 0 || task.RunToken == "" || task.Status != "RUNNING" {
		return
	}

	pipe.ZAddArgs(ctx, transferRunningLastSeenKey(), redis.ZAddArgs{
		XX: true,
		Members: []redis.Z{
			{
				Score:  float64(time.Now().Unix()),
				Member: transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken),
			},
		},
	})
}

func MarkTransferTaskActive(c *gin.Context) {
	type activateRequest struct {
		JobID    int64  `json:"job_id"`
		TaskID   int64  `json:"task_id"`
		RunToken string `json:"run_token"`
		WorkerID string `json:"worker_id"`
	}

	var req activateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := context.Background()
	current, _, err := getCurrentTransferTask(ctx, req.JobID, req.TaskID)
	if err == redis.Nil {
		log.Printf("[TransferTaskActivate] job=%d task=%d worker=%s run_token=%s result=not_found", req.JobID, req.TaskID, req.WorkerID, req.RunToken)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "not_found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if current.Status != "RUNNING" {
		log.Printf("[TransferTaskActivate] job=%d task=%d worker=%s run_token=%s result=ignored reason=task_not_running current_status=%s", req.JobID, req.TaskID, req.WorkerID, req.RunToken, current.Status)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "task_not_running"})
		return
	}
	if current.RunToken != req.RunToken {
		log.Printf("[TransferTaskActivate] job=%d task=%d worker=%s run_token=%s result=ignored reason=run_token_mismatch current_run_token=%s", req.JobID, req.TaskID, req.WorkerID, req.RunToken, current.RunToken)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "run_token_mismatch"})
		return
	}
	if current.WorkerID != "" && current.WorkerID != req.WorkerID {
		log.Printf("[TransferTaskActivate] job=%d task=%d worker=%s run_token=%s result=ignored reason=worker_mismatch current_worker=%s", req.JobID, req.TaskID, req.WorkerID, req.RunToken, current.WorkerID)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "worker_mismatch"})
		return
	}

	pipe := database.RDB.Pipeline()
	pipe.ZRem(ctx, transferClaimedRunningKey(), transferClaimedRunningMember(current.JobID, current.ID, current.RunToken))
	pipe.ZAdd(ctx, transferRunningLastSeenKey(), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: transferRunningLastSeenMember(current.JobID, current.ID, current.RunToken),
	})
	if _, err := pipe.Exec(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	log.Printf("[TransferTaskActivate] job=%d task=%d worker=%s run_token=%s result=activated", req.JobID, req.TaskID, req.WorkerID, req.RunToken)
	c.JSON(http.StatusOK, gin.H{"status": "activated"})
}

func UpdateTransferTaskProgress(c *gin.Context) {
	type progressRequest struct {
		JobID    int64  `json:"job_id"`
		TaskID   int64  `json:"task_id"`
		RunToken string `json:"run_token"`
	}

	var req progressRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := context.Background()
	task, _, err := getCurrentTransferTask(ctx, req.JobID, req.TaskID)
	if err == redis.Nil {
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "not_found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if task.RunToken != req.RunToken || task.Status != "RUNNING" {
		c.JSON(http.StatusOK, gin.H{"status": "ignored"})
		return
	}

	pipe := database.RDB.Pipeline()
	touchTransferRunningLastSeen(pipe, ctx, task)
	if _, err := pipe.Exec(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func RecordTransferWorkerHeartbeat(c *gin.Context) {
	type heartbeatRequest struct {
		WorkerID string `json:"worker_id"`
	}

	var req heartbeatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
		return
	}

	if err := database.RDB.Set(context.Background(), transferWorkerHeartbeatKey(req.WorkerID), time.Now().UTC().Format(time.RFC3339Nano), TransferWorkerHeartbeatTTL).Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type transferMultipartCheckpoint struct {
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

func transferMultipartCheckpointTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("TRANSFER_MULTIPART_CHECKPOINT_TTL_HOURS"))
	if raw == "" {
		return DefaultTransferMultipartCheckpointTTL
	}
	hours, err := strconv.Atoi(raw)
	if err != nil || hours <= 0 {
		return DefaultTransferMultipartCheckpointTTL
	}
	return time.Duration(hours) * time.Hour
}

func loadTransferMultipartCheckpointRecord(ctx context.Context, jobID, taskID int64) (*transferMultipartCheckpoint, error) {
	raw, err := database.RDB.Get(ctx, transferMultipartCheckpointKey(jobID, taskID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var checkpoint transferMultipartCheckpoint
	if err := json.Unmarshal([]byte(raw), &checkpoint); err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	return &checkpoint, nil
}

func clearTransferMultipartCheckpointRecord(ctx context.Context, jobID, taskID int64) error {
	return database.RDB.Del(ctx, transferMultipartCheckpointKey(jobID, taskID)).Err()
}

func LoadTransferMultipartCheckpoint(c *gin.Context) {
	type loadRequest struct {
		JobID  int64 `json:"job_id"`
		TaskID int64 `json:"task_id"`
	}

	var req loadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.JobID <= 0 || req.TaskID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and task_id are required"})
		return
	}

	ctx := context.Background()
	checkpoint, err := loadTransferMultipartCheckpointRecord(ctx, req.JobID, req.TaskID)
	if err == redis.Nil || checkpoint == nil {
		c.JSON(http.StatusOK, gin.H{"found": false})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"found":      true,
		"checkpoint": checkpoint,
	})
}

func SaveTransferMultipartCheckpoint(c *gin.Context) {
	var checkpoint transferMultipartCheckpoint
	if err := c.ShouldBindJSON(&checkpoint); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if checkpoint.JobID <= 0 || checkpoint.TaskID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and task_id are required"})
		return
	}
	if strings.TrimSpace(checkpoint.UploadID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "upload_id is required"})
		return
	}

	now := time.Now().UTC()
	if checkpoint.CreatedAt.IsZero() {
		checkpoint.CreatedAt = now
	}
	checkpoint.UpdatedAt = now

	data, err := json.Marshal(checkpoint)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("marshal checkpoint: %v", err)})
		return
	}

	ctx := context.Background()
	if err := database.RDB.Set(ctx, transferMultipartCheckpointKey(checkpoint.JobID, checkpoint.TaskID), data, transferMultipartCheckpointTTL()).Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "saved"})
}

func ClearTransferMultipartCheckpoint(c *gin.Context) {
	type clearRequest struct {
		JobID  int64 `json:"job_id"`
		TaskID int64 `json:"task_id"`
	}

	var req clearRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.JobID <= 0 || req.TaskID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and task_id are required"})
		return
	}

	ctx := context.Background()
	if err := clearTransferMultipartCheckpointRecord(ctx, req.JobID, req.TaskID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "cleared"})
}

func applyTransferTaskDBStatusTransition(jobID int64, oldStatus, newStatus string, successBytes int64) error {
	var pendingDelta, runningDelta, successDelta, failedDelta int

	switch oldStatus {
	case "PENDING":
		pendingDelta--
	case "RUNNING":
		runningDelta--
	case "COMPLETED":
		successDelta--
	case "FAILED":
		failedDelta--
	}

	switch newStatus {
	case "PENDING":
		pendingDelta++
	case "RUNNING":
		runningDelta++
	case "COMPLETED":
		successDelta++
	case "FAILED":
		failedDelta++
	}

	query := `
		UPDATE transfer_jobs SET
			pending_count = GREATEST(0, pending_count + ?),
			running_count = GREATEST(0, running_count + ?),
			success_count = GREATEST(0, success_count + ?),
			failed_count = GREATEST(0, failed_count + ?),
			success_size_bytes = success_size_bytes + ?,
			status = CASE
				WHEN status = 'PENDING' AND (GREATEST(0, running_count + ?)) > 0 THEN 'RUNNING'
				WHEN status = 'RUNNING' AND (GREATEST(0, pending_count + ?)) <= 0 AND (GREATEST(0, running_count + ?)) <= 0 THEN 'COMPLETED'
				ELSE status
			END,
			updated_at = NOW()
		WHERE job_id = ?
	`

	return database.DB.Exec(
		query,
		pendingDelta,
		runningDelta,
		successDelta,
		failedDelta,
		successBytes,
		runningDelta,
		pendingDelta,
		runningDelta,
		jobID,
	).Error
}

func transferWorkerAlive(ctx context.Context, workerID string) bool {
	if strings.TrimSpace(workerID) == "" {
		return false
	}
	exists, err := database.RDB.Exists(ctx, transferWorkerHeartbeatKey(workerID)).Result()
	return err == nil && exists > 0
}

func transferTaskExists(jobID int64) bool {
	return jobID > 0
}

var _ = gorm.ErrRecordNotFound
