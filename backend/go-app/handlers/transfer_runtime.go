package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	TransferWorkerHeartbeatTTL  = 2 * time.Minute
	TransferClaimedStaleAfter   = 20 * time.Minute
	TransferRunningStaleAfter   = 15 * time.Minute
	TransferHeartbeatInterval   = 30 * time.Second
	TransferActiveTouchInterval = 5 * time.Second
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
