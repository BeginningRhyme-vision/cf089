package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultTransferAutoRetryFailedCooldownSeconds = 60
	DefaultTransferAutoRetryFailedBatchSize       = 200
	DefaultTransferAutoRetryFailedPollSeconds     = 5
	transferAutoRetryLockTTL                      = 15 * time.Second
)

func transferAutoRetryDueKey() string {
	return "tx:auto:retry:due"
}

func transferAutoRetryLockKey() string {
	return "tx:auto:retry:lock"
}

func transferAutoRetryLockToken() string {
	return fmt.Sprintf("%d:%d", os.Getpid(), time.Now().UTC().UnixNano())
}

func transferAutoRetryMember(jobID, taskID int64) string {
	return fmt.Sprintf("%d:%d", jobID, taskID)
}

func parseTransferAutoRetryMember(member string) (int64, int64, error) {
	var jobID, taskID int64
	if _, err := fmt.Sscanf(member, "%d:%d", &jobID, &taskID); err != nil {
		return 0, 0, fmt.Errorf("parse auto retry member %q: %w", member, err)
	}
	if jobID <= 0 || taskID <= 0 {
		return 0, 0, fmt.Errorf("invalid auto retry member %q", member)
	}
	return jobID, taskID, nil
}

func transferAutoRetryScheduledKey(jobID, taskID int64) string {
	return fmt.Sprintf("tx:auto:retry:scheduled:%d:%d", jobID, taskID)
}

func transferAutoRetryEnabled() bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv("TRANSFER_AUTO_RETRY_FAILED_ENABLED")))
	if raw == "" {
		raw = "false"
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func transferAutoRetryFailedCooldown() time.Duration {
	seconds := getTransferEnvInt("TRANSFER_AUTO_RETRY_FAILED_COOLDOWN_SECONDS", DefaultTransferAutoRetryFailedCooldownSeconds)
	if seconds <= 0 {
		seconds = DefaultTransferAutoRetryFailedCooldownSeconds
	}
	return time.Duration(seconds) * time.Second
}

func transferAutoRetryFailedBatchSize() int64 {
	value := getTransferEnvInt("TRANSFER_AUTO_RETRY_FAILED_BATCH_SIZE", DefaultTransferAutoRetryFailedBatchSize)
	if value <= 0 {
		value = DefaultTransferAutoRetryFailedBatchSize
	}
	return int64(value)
}

func transferAutoRetryPollInterval() time.Duration {
	seconds := getTransferEnvInt("TRANSFER_AUTO_RETRY_FAILED_POLL_SECONDS", DefaultTransferAutoRetryFailedPollSeconds)
	if seconds <= 0 {
		seconds = DefaultTransferAutoRetryFailedPollSeconds
	}
	return time.Duration(seconds) * time.Second
}

func isTransferPermanentFailure(msg string) bool {
	normalized := strings.ToLower(strings.TrimSpace(msg))
	if normalized == "" {
		return false
	}
	return strings.Contains(normalized, "sourcenotfound") ||
		strings.Contains(normalized, "source fetch returned 404") ||
		strings.Contains(normalized, "zerosizedisabled") ||
		strings.Contains(normalized, "zero-byte transfer disabled by config")
}

func setTransferAutoRetryScheduled(pipe redis.Pipeliner, ctx context.Context, jobID, taskID int64, dueAt time.Time) {
	if pipe == nil || jobID <= 0 || taskID <= 0 {
		return
	}
	ttl := transferMultipartCheckpointTTL()
	if ttl < transferAutoRetryFailedCooldown() {
		ttl = transferAutoRetryFailedCooldown()
	}
	pipe.Set(ctx, transferAutoRetryScheduledKey(jobID, taskID), "1", ttl)
	pipe.ZAdd(ctx, transferAutoRetryDueKey(), redis.Z{
		Score:  float64(dueAt.Unix()),
		Member: transferAutoRetryMember(jobID, taskID),
	})
}

func clearTransferAutoRetrySchedule(pipe redis.Pipeliner, ctx context.Context, jobID, taskID int64) {
	if pipe == nil || jobID <= 0 || taskID <= 0 {
		return
	}
	pipe.Del(ctx, transferAutoRetryScheduledKey(jobID, taskID))
	pipe.ZRem(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(jobID, taskID))
}

func isTransferAutoRetryScheduled(ctx context.Context, jobID, taskID int64) (bool, error) {
	count, err := database.RDB.Exists(ctx, transferAutoRetryScheduledKey(jobID, taskID)).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func resolveTransferAutoRetryManualState(ctx context.Context, jobID, taskID int64, pipe redis.Pipeliner) (bool, error) {
	scheduled, err := isTransferAutoRetryScheduled(ctx, jobID, taskID)
	if err != nil {
		return false, err
	}
	if !scheduled {
		return false, nil
	}
	if transferAutoRetryEnabled() {
		return true, nil
	}
	clearTransferAutoRetrySchedule(pipe, ctx, jobID, taskID)
	return false, nil
}

func scheduleTransferAutoRetry(pipe redis.Pipeliner, ctx context.Context, task models.TransferTask, dueAt time.Time) {
	if pipe == nil || task.JobID <= 0 || task.ID <= 0 {
		return
	}
	setTransferAutoRetryScheduled(pipe, ctx, task.JobID, task.ID, dueAt)
}

func shouldRetryTransferTask(task models.TransferTask) bool {
	if task.Status != "FAILED" {
		return false
	}
	return !isTransferPermanentFailure(task.ErrorMessage)
}

func resetTransferTaskForRetry(task *models.TransferTask, now time.Time) {
	if task == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	task.Status = "PENDING"
	task.UpdatedAt = now
	task.ErrorMessage = ""
	task.RunToken = ""
	task.WorkerID = ""
	task.StartedAt = time.Time{}
	task.CompletedAt = time.Time{}
}

func computeTransferRetryRewindOffset(currentOffset, taskID int64, sharded bool) (int64, bool) {
	if sharded {
		target := taskID - 1
		if target < 0 {
			target = 0
		}
		if currentOffset <= target {
			return 0, false
		}
		return target, true
	}
	if currentOffset <= 0 {
		return 0, false
	}
	return 0, true
}

func refreshTransferRetryPoolCandidate(ctx context.Context, job models.TransferJob, task models.TransferTask, pipe redis.Pipeliner, logPrefix string) {
	checkpoint, loadErr := loadTransferMultipartCheckpointRecord(ctx, task.JobID, task.ID)
	if loadErr != nil {
		log.Printf("[%s] failed to load checkpoint for job=%d task=%d while refreshing retry pool candidate: %v", logPrefix, task.JobID, task.ID, loadErr)
		clearTransferResumeCandidate(pipe, ctx, task.JobID, task.ID)
		return
	}
	if transferTaskCanUseCheckpoint(job, task, checkpoint) {
		setTransferResumeCandidate(pipe, ctx, task.JobID, task.ID)
		return
	}
	clearTransferResumeCandidate(pipe, ctx, task.JobID, task.ID)
}

func scheduleTransferAutoRetryAfterFailure(pipe redis.Pipeliner, ctx context.Context, task models.TransferTask) {
	if !transferAutoRetryEnabled() {
		clearTransferAutoRetrySchedule(pipe, ctx, task.JobID, task.ID)
		return
	}
	if !shouldRetryTransferTask(task) {
		clearTransferAutoRetrySchedule(pipe, ctx, task.JobID, task.ID)
		return
	}
	dueAt := time.Now().UTC().Add(transferAutoRetryFailedCooldown())
	scheduleTransferAutoRetry(pipe, ctx, task, dueAt)
}

func StartTransferAutoRetryScheduler() {
	go func() {
		ticker := time.NewTicker(transferAutoRetryPollInterval())
		defer ticker.Stop()

		runTransferAutoRetryBatch()
		for range ticker.C {
			runTransferAutoRetryBatch()
		}
	}()
}

func acquireTransferAutoRetryLock(ctx context.Context) (string, bool, error) {
	token := transferAutoRetryLockToken()
	locked, err := database.RDB.SetNX(ctx, transferAutoRetryLockKey(), token, transferAutoRetryLockTTL).Result()
	if err != nil {
		return "", false, err
	}
	if !locked {
		return "", false, nil
	}
	return token, true, nil
}

func releaseTransferAutoRetryLock(ctx context.Context, token string) error {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`
	return database.RDB.Eval(ctx, script, []string{transferAutoRetryLockKey()}, token).Err()
}

func runTransferAutoRetryBatch() {
	if !transferAutoRetryEnabled() {
		return
	}

	ctx := context.Background()
	lockToken, locked, err := acquireTransferAutoRetryLock(ctx)
	if err != nil {
		log.Printf("[TransferAutoRetry] failed to acquire scheduler lock: %v", err)
		return
	}
	if !locked {
		return
	}
	defer func() {
		if err := releaseTransferAutoRetryLock(ctx, lockToken); err != nil {
			log.Printf("[TransferAutoRetry] failed to release scheduler lock: %v", err)
		}
	}()

	members, err := database.RDB.ZRangeByScore(ctx, transferAutoRetryDueKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", time.Now().Unix()),
		Count: transferAutoRetryFailedBatchSize(),
	}).Result()
	if err != nil {
		log.Printf("[TransferAutoRetry] failed to load due members: %v", err)
		return
	}

	for _, member := range members {
		if err := processTransferAutoRetryMember(ctx, member); err != nil {
			log.Printf("[TransferAutoRetry] failed to process member %s: %v", member, err)
		}
	}
}

func transferAutoRetryMemberReady(ctx context.Context, jobID, taskID int64) (bool, error) {
	scheduled, err := isTransferAutoRetryScheduled(ctx, jobID, taskID)
	if err != nil {
		return false, err
	}
	if !scheduled {
		if err := database.RDB.ZRem(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(jobID, taskID)).Err(); err != nil {
			return false, err
		}
		return false, nil
	}

	score, err := database.RDB.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(jobID, taskID)).Result()
	if err == redis.Nil {
		if err := database.RDB.Del(ctx, transferAutoRetryScheduledKey(jobID, taskID)).Err(); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return score <= float64(time.Now().UTC().Unix()), nil
}

func processTransferAutoRetryMember(ctx context.Context, member string) error {
	jobID, taskID, err := parseTransferAutoRetryMember(member)
	if err != nil {
		pipe := database.RDB.Pipeline()
		pipe.ZRem(ctx, transferAutoRetryDueKey(), member)
		_, _ = pipe.Exec(ctx)
		return nil
	}

	current, taskKey, err := getCurrentTransferTask(ctx, jobID, taskID)
	if err == redis.Nil {
		pipe := database.RDB.Pipeline()
		clearTransferAutoRetrySchedule(pipe, ctx, jobID, taskID)
		_, _ = pipe.Exec(ctx)
		return nil
	}
	if err != nil {
		return err
	}

	if current.Status != "FAILED" {
		pipe := database.RDB.Pipeline()
		clearTransferAutoRetrySchedule(pipe, ctx, current.JobID, current.ID)
		_, _ = pipe.Exec(ctx)
		return nil
	}
	if !shouldRetryTransferTask(current) {
		pipe := database.RDB.Pipeline()
		clearTransferAutoRetrySchedule(pipe, ctx, current.JobID, current.ID)
		_, _ = pipe.Exec(ctx)
		return nil
	}
	ready, err := transferAutoRetryMemberReady(ctx, current.JobID, current.ID)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	var job models.TransferJob
	if err := database.DB.Preload("Metadata").Where("job_id = ?", current.JobID).First(&job).Error; err != nil {
		return err
	}

	return retrySingleTransferTask(ctx, taskKey, job, current, "auto")
}

func retrySingleTransferTask(ctx context.Context, taskKey string, job models.TransferJob, task models.TransferTask, reason string) error {
	now := time.Now().UTC()
	updated := task
	resetTransferTaskForRetry(&updated, now)

	data, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	pipe := database.RDB.Pipeline()
	pipe.Set(ctx, taskKey, data, 0)
	refreshTransferRetryPoolCandidate(ctx, job, updated, pipe, "TransferAutoRetry")
	clearTransferAutoRetrySchedule(pipe, ctx, updated.JobID, updated.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	query := `
		UPDATE transfer_jobs
		SET
			failed_count = GREATEST(0, failed_count - 1),
			pending_count = pending_count + 1,
			status = CASE
				WHEN status = 'COMPLETED' OR status = 'FAILED' THEN 'PENDING'
				ELSE status
			END,
			updated_at = NOW()
		WHERE job_id = ?
	`
	if err := database.DB.Exec(query, updated.JobID).Error; err != nil {
		if rollbackErr := rollbackAutoRetriedTransferTask(ctx, taskKey, task); rollbackErr != nil {
			log.Printf("[TransferAutoRetry] failed to rollback job=%d task=%d after db error: %v", task.JobID, task.ID, rollbackErr)
		}
		return err
	}

	offsetKey := fmt.Sprintf("tx:job:%d:offset", updated.JobID)
	offsetBefore, offsetBeforeErr := database.RDB.Get(ctx, offsetKey).Int64()
	if offsetBeforeErr == redis.Nil {
		offsetBefore = 0
		offsetBeforeErr = nil
	}
	if offsetBeforeErr == nil {
		if rewindTo, shouldRewind := computeTransferRetryRewindOffset(offsetBefore, updated.ID, isJobSharded(ctx, updated.JobID)); shouldRewind {
			if err := database.RDB.Set(ctx, offsetKey, rewindTo, 0).Err(); err != nil {
				log.Printf("[TransferAutoRetry] failed to rewind offset for job=%d task=%d after %s retry reset: %v", updated.JobID, updated.ID, reason, err)
			}
		}
	} else {
		log.Printf("[TransferAutoRetry] failed to read offset before retry rewind for job=%d task=%d after %s retry reset: %v", updated.JobID, updated.ID, reason, offsetBeforeErr)
	}
	triggerTxRefill(updated.JobID)
	log.Printf("[TransferAutoRetry] reset task job=%d task=%d to pending via %s retry", updated.JobID, updated.ID, reason)
	return nil
}

func rollbackAutoRetriedTransferTask(ctx context.Context, taskKey string, original models.TransferTask) error {
	data, err := json.Marshal(original)
	if err != nil {
		return err
	}

	pipe := database.RDB.Pipeline()
	pipe.Set(ctx, taskKey, data, 0)
	clearTransferResumeCandidate(pipe, ctx, original.JobID, original.ID)
	scheduleTransferAutoRetryAfterFailure(pipe, ctx, original)
	_, err = pipe.Exec(ctx)
	return err
}
