package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"unbound-future-backend/database"
	"unbound-future-backend/metrics"
	"unbound-future-backend/models"
)

// Global Buffer Manager
var (
	jobBuffers       = make(map[int64]chan models.YoutubeTask)
	txJobBuffers     = make(map[int64]chan models.TransferTask)
	ffmpegJobBuffers = make(map[int64]chan models.FfmpegTask)
	bufferMutex      sync.RWMutex
	fillingMap       sync.Map // prevent concurrent fills for same job
)

const (
	BufferSize            = 1000
	FetchBatchSize        = 100
	BufferLowWater        = 500 // Refill when below this
	LockExpiration        = 30 * time.Second
	StatsDirtySetYoutube  = "stats:dirty_jobs:youtube"
	StatsDirtySetTransfer = "stats:dirty_jobs:transfer"
	StatsDirtySetFfmpeg   = "stats:dirty_jobs:ffmpeg"
	StuckScanLimitPerRun  = 10000 // Max items to scan per job per cycle
)

// StartBufferService initializes the background pre-fetching service
func StartBufferService() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		monitorTicker := time.NewTicker(10 * time.Minute) // Check for stuck tasks every 10m
		for {
			select {
			case <-ticker.C:
				checkAndRefillBuffers()
				checkAndRefillTxBuffers()
				checkAndRefillFfmpegBuffers()
				flushStats()
			case <-monitorTicker.C:
				scanStuckYoutubeTasks()
				scanStuckTransferTasks()
				scanStuckFfmpegTasks()
				scanDroppedDownloadTasks()
				scanDroppedPendingTasks()
			}
		}
	}()
}

func scanStuckYoutubeTasks() {
	ctx := context.Background()
	var jobs []models.YoutubeJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		scanGenericStuckJob(ctx, int64(job.ID), "youtube", "job:%d:tasks", "task:%s", "queue:youtube:metadata_retry", StatsDirtySetYoutube,
			func(str string) (bool, []byte, string, int64) {
				var t models.YoutubeTask
				if err := json.Unmarshal([]byte(str), &t); err == nil {
					refTime := t.UpdatedAt
					if refTime.IsZero() {
						refTime = t.CreatedAt
					}
					if t.Status == "RUNNING" && time.Since(refTime) > 3*time.Hour {
						t.Status = "PENDING"
						t.WorkerID = ""
						t.ErrorMessage = "Reset by stuck monitor"
						t.UpdatedAt = time.Now()
						t.IsDownloadFail = false
						data, _ := json.Marshal(t)
						return true, data, t.Status, t.ID
					}
				}
				return false, nil, "", 0
			})
	}
}

func scanStuckTransferTasks() {
	ctx := context.Background()
	var jobs []models.TransferJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		scanGenericStuckJob(ctx, int64(job.JobID), "transfer", "tx:job:%d:tasks", "tx:task:%s", "queue:transfer:retry", StatsDirtySetTransfer,
			func(str string) (bool, []byte, string, int64) {
				var t models.TransferTask
				if err := json.Unmarshal([]byte(str), &t); err == nil {
					refTime := t.UpdatedAt
					if refTime.IsZero() {
						refTime = t.CreatedAt
					}
					if t.Status == "RUNNING" && time.Since(refTime) > 3*time.Hour {
						t.Status = "PENDING"
						t.WorkerID = ""
						t.ErrorMessage = "Reset by stuck monitor"
						t.UpdatedAt = time.Now()
						data, _ := json.Marshal(t)
						return true, data, t.Status, t.ID
					}
				}
				return false, nil, "", 0
			})
	}
}

func scanStuckFfmpegTasks() {
	ctx := context.Background()
	var jobs []models.FfmpegJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		scanGenericStuckJob(ctx, int64(job.ID), "ffmpeg", "ff:job:%d:tasks", "ff:task:%s", "queue:ffmpeg:retry", StatsDirtySetFfmpeg,
			func(str string) (bool, []byte, string, int64) {
				var t models.FfmpegTask
				if err := json.Unmarshal([]byte(str), &t); err == nil {
					refTime := t.UpdatedAt
					if refTime.IsZero() {
						refTime = t.CreatedAt
					}
					if t.Status == "RUNNING" && time.Since(refTime) > 3*time.Hour {
						t.Status = "PENDING"
						t.WorkerID = ""
						t.ErrorMessage = "Reset by stuck monitor"
						t.UpdatedAt = time.Now()
						data, _ := json.Marshal(t)
						return true, data, t.Status, t.ID
					}
				}
				return false, nil, "", 0
			})
	}
}

func scanGenericStuckJob(ctx context.Context, jobID int64, jobType, zsetKeyTpl, taskKeyTpl, retryQueue, dirtySet string,
	checkFunc func(string) (bool, []byte, string, int64)) {

	jobKey := fmt.Sprintf(zsetKeyTpl, jobID)
	cursorKey := fmt.Sprintf("scan:stuck:%s:job:%d:cursor", jobType, jobID)

	// Get total count
	total, err := database.RDB.ZCard(ctx, jobKey).Result()
	if err != nil || total == 0 {
		return
	}

	// Get current cursor
	var offset int64
	cursorStr, _ := database.RDB.Get(ctx, cursorKey).Result()
	if cursorStr != "" {
		fmt.Sscanf(cursorStr, "%d", &offset)
	}

	// Reset cursor if out of bounds
	if offset >= total {
		offset = 0
	}

	processedCount := 0
	batchSize := 1000

	for processedCount < StuckScanLimitPerRun {
		stop := offset + int64(batchSize) - 1
		ids, err := database.RDB.ZRange(ctx, jobKey, offset, stop).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, id := range ids {
			keys = append(keys, fmt.Sprintf(taskKeyTpl, id))
		}

		tasksJSON, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			offset += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		type updateOp struct {
			cmd       *redis.Cmd
			taskID    int64
			jobID     int64
			newStatus string
		}
		var ops []updateOp
		hasUpdates := false

		for _, val := range tasksJSON {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			isStuck, newData, newStatus, taskID := checkFunc(str)
			if isStuck {
				taskKey := fmt.Sprintf(taskKeyTpl, strconv.FormatInt(taskID, 10))
				cmd := pipe.Eval(ctx, updateTaskScript, []string{taskKey}, newData, newStatus, "0")
				ops = append(ops, updateOp{cmd: cmd, taskID: taskID, jobID: jobID, newStatus: newStatus})
				pipe.RPush(ctx, retryQueue, taskID)
				hasUpdates = true
			}
		}

		if hasUpdates {
			_, err := pipe.Exec(ctx)
			if err == nil {
				statsPipe := database.RDB.Pipeline()
				hasStatsUpdates := false
				for _, op := range ops {
					res, err := op.cmd.Result()
					if err == nil {
						oldStatus, ok := res.(string)
						if ok && oldStatus != "MISSING" && oldStatus != op.newStatus {
							oldB := getStatsBucket(oldStatus)
							newB := getStatsBucket(op.newStatus)
							if oldB != newB {
								// Fix: zsetKeyTpl is like job:%d:tasks, we need job:%d:stats
								// Hacky replacement or just use passed stats key?
								// We didn't pass stats key template. Let's reconstruct or pass it.
								// Actually simpler: we passed dirtySet, we can infer stats key or just require it.
								// Let's assume standard naming based on jobType or derive from logic.
								// For now, let's reconstruct since patterns are consistent.
								statsKey := ""
								if jobType == "youtube" {
									statsKey = fmt.Sprintf("job:%d:stats", op.jobID)
								} else if jobType == "transfer" {
									statsKey = fmt.Sprintf("tx:job:%d:stats", op.jobID)
								} else if jobType == "ffmpeg" {
									statsKey = fmt.Sprintf("ff:job:%d:stats", op.jobID)
								}

								statsPipe.HIncrBy(ctx, statsKey, oldB, -1)
								statsPipe.HIncrBy(ctx, statsKey, newB, 1)
								statsPipe.SAdd(ctx, dirtySet, op.jobID)
								hasStatsUpdates = true
								metrics.TaskStatusChangeTotal.WithLabelValues(jobType, newB).Inc()
							}
							// ZSet update is implicit as we don't remove from ZSet here
						}
					}
				}
				if hasStatsUpdates {
					statsPipe.Exec(ctx)
				}
			}
		}

		count := len(ids)
		offset += int64(count)
		processedCount += count

		if offset >= total {
			offset = 0 // Wrap around
			break      // Finish this run, start from 0 next time
		}
	}

	// Save new cursor
	database.RDB.Set(ctx, cursorKey, offset, 0)
}

func scanDroppedDownloadTasks() {
	ctx := context.Background()

	// 1. Check if queue is effectively empty
	n, err := database.RDB.LLen(ctx, "queue:youtube:download_ready").Result()
	if err != nil || n > 10 { // Only scan if queue is very low
		return
	}

	var jobs []models.YoutubeJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	// Limit scan to avoid load spikes
	// We can randomize job order or just iterate.
	for _, job := range jobs {
		jobKey := fmt.Sprintf("job:%d:tasks", job.ID)
		batchSize := 1000
		var offset int64 = 0

		// Safety breaker to avoid full scan of huge jobs every time
		loops := 0
		// Check first 5000 tasks? Or maybe last 5000?
		// Actually, METADATA_FETCHED tasks are likely "in progress" or "recent".
		// But if they are old dropped ones, they could be anywhere.
		// Let's scan all for now, assuming not too many active jobs have huge lists.

		for {
			loops++
			if loops > 100 { // Limit to 100k tasks per job per scan to be safe
				break
			}

			ids, err := database.RDB.ZRange(ctx, jobKey, offset, offset+int64(batchSize)-1).Result()
			if err != nil || len(ids) == 0 {
				break
			}

			var keys []string
			for _, id := range ids {
				keys = append(keys, fmt.Sprintf("task:%s", id))
			}

			tasksJSON, err := database.RDB.MGet(ctx, keys...).Result()
			if err != nil {
				offset += int64(batchSize)
				continue
			}

			foundCount := 0
			for _, val := range tasksJSON {
				if val == nil {
					continue
				}
				str, ok := val.(string)
				if !ok {
					continue
				}

				var t models.YoutubeTask
				if err := json.Unmarshal([]byte(str), &t); err == nil {
					if t.Status == "METADATA_FETCHED" {
						// Found a dropped task!
						database.RDB.RPush(ctx, "queue:youtube:download_ready", t.ID)
						foundCount++
					}
				}
			}

			if foundCount > 0 {
				fmt.Printf("Recovered %d dropped download tasks for Job %d\n", foundCount, job.ID)
			}

			if len(ids) < batchSize {
				break
			}
			offset += int64(batchSize)
		}
	}
}

func scanDroppedPendingTasks() {
	scanDroppedYoutubePending()
	scanDroppedTransferPending()
	scanDroppedFfmpegPending()
}

func scanDroppedYoutubePending() {
	ctx := context.Background()
	// Only scan if retry queue is empty to avoid congestion
	if n, err := database.RDB.LLen(ctx, "queue:youtube:metadata_retry").Result(); err != nil || n > 100 {
		return
	}

	var jobs []models.YoutubeJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		scanGenericDroppedPendingTask(ctx, int64(job.ID), "job:%d:tasks", "task:%s", "queue:youtube:metadata_retry", func(str string) (int64, string, time.Time, bool) {
			var t models.YoutubeTask
			if err := json.Unmarshal([]byte(str), &t); err == nil {
				refTime := t.UpdatedAt
				if refTime.IsZero() {
					refTime = t.CreatedAt
				}
				return t.ID, t.Status, refTime, true
			}
			return 0, "", time.Time{}, false
		})
	}
}

func scanDroppedTransferPending() {
	ctx := context.Background()
	if n, err := database.RDB.LLen(ctx, "queue:transfer:retry").Result(); err != nil || n > 100 {
		return
	}

	var jobs []models.TransferJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		scanGenericDroppedPendingTask(ctx, int64(job.JobID), "tx:job:%d:tasks", "tx:task:%s", "queue:transfer:retry", func(str string) (int64, string, time.Time, bool) {
			var t models.TransferTask
			if err := json.Unmarshal([]byte(str), &t); err == nil {
				refTime := t.UpdatedAt
				if refTime.IsZero() {
					refTime = t.CreatedAt
				}
				return t.ID, t.Status, refTime, true
			}
			return 0, "", time.Time{}, false
		})
	}
}

func scanDroppedFfmpegPending() {
	ctx := context.Background()
	if n, err := database.RDB.LLen(ctx, "queue:ffmpeg:retry").Result(); err != nil || n > 100 {
		return
	}

	var jobs []models.FfmpegJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		scanGenericDroppedPendingTask(ctx, int64(job.ID), "ff:job:%d:tasks", "ff:task:%s", "queue:ffmpeg:retry", func(str string) (int64, string, time.Time, bool) {
			var t models.FfmpegTask
			if err := json.Unmarshal([]byte(str), &t); err == nil {
				refTime := t.UpdatedAt
				if refTime.IsZero() {
					refTime = t.CreatedAt
				}
				return t.ID, t.Status, refTime, true
			}
			return 0, "", time.Time{}, false
		})
	}
}

func scanGenericDroppedPendingTask(ctx context.Context, jobID int64, zsetKeyTpl, taskKeyTpl, queueName string, parseFunc func(string) (int64, string, time.Time, bool)) {
	jobKey := fmt.Sprintf(zsetKeyTpl, jobID)
	batchSize := 1000
	var offset int64 = 0
	loops := 0

	// Limit scan depth to avoid performance hit on large jobs
	// We assume "dropped" pending tasks are likely at the beginning (skipped by offset) or near the end?
	// Actually, if offset skipped them, they are at the beginning.
	// So we scan from 0.
	for {
		loops++
		if loops > 50 { // Scan max 50k tasks per job
			break
		}

		ids, err := database.RDB.ZRange(ctx, jobKey, offset, offset+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, id := range ids {
			keys = append(keys, fmt.Sprintf(taskKeyTpl, id))
		}

		tasksJSON, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			offset += int64(batchSize)
			continue
		}

		foundCount := 0
		// For atomic updates if we wanted, but here we just push to queue and maybe update UpdatedAt?
		// Actually, we should update UpdatedAt to prevent immediate re-scan
		
		var updates []string
		
		for i, val := range tasksJSON {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			id, status, refTime, ok := parseFunc(str)
			if ok && status == "PENDING" {
				// Check age: 30 minutes
				if time.Since(refTime) > 30*time.Minute {
					// Found stuck pending task
					database.RDB.RPush(ctx, queueName, id)
					// Update UpdatedAt in Redis? 
					// Parsing and unmarshaling again is expensive.
					// For now, we trust the queue consumers will pick it up and update UpdatedAt soon.
					// Or we can just log.
					foundCount++
					
					// Optional: Update UpdatedAt via Lua or simple SET if we had the struct.
					// Since we are inside a generic function without the full struct, we skip update.
					// The risk is re-queuing every 10 mins if consumers are slow.
					// But we checked Queue Length at start, so consumers shouldn't be slow.
				}
			}
			
			// optimization: if we found running/completed, we might be scanning processed territory.
			// But ZSet is ordered by ID usually.
			_ = i
		}
		_ = updates

		if foundCount > 0 {
			// limit log spam
			// fmt.Printf("Recovered %d stuck pending tasks for Job %d (Type: %s)\n", foundCount, jobID, queueName)
		}

		if len(ids) < batchSize {
			break
		}
		offset += int64(batchSize)
	}
}

func flushStats() {
	ctx := context.Background()

	// 1. Youtube Jobs
	flushGenericStats(ctx, StatsDirtySetYoutube, "job:%d:stats", "youtube_jobs")

	// 2. Transfer Jobs
	flushGenericStats(ctx, StatsDirtySetTransfer, "tx:job:%d:stats", "transfer_jobs")

	// 3. Ffmpeg Jobs
	flushGenericStats(ctx, StatsDirtySetFfmpeg, "ff:job:%d:stats", "ffmpeg_jobs")
}

func flushGenericStats(ctx context.Context, setKey, redisHashKeyTpl, tableName string) {
	jobIDsStr, err := database.RDB.SPopN(ctx, setKey, 50).Result()
	if err != nil || len(jobIDsStr) == 0 {
		return
	}

	for _, idStr := range jobIDsStr {
		jobID, _ := strconv.ParseInt(idStr, 10, 64)
		if jobID == 0 {
			continue
		}

		statsKey := fmt.Sprintf(redisHashKeyTpl, jobID)
		stats, err := database.RDB.HGetAll(ctx, statsKey).Result()
		if err != nil {
			continue
		}

		pending, _ := strconv.Atoi(stats["pending"])
		running, _ := strconv.Atoi(stats["running"])
		success, _ := strconv.Atoi(stats["success"])
		failed, _ := strconv.Atoi(stats["failed"])

		idCol := "id"
		if tableName == "transfer_jobs" {
			idCol = "job_id"
		}

		query := fmt.Sprintf(`
			UPDATE %s SET 
				pending_count = ?, 
				running_count = ?, 
				success_count = ?, 
				failed_count = ?,
				status = CASE 
					WHEN status = 'PENDING' AND ? > 0 THEN 'RUNNING'
					WHEN status = 'RUNNING' AND ? <= 0 AND ? <= 0 THEN 'COMPLETED'
					WHEN (status = 'COMPLETED' OR status = 'FAILED' OR status = 'STOPPED') AND ? > 0 THEN 'PENDING'
					ELSE status 
				END
			WHERE %s = ?
		`, tableName, idCol)

		database.DB.Exec(query,
			pending, running, success, failed,
			running,          // PENDING -> RUNNING
			pending, running, // RUNNING -> COMPLETED
			pending,          // REVIVAL -> PENDING
			jobID,
		)
	}
}

// trackStatusChange for Youtube Jobs
func trackStatusChange(pipe redis.Pipeliner, jobID int64, oldStatus, newStatus string) {
	if oldStatus == newStatus {
		return
	}
	trackGenericStatusChange(pipe, fmt.Sprintf("job:%d:stats", jobID), StatsDirtySetYoutube, jobID, oldStatus, newStatus, "youtube")
}

// trackTransferStatusChange for Transfer Jobs
func trackTransferStatusChange(pipe redis.Pipeliner, jobID int64, oldStatus, newStatus string) {
	if oldStatus == newStatus {
		return
	}
	trackGenericStatusChange(pipe, fmt.Sprintf("tx:job:%d:stats", jobID), StatsDirtySetTransfer, jobID, oldStatus, newStatus, "transfer")
}

// trackFfmpegStatusChange for Ffmpeg Jobs
func trackFfmpegStatusChange(pipe redis.Pipeliner, jobID int64, oldStatus, newStatus string) {
	if oldStatus == newStatus {
		return
	}
	trackGenericStatusChange(pipe, fmt.Sprintf("ff:job:%d:stats", jobID), StatsDirtySetFfmpeg, jobID, oldStatus, newStatus, "ffmpeg")
}

func trackGenericStatusChange(pipe redis.Pipeliner, statsKey, dirtySet string, jobID int64, oldStatus, newStatus, metricType string) {
	// Helper to map status to bucket
	getBucket := func(s string) string {
		switch s {
		case "PENDING":
			return "pending"
		case "COMPLETED":
			return "success"
		case "FAILED":
			return "failed"
		default:
			return "running"
		}
	}

	oldBucket := getBucket(oldStatus)
	newBucket := getBucket(newStatus)

	if oldBucket == newBucket {
		return
	}

	metrics.TaskStatusChangeTotal.WithLabelValues(metricType, newBucket).Inc()

	pipe.HIncrBy(context.Background(), statsKey, oldBucket, -1)
	pipe.HIncrBy(context.Background(), statsKey, newBucket, 1)
	pipe.SAdd(context.Background(), dirtySet, jobID)
}

func checkAndRefillBuffers() {
	var jobs []models.YoutubeJob
	// Find active jobs
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	ctx := context.Background()

	for _, job := range jobs {
		jid := int64(job.ID)
		ensureBuffer(jid)

		bufferMutex.RLock()
		ch, exists := jobBuffers[jid]
		bufferMutex.RUnlock()

		if !exists {
			continue
		}

		// Check for stuck state: Pending > 0, Buffer Empty, Not Filling, Offset >= Total
		if job.PendingCount > 0 && len(ch) == 0 {
			if _, filling := fillingMap.Load(jid); !filling {
				offsetKey := fmt.Sprintf("job:%d:offset", jid)
				jobKey := fmt.Sprintf("job:%d:tasks", jid)

				total, _ := database.RDB.ZCard(ctx, jobKey).Result()
				offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
				var offset int64
				if offsetStr != "" {
					fmt.Sscanf(offsetStr, "%d", &offset)
				}

				if total > 0 && offset >= total {
					// Reset offset
					fmt.Printf("Resetting offset for stuck job %d (Total: %d, Offset: %d, Pending: %d)\n", jid, total, offset, job.PendingCount)
					database.RDB.Set(ctx, offsetKey, 0, 0)
				}
			}
		}

		if len(ch) < BufferLowWater {
			triggerRefill(jid)
		}
	}
}

func ensureBuffer(jobID int64) {
	bufferMutex.Lock()
	defer bufferMutex.Unlock()
	if _, ok := jobBuffers[jobID]; !ok {
		jobBuffers[jobID] = make(chan models.YoutubeTask, BufferSize)
	}
}

func triggerRefill(jobID int64) {
	// Trigger refill if not already filling
	if _, filling := fillingMap.Load(jobID); !filling {
		fillingMap.Store(jobID, true)
		go func(jid int64) {
			defer fillingMap.Delete(jid)
			fillJobBuffer(jid)
		}(jobID)
	}
}

func fillJobBuffer(jobID int64) {
	ctx := context.Background()
	lockKey := fmt.Sprintf("job:%d:lock", jobID)

	// Acquire Lock
	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()
	if err != nil || !ok {
		return // Failed to acquire lock
	}
	defer database.RDB.Del(ctx, lockKey)

	// Get Offset
	offsetKey := fmt.Sprintf("job:%d:offset", jobID)
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var offset int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}

	// Fetch batch from Redis
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)
	start := offset
	stop := offset + int64(FetchBatchSize) - 1

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()
	if err != nil || len(ids) == 0 {
		return
	}

	// Fetch details
	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("task:%s", id))
	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		return
	}

	// Push to buffer
	bufferMutex.RLock()
	ch, exists := jobBuffers[jobID]
	bufferMutex.RUnlock()

	if !exists {
		return
	}

	count := 0
	processed := 0
	for _, item := range jsonList {
		processed++
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.YoutubeTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status != "PENDING" {
				continue
			}

			// Non-blocking send or timeout?
			// Since we check len < LowWater, there should be space.
			// But to be safe, use select
			select {
			case ch <- task:
				count++
			default:
				// Buffer full, stop filling
				processed--
				goto FINISH
			}
		}
	}

FINISH:
	// Update offset
	if processed > 0 {
		newOffset := offset + int64(processed)
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
	}
}

func BatchInsert(c *gin.Context) {
	var tasks []models.YoutubeTask
	if err := c.ShouldBindJSON(&tasks); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(tasks) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No tasks to insert"})
		return
	}

	ctx := context.Background()
	pipe := database.RDB.Pipeline()

	for _, task := range tasks {
		data, err := json.Marshal(task)
		if err != nil {
			continue
		}

		taskKey := fmt.Sprintf("task:%d", task.ID)
		jobKey := fmt.Sprintf("job:%d:tasks", task.JobID)
		statsKey := fmt.Sprintf("job:%d:stats", task.JobID)

		pipe.Set(ctx, taskKey, data, 0)
		pipe.ZAdd(ctx, jobKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})

		// Update Stats
		pipe.HIncrBy(ctx, statsKey, "pending", 1)
		pipe.SAdd(ctx, StatsDirtySetYoutube, task.JobID)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save tasks: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"count": len(tasks)})
}

// Lua script for atomic task update (single slot)
const updateTaskScript = `
local taskKey = KEYS[1]
local newTaskJSON = ARGV[1]
local newStatus = ARGV[2]
local shouldDelete = ARGV[3]

local currentVal = redis.call("GET", taskKey)
if not currentVal then
    return "MISSING"
end

local oldStatus = "PENDING"
local decoded = cjson.decode(currentVal)
if decoded and decoded.Status then
    oldStatus = decoded.Status
end

if shouldDelete == "1" and newStatus == "COMPLETED" then
    redis.call("DEL", taskKey)
else
    redis.call("SET", taskKey, newTaskJSON)
end

return oldStatus
`

func getStatsBucket(s string) string {
	switch s {
	case "PENDING":
		return "pending"
	case "COMPLETED":
		return "success"
	case "FAILED":
		return "failed"
	default:
		return "running"
	}
}

func BatchUpdate(c *gin.Context) {
	var updates []models.YoutubeTask
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(updates) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No updates"})
		return
	}

	ctx := context.Background()

	// 1. Fetch existing tasks to merge updates
	var keys []string
	for _, u := range updates {
		keys = append(keys, fmt.Sprintf("task:%d", u.ID))
	}

	existingJSONs, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch existing tasks: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()
	// Struct to hold context for each operation
	type updateOp struct {
		cmd          *redis.Cmd
		task         models.YoutubeTask
		shouldDelete string
		newStatus    string
	}
	var ops []updateOp

	for i, u := range updates {
		val := existingJSONs[i]
		if val == nil {
			continue // Task not found
		}

		var existing models.YoutubeTask
		if err := json.Unmarshal([]byte(val.(string)), &existing); err != nil {
			continue
		}

		// 2. Merge fields
		if u.Status != "" {
			existing.Status = u.Status
		}
		if u.ErrorMessage != "" {
			existing.ErrorMessage = u.ErrorMessage
		}
		if u.WorkerID != "" {
			existing.WorkerID = u.WorkerID
		}
		if u.IsDownloadFail {
			existing.IsDownloadFail = true
		}
		if u.Status == "RUNNING" || u.Status == "PENDING" {
			existing.IsDownloadFail = false
			existing.ErrorMessage = ""
		}
		if u.AudioURL != "" {
			existing.AudioURL = u.AudioURL
		}
		if u.AudioSize != 0 {
			existing.AudioSize = u.AudioSize
		}
		if u.VideoURL != "" {
			existing.VideoURL = u.VideoURL
		}
		if u.VideoSize != 0 {
			existing.VideoSize = u.VideoSize
		}
		if u.Title != "" {
			existing.Title = u.Title
		}
		if u.VideoID != "" {
			existing.VideoID = u.VideoID
		}

		existing.UpdatedAt = time.Now()
		if u.Status == "RUNNING" && existing.StartedAt.IsZero() {
			existing.StartedAt = time.Now()
		}
		if (u.Status == "COMPLETED" || u.Status == "FAILED") && existing.CompletedAt.IsZero() {
			existing.CompletedAt = time.Now()
		}

		existingCopy := existing // Safety copy

		// Prepare Lua Script Args
		data, _ := json.Marshal(existingCopy)
		taskKey := fmt.Sprintf("task:%d", existingCopy.ID)

		shouldDelete := "0"
		// Optimization: Delete successful tasks from Redis to save space
		if existingCopy.Status == "COMPLETED" {
			shouldDelete = "1"
		}

		// KEYS: [taskKey]
		// ARGV: [newTaskJSON, newStatus, shouldDelete]
		cmd := pipe.Eval(ctx, updateTaskScript,
			[]string{taskKey},
			data, existingCopy.Status, shouldDelete,
		)
		ops = append(ops, updateOp{
			cmd:          cmd,
			task:         existingCopy,
			shouldDelete: shouldDelete,
			newStatus:    existingCopy.Status,
		})

		// Status Machine Transition: METADATA_FETCHED -> Ready for Download
		if u.Status == "METADATA_FETCHED" {
			pipe.RPush(ctx, "queue:youtube:download_ready", existingCopy.ID)
		}
	}

	if len(ops) > 0 {
		_, err = pipe.Exec(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update tasks: " + err.Error()})
			return
		}

		// Post-process metrics and side effects
		statsPipe := database.RDB.Pipeline()
		hasStatsUpdates := false

		for _, op := range ops {
			res, err := op.cmd.Result()
			if err == nil {
				oldStatus, ok := res.(string)
				if ok && oldStatus != "MISSING" {
					// Check if status changed
					if oldStatus != op.newStatus {
						oldB := getStatsBucket(oldStatus)
						newB := getStatsBucket(op.newStatus)

						if oldB != newB {
							statsKey := fmt.Sprintf("job:%d:stats", op.task.JobID)
							statsPipe.HIncrBy(ctx, statsKey, oldB, -1)
							statsPipe.HIncrBy(ctx, statsKey, newB, 1)
							statsPipe.SAdd(ctx, StatsDirtySetYoutube, op.task.JobID)
							hasStatsUpdates = true

							// Metrics
							metrics.TaskStatusChangeTotal.WithLabelValues("youtube", newB).Inc()
						}
					}

					// Handle ZSet and CompletedHash
					jobKey := fmt.Sprintf("job:%d:tasks", op.task.JobID)
					completedKey := fmt.Sprintf("job:%d:completed", op.task.JobID)

					if op.shouldDelete == "1" && op.newStatus == "COMPLETED" {
						statsPipe.ZRem(ctx, jobKey, op.task.ID)
						if op.task.URL != "" {
							statsPipe.HSet(ctx, completedKey, op.task.URL, "1")
						}
						hasStatsUpdates = true
					} else {
						// Ensure in ZSet
						statsPipe.ZAdd(ctx, jobKey, redis.Z{Score: float64(op.task.ID), Member: op.task.ID})
						hasStatsUpdates = true
					}
				}
			}
		}

		if hasStatsUpdates {
			statsPipe.Exec(ctx)
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func AcquireTasks(c *gin.Context) {
	type AcquireRequest struct {
		WorkerID string `json:"worker_id"`
		Stage    string `json:"stage"` // "metadata" (default) or "download"
		Limit    int    `json:"limit"`
	}

	var req AcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.Stage == "" {
		req.Stage = "metadata"
	}

	ctx := context.Background()
	tasks := []models.YoutubeTask{}

	if req.Stage == "download" {
		// Pop from download queue
		// RPopCount is available in newer redis, but let's loop for safety with older go-redis or just RPop
		// We use a loop with RPOP for now
		for i := 0; i < req.Limit; i++ {
			idStr, err := database.RDB.LPop(ctx, "queue:youtube:download_ready").Result()
			if err == redis.Nil {
				break
			}
			if err != nil {
				continue
			}

			// Fetch task details
			taskData, err := database.RDB.Get(ctx, fmt.Sprintf("task:%s", idStr)).Result()
			if err == nil {
				var t models.YoutubeTask
				if err := json.Unmarshal([]byte(taskData), &t); err == nil {
					// Optimization: Check if actually METADATA_FETCHED?
					// Strictly check status to avoid duplicates from recovery scan
					if t.Status == "METADATA_FETCHED" {
						tasks = append(tasks, t)
					}
				}
			}
		}
	} else {
		// Metadata stage

		// 1. Check Retry Queue
		for len(tasks) < req.Limit {
			idStr, err := database.RDB.LPop(ctx, "queue:youtube:metadata_retry").Result()
			if err == redis.Nil {
				break
			}
			if err == nil {
				taskData, err := database.RDB.Get(ctx, fmt.Sprintf("task:%s", idStr)).Result()
				if err == nil {
					var t models.YoutubeTask
					if err := json.Unmarshal([]byte(taskData), &t); err == nil {
						if t.Status == "PENDING" {
							tasks = append(tasks, t)
						}
					}
				}
			}
		}

		if len(tasks) >= req.Limit {
			c.JSON(http.StatusOK, tasks)
			return
		}

		// 2. Round-robin across active job buffers
		// We need to iterate over map.
		bufferMutex.RLock()
		// Get keys
		var jobIDs []int64
		for jid := range jobBuffers {
			jobIDs = append(jobIDs, jid)
		}
		bufferMutex.RUnlock()

		if len(jobIDs) == 0 {
			c.JSON(http.StatusOK, tasks)
			return
		}

		// Simple strategy: Try each job until we have enough tasks
		for _, jid := range jobIDs {
			if len(tasks) >= req.Limit {
				break
			}

			bufferMutex.RLock()
			ch, ok := jobBuffers[jid]
			bufferMutex.RUnlock()

			if !ok {
				continue
			}

			// Proactive refill
			if len(ch) < BufferLowWater {
				triggerRefill(jid)
			}

			// Drain what we can
		loop:
			for len(tasks) < req.Limit {
				select {
				case t := <-ch:
					// Check if status is PENDING?
					// The buffer *should* ideally only contain pending, but if we have restarts...
					// For now assume buffer is raw tasks from ZRange.
					// We might want to check status if we want strict PENDING.
					// But let's assume Python worker handles idempotency.
					tasks = append(tasks, t)
				default:
					break loop
				}
			}
		}
	}

	c.JSON(http.StatusOK, tasks) // Direct array response
}

func BatchFetch(c *gin.Context) {
	type FetchRequest struct {
		JobID  int64 `json:"job_id"`
		Limit  int   `json:"limit"`
		Offset int   `json:"offset"`
	}

	var req FetchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := context.Background()
	jobKey := fmt.Sprintf("job:%d:tasks", req.JobID)

	// Get Total Count
	total, err := database.RDB.ZCard(ctx, jobKey).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get count: " + err.Error()})
		return
	}

	if total == 0 {
		c.JSON(http.StatusOK, gin.H{"tasks": []models.YoutubeTask{}, "total": 0})
		return
	}

	// Fetch IDs (Pagination)
	// Using ZRange (0 to -1 is all, so we use start/stop)
	start := int64(req.Offset)
	stop := int64(req.Offset + req.Limit - 1)

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch IDs: " + err.Error()})
		return
	}

	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"tasks": []models.YoutubeTask{}, "total": total})
		return
	}

	// Fetch details
	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("task:%s", id))
	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch details: " + err.Error()})
		return
	}

	var tasks []models.YoutubeTask
	for _, item := range jsonList {
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var t models.YoutubeTask
		if err := json.Unmarshal([]byte(str), &t); err == nil {
			tasks = append(tasks, t)
		}
	}

	c.JSON(http.StatusOK, gin.H{"tasks": tasks, "total": total})
}

func BatchDelete(c *gin.Context) {
	type DeleteRequest struct {
		IDs []int64 `json:"ids"`
	}
	var req DeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.IDs) == 0 {
		c.JSON(http.StatusOK, gin.H{"status": "no ids"})
		return
	}

	ctx := context.Background()

	// First, we need to know which job these tasks belong to, to remove from ZSet.
	// We can MGet them.
	var keys []string
	for _, id := range req.IDs {
		keys = append(keys, fmt.Sprintf("task:%d", id))
	}

	// We handle this in chunks or just MGet all. Assuming reasonable batch size.
	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch tasks for deletion: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()

	for i, item := range jsonList {
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.YoutubeTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			// Remove from Job ZSet
			jobKey := fmt.Sprintf("job:%d:tasks", task.JobID)
			pipe.ZRem(ctx, jobKey, task.ID)

			// Update Stats (Decrement)
			statsKey := fmt.Sprintf("job:%d:stats", task.JobID)
			bucket := "pending"
			switch task.Status {
			case "RUNNING":
				bucket = "running"
			case "COMPLETED":
				bucket = "success"
			case "FAILED":
				bucket = "failed"
			case "PENDING":
				bucket = "pending"
			default:
				bucket = "running"
			}
			pipe.HIncrBy(ctx, statsKey, bucket, -1)
			pipe.SAdd(ctx, StatsDirtySetYoutube, task.JobID)
		}

		// Remove Task Key
		pipe.Del(ctx, keys[i])
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete tasks: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

// AddTasksToJob adds new tasks to an existing job in Redis
func AddTasksToJob(jobID int64, urls []string) (int, error) {
	if len(urls) == 0 {
		return 0, nil
	}

	ctx := context.Background()
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)
	statsKey := fmt.Sprintf("job:%d:stats", jobID)
	completedKey := fmt.Sprintf("job:%d:completed", jobID)

	// 0. Filter out already completed tasks (dedup)
	// We check against the completed hash
	checkPipe := database.RDB.Pipeline()
	for _, url := range urls {
		checkPipe.HExists(ctx, completedKey, url)
	}
	results, err := checkPipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	var newUrls []string
	for i, res := range results {
		exists, _ := res.(*redis.BoolCmd).Result()
		if !exists {
			newUrls = append(newUrls, urls[i])
		}
	}

	if len(newUrls) == 0 {
		return 0, nil
	}
	urls = newUrls

	// 1. Determine start ID
	// Try to get the last ID from ZSet to continue sequence
	lastIDs, err := database.RDB.ZRevRange(ctx, jobKey, 0, 0).Result()
	var startID int64
	if err != nil || len(lastIDs) == 0 {
		// No tasks yet, use default start
		startID = jobID * 1000000
	} else {
		// Parse last ID
		// Redis returns string member
		fmt.Sscanf(lastIDs[0], "%d", &startID)
		startID++ // Start from next
	}

	now := time.Now()
	batchSize := 10000
	totalAdded := 0

	// 2. Process in batches to reduce memory usage and pipeline size
	for i := 0; i < len(urls); i += batchSize {
		end := i + batchSize
		if end > len(urls) {
			end = len(urls)
		}

		batchUrls := urls[i:end]
		var zMembers []redis.Z
		pipe := database.RDB.Pipeline()

		for j, url := range batchUrls {
			taskID := startID + int64(i+j)
			task := models.YoutubeTask{
				ID:        taskID,
				JobID:     jobID,
				URL:       url,
				Status:    "PENDING",
				CreatedAt: now,
				UpdatedAt: now,
			}

			data, err := json.Marshal(task)
			if err != nil {
				continue
			}

			taskKey := fmt.Sprintf("task:%d", task.ID)
			pipe.Set(ctx, taskKey, data, 0)

			zMembers = append(zMembers, redis.Z{
				Score:  float64(task.ID),
				Member: task.ID,
			})
		}

		if len(zMembers) > 0 {
			// Optimize: Single ZAdd for the whole batch
			pipe.ZAdd(ctx, jobKey, zMembers...)

			// Update Stats
			pipe.HIncrBy(ctx, statsKey, "pending", int64(len(zMembers)))
			pipe.SAdd(ctx, StatsDirtySetYoutube, jobID)

			_, err := pipe.Exec(ctx)
			if err != nil {
				return totalAdded, err
			}
			totalAdded += len(zMembers)
		}
	}

	return totalAdded, nil
}

type TransferTaskInput struct {
	Src  string `json:"src"`
	Size int64  `json:"size"`
}

// AddTransferTasksToJob adds new transfer tasks to an existing job in Redis with deduplication
func AddTransferTasksToJob(jobID int64, inputs []TransferTaskInput) (int, error) {
	if len(inputs) == 0 {
		return 0, nil
	}

	// 0. Dedup input slice in memory
	uniqueInputs := make([]TransferTaskInput, 0, len(inputs))
	seen := make(map[string]bool)
	for _, input := range inputs {
		if !seen[input.Src] {
			seen[input.Src] = true
			uniqueInputs = append(uniqueInputs, input)
		}
	}
	inputs = uniqueInputs // Use unique list

	ctx := context.Background()
	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	dedupKey := fmt.Sprintf("tx:job:%d:dedup", jobID) // Hash: Src -> 1 (or TaskID)
	statsKey := fmt.Sprintf("tx:job:%d:stats", jobID)

	// 1. Filter out duplicates from Redis
	// Pipeline exists check
	checkPipe := database.RDB.Pipeline()
	for _, input := range inputs {
		checkPipe.HExists(ctx, dedupKey, input.Src)
	}
	results, err := checkPipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	var newInputs []TransferTaskInput
	for i, res := range results {
		exists, _ := res.(*redis.BoolCmd).Result()
		if !exists {
			newInputs = append(newInputs, inputs[i])
		}
	}

	if len(newInputs) == 0 {
		return 0, nil
	}

	// 2. Determine start ID
	lastIDs, err := database.RDB.ZRevRange(ctx, jobKey, 0, 0).Result()
	var startID int64
	if err != nil || len(lastIDs) == 0 {
		startID = jobID * 1000000
	} else {
		// Parse last ID
		// Redis returns string member
		var member string = lastIDs[0]
		fmt.Sscanf(member, "%d", &startID)
		startID++
	}

	var tasks []models.TransferTask
	now := time.Now()

	// 3. Prepare tasks and Pipeline Insert
	pipe := database.RDB.Pipeline()

	for i, input := range newInputs {
		task := models.TransferTask{
			ID:        startID + int64(i),
			JobID:     jobID,
			Src:       input.Src,
			Size:      input.Size,
			Status:    "PENDING",
			CreatedAt: now,
			UpdatedAt: now,
		}
		tasks = append(tasks, task)

		data, err := json.Marshal(task)
		if err != nil {
			continue
		}

		taskKey := fmt.Sprintf("tx:task:%d", task.ID)
		pipe.Set(ctx, taskKey, data, 0)
		pipe.ZAdd(ctx, jobKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})
		// Add to dedup map
		pipe.HSet(ctx, dedupKey, input.Src, task.ID)
	}

	// Update Stats
	if len(tasks) > 0 {
		pipe.HIncrBy(ctx, statsKey, "pending", int64(len(tasks)))
		pipe.SAdd(ctx, StatsDirtySetTransfer, jobID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(tasks), nil
}

// --- Transfer Task Buffer Logic ---

func checkAndRefillTxBuffers() {
	var jobs []models.TransferJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning, models.StatusCompleted}).Find(&jobs).Error; err != nil {
		return
	}

	ctx := context.Background()

	for _, job := range jobs {
		jid := int64(job.JobID)
		ensureTxBuffer(jid)

		bufferMutex.RLock()
		ch, exists := txJobBuffers[jid]
		bufferMutex.RUnlock()

		if !exists {
			continue
		}

		// Check for stuck state (Pending > 0, Buffer Empty, Not Filling, Offset >= Total)
		if job.PendingCount > 0 && len(ch) == 0 {
			key := fmt.Sprintf("tx:%d", jid)
			if _, filling := fillingMap.Load(key); !filling {
				offsetKey := fmt.Sprintf("tx:job:%d:offset", jid)
				jobKey := fmt.Sprintf("tx:job:%d:tasks", jid)

				total, _ := database.RDB.ZCard(ctx, jobKey).Result()
				offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
				var offset int64
				if offsetStr != "" {
					fmt.Sscanf(offsetStr, "%d", &offset)
				}

				if total > 0 && offset >= total {
					// Reset offset
					fmt.Printf("Resetting offset for stuck transfer job %d (Total: %d, Offset: %d, Pending: %d)\n", jid, total, offset, job.PendingCount)
					database.RDB.Set(ctx, offsetKey, 0, 0)
				}
			}
		}

		if len(ch) < BufferLowWater {
			triggerTxRefill(jid)
		}
	}
}

func ensureTxBuffer(jobID int64) {
	bufferMutex.Lock()
	defer bufferMutex.Unlock()
	if _, ok := txJobBuffers[jobID]; !ok {
		txJobBuffers[jobID] = make(chan models.TransferTask, BufferSize)
	}
}

func triggerTxRefill(jobID int64) {
	key := fmt.Sprintf("tx:%d", jobID)
	if _, filling := fillingMap.Load(key); !filling {
		fillingMap.Store(key, true)
		go func(jid int64) {
			defer fillingMap.Delete(fmt.Sprintf("tx:%d", jid))
			fillTxJobBuffer(jid)
		}(jobID)
	}
}

func fillTxJobBuffer(jobID int64) {
	ctx := context.Background()
	lockKey := fmt.Sprintf("tx:job:%d:lock", jobID)

	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()
	if err != nil || !ok {
		return
	}
	defer database.RDB.Del(ctx, lockKey)

	offsetKey := fmt.Sprintf("tx:job:%d:offset", jobID)
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var offset int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &offset)
	}

	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	start := offset
	stop := offset + int64(FetchBatchSize) - 1

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()
	if err != nil || len(ids) == 0 {
		return
	}

	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("tx:task:%s", id))
	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		return
	}

	bufferMutex.RLock()
	ch, exists := txJobBuffers[jobID]
	bufferMutex.RUnlock()

	if !exists {
		return
	}

	count := 0
	processed := 0
	for _, item := range jsonList {
		processed++
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.TransferTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status != "PENDING" {
				continue
			}
			select {
			case ch <- task:
				count++
			default:
				// Buffer full, back off the 'processed' count for this item as we didn't consume it
				processed--
				goto FINISH
			}
		}
	}

FINISH:
	if processed > 0 {
		newOffset := offset + int64(processed)
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
	}
}

func AcquireTransferTasks(c *gin.Context) {
	type AcquireRequest struct {
		WorkerID string `json:"worker_id"`
		Limit    int    `json:"limit"`
	}
	var req AcquireRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}

	tasks := []models.TransferTask{}
	ctx := context.Background()

	// 1. Check Retry Queue
	for len(tasks) < req.Limit {
		idStr, err := database.RDB.LPop(ctx, "queue:transfer:retry").Result()
		if err == redis.Nil {
			break
		}
		if err == nil {
			taskData, err := database.RDB.Get(ctx, fmt.Sprintf("tx:task:%s", idStr)).Result()
			if err == nil {
				var t models.TransferTask
				if err := json.Unmarshal([]byte(taskData), &t); err == nil {
					if t.Status == "PENDING" {
						tasks = append(tasks, t)
					}
				}
			}
		}
	}

	if len(tasks) >= req.Limit {
		c.JSON(http.StatusOK, tasks)
		return
	}

	bufferMutex.RLock()
	var jobIDs []int64
	for jid := range txJobBuffers {
		jobIDs = append(jobIDs, jid)
	}
	bufferMutex.RUnlock()

	if len(jobIDs) == 0 {
		c.JSON(http.StatusOK, tasks)
		return
	}

	// Simple round robin or random
	// Ideally verify Job Status is RUNNING here too, but buffer manager handles removal?
	// Currently buffer manager doesn't remove keys from map if job stops.
	// But `checkAndRefill` only checks active jobs.
	// We might serve stale buffer for stopped jobs. Acceptable for now.

	for _, jid := range jobIDs {
		if len(tasks) >= req.Limit {
			break
		}

		bufferMutex.RLock()
		ch, ok := txJobBuffers[jid]
		bufferMutex.RUnlock()
		if !ok {
			continue
		}

		if len(ch) < BufferLowWater {
			triggerTxRefill(jid)
		}

	loop:
		for len(tasks) < req.Limit {
			select {
			case t := <-ch:
				tasks = append(tasks, t)
			default:
				break loop
			}
		}
	}
	c.JSON(http.StatusOK, tasks)
}

func BatchUpdateTransfer(c *gin.Context) {
	var updates []models.TransferTask
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(updates) == 0 {
		c.JSON(http.StatusOK, gin.H{"status": "no updates"})
		return
	}

	ctx := context.Background()

	// 1. Fetch existing tasks
	var keys []string
	for _, u := range updates {
		keys = append(keys, fmt.Sprintf("tx:task:%d", u.ID))
	}

	existingJSONs, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch existing tasks: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()
	// Struct to hold context
	type updateOp struct {
		cmd          *redis.Cmd
		task         models.TransferTask
		shouldDelete string
		newStatus    string
	}
	var ops []updateOp

	for i, u := range updates {
		val := existingJSONs[i]
		if val == nil {
			continue
		}
		str, ok := val.(string)
		if !ok {
			continue
		}

		var existing models.TransferTask
		if err := json.Unmarshal([]byte(str), &existing); err != nil {
			continue
		}

		// 2. Merge fields
		if u.Status != "" {
			existing.Status = u.Status
		}
		if u.ErrorMessage != "" {
			existing.ErrorMessage = u.ErrorMessage
		}
		if u.WorkerID != "" {
			existing.WorkerID = u.WorkerID
		}
		// Clear error if retrying
		if u.Status == "PENDING" || u.Status == "RUNNING" {
			existing.ErrorMessage = ""
		}

		existing.UpdatedAt = time.Now()
		if u.Status == "RUNNING" && existing.StartedAt.IsZero() {
			existing.StartedAt = time.Now()
		}
		if (u.Status == "COMPLETED" || u.Status == "FAILED") && existing.CompletedAt.IsZero() {
			existing.CompletedAt = time.Now()
		}

		existingCopy := existing // Safety Copy

		// Prepare Lua Script
		data, err := json.Marshal(existingCopy)
		if err != nil {
			continue
		}

		taskKey := fmt.Sprintf("tx:task:%d", existingCopy.ID)

		// Transfer tasks are not deleted on completion in current logic
		shouldDelete := "0"

		// KEYS: [taskKey]
		// ARGV: [newTaskJSON, newStatus, shouldDelete]
		cmd := pipe.Eval(ctx, updateTaskScript,
			[]string{taskKey},
			data, existingCopy.Status, shouldDelete,
		)
		ops = append(ops, updateOp{
			cmd:          cmd,
			task:         existingCopy,
			shouldDelete: shouldDelete,
			newStatus:    existingCopy.Status,
		})
	}

	if len(ops) > 0 {
		_, err = pipe.Exec(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Post-process stats
		statsPipe := database.RDB.Pipeline()
		hasStatsUpdates := false

		for _, op := range ops {
			res, err := op.cmd.Result()
			if err == nil {
				oldStatus, ok := res.(string)
				if ok && oldStatus != "MISSING" {
					if oldStatus != op.newStatus {
						oldB := getStatsBucket(oldStatus)
						newB := getStatsBucket(op.newStatus)
						if oldB != newB {
							statsKey := fmt.Sprintf("tx:job:%d:stats", op.task.JobID)
							statsPipe.HIncrBy(ctx, statsKey, oldB, -1)
							statsPipe.HIncrBy(ctx, statsKey, newB, 1)
							statsPipe.SAdd(ctx, StatsDirtySetTransfer, op.task.JobID)
							hasStatsUpdates = true

							metrics.TaskStatusChangeTotal.WithLabelValues("transfer", newB).Inc()
						}
					}
					// ZSet Logic (Transfer jobs always keep tasks)
					jobKey := fmt.Sprintf("tx:job:%d:tasks", op.task.JobID)
					statsPipe.ZAdd(ctx, jobKey, redis.Z{Score: float64(op.task.ID), Member: op.task.ID})
					hasStatsUpdates = true
				}
			}
		}

		if hasStatsUpdates {
			statsPipe.Exec(ctx)
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// --- Ffmpeg Task Logic ---

func AddFfmpegTasksToJob(jobID int64, tasks []models.FfmpegTask) (int, error) {

	if len(tasks) == 0 {

		return 0, nil

	}

	ctx := context.Background()

	jobKey := fmt.Sprintf("ff:job:%d:tasks", jobID)
	statsKey := fmt.Sprintf("ff:job:%d:stats", jobID)

	// Determine start ID

	lastIDs, err := database.RDB.ZRevRange(ctx, jobKey, 0, 0).Result()

	var startID int64

	if err != nil || len(lastIDs) == 0 {

		startID = jobID * 1000000

	} else {

		fmt.Sscanf(lastIDs[0], "%d", &startID)

		startID++

	}

	now := time.Now()

	pipe := database.RDB.Pipeline()

	for i, task := range tasks {

		task.ID = startID + int64(i)

		task.JobID = jobID

		task.CreatedAt = now

		task.UpdatedAt = now

		data, err := json.Marshal(task)

		if err != nil {

			continue

		}

		taskKey := fmt.Sprintf("ff:task:%d", task.ID)

		pipe.Set(ctx, taskKey, data, 0)

		pipe.ZAdd(ctx, jobKey, redis.Z{

			Score: float64(task.ID),

			Member: task.ID,
		})

	}

	// Update Stats
	if len(tasks) > 0 {
		pipe.HIncrBy(ctx, statsKey, "pending", int64(len(tasks)))
		pipe.SAdd(ctx, StatsDirtySetFfmpeg, jobID)
	}

	_, err = pipe.Exec(ctx)

	if err != nil {

		return 0, err

	}

	return len(tasks), nil

}

func checkAndRefillFfmpegBuffers() {
	var jobs []models.FfmpegJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	ctx := context.Background()

	for _, job := range jobs {
		jid := int64(job.ID)
		ensureFfmpegBuffer(jid)

		bufferMutex.RLock()
		ch, exists := ffmpegJobBuffers[jid]
		bufferMutex.RUnlock()

		if !exists {
			continue
		}

		if job.PendingCount > 0 && len(ch) == 0 {
			key := fmt.Sprintf("ff:%d", jid)
			if _, filling := fillingMap.Load(key); !filling {
				offsetKey := fmt.Sprintf("ff:job:%d:offset", jid)
				jobKey := fmt.Sprintf("ff:job:%d:tasks", jid)

				total, _ := database.RDB.ZCard(ctx, jobKey).Result()
				offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
				var offset int64
				if offsetStr != "" {
					fmt.Sscanf(offsetStr, "%d", &offset)
				}

				if total > 0 && offset >= total {
					fmt.Printf("Resetting offset for stuck ffmpeg job %d (Total: %d, Offset: %d, Pending: %d)\n", jid, total, offset, job.PendingCount)
					database.RDB.Set(ctx, offsetKey, 0, 0)
				}
			}
		}

		if len(ch) < BufferLowWater {
			triggerFfmpegRefill(jid)
		}
	}
}

func ensureFfmpegBuffer(jobID int64) {

	bufferMutex.Lock()

	defer bufferMutex.Unlock()

	if _, ok := ffmpegJobBuffers[jobID]; !ok {

		ffmpegJobBuffers[jobID] = make(chan models.FfmpegTask, BufferSize)

	}

}

func triggerFfmpegRefill(jobID int64) {

	key := fmt.Sprintf("ff:%d", jobID)

	if _, filling := fillingMap.Load(key); !filling {

		fillingMap.Store(key, true)

		go func(jid int64) {

			defer fillingMap.Delete(fmt.Sprintf("ff:%d", jid))

			fillFfmpegJobBuffer(jid)

		}(jobID)

	}

}

func fillFfmpegJobBuffer(jobID int64) {

	ctx := context.Background()

	lockKey := fmt.Sprintf("ff:job:%d:lock", jobID)

	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()

	if err != nil || !ok {

		return

	}

	defer database.RDB.Del(ctx, lockKey)

	offsetKey := fmt.Sprintf("ff:job:%d:offset", jobID)

	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()

	var offset int64

	if offsetStr != "" {

		fmt.Sscanf(offsetStr, "%d", &offset)

	}

	jobKey := fmt.Sprintf("ff:job:%d:tasks", jobID)

	start := offset

	stop := offset + int64(FetchBatchSize) - 1

	ids, err := database.RDB.ZRange(ctx, jobKey, start, stop).Result()

	if err != nil || len(ids) == 0 {

		return

	}

	var keys []string

	for _, id := range ids {

		keys = append(keys, fmt.Sprintf("ff:task:%s", id))

	}

	jsonList, err := database.RDB.MGet(ctx, keys...).Result()

	if err != nil {

		return

	}

	bufferMutex.RLock()

	ch, exists := ffmpegJobBuffers[jobID]

	bufferMutex.RUnlock()

	if !exists {

		return

	}

	count := 0
	processed := 0

	for _, item := range jsonList {
		processed++

		if item == nil {
			continue
		}

		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.FfmpegTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status != "PENDING" {
				continue
			}

			select {
			case ch <- task:
				count++
			default:
				processed--
				goto FINISH
			}
		}
	}

FINISH:
	if processed > 0 {
		newOffset := offset + int64(processed)
		database.RDB.Set(ctx, offsetKey, newOffset, 0)
	}

}

func AcquireFfmpegTasks(c *gin.Context) {

	type AcquireRequest struct {
		WorkerID string `json:"worker_id"`

		Limit int `json:"limit"`
	}

	var req AcquireRequest

	if err := c.ShouldBindJSON(&req); err != nil {

		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return

	}

	if req.Limit <= 0 {
		req.Limit = 1
	}

	tasks := []models.FfmpegTask{}
	ctx := context.Background()

	// 1. Check Retry Queue
	for len(tasks) < req.Limit {
		idStr, err := database.RDB.LPop(ctx, "queue:ffmpeg:retry").Result()
		if err == redis.Nil {
			break
		}
		if err == nil {
			taskData, err := database.RDB.Get(ctx, fmt.Sprintf("ff:task:%s", idStr)).Result()
			if err == nil {
				var t models.FfmpegTask
				if err := json.Unmarshal([]byte(taskData), &t); err == nil {
					if t.Status == "PENDING" {
						tasks = append(tasks, t)
					}
				}
			}
		}
	}

	if len(tasks) >= req.Limit {
		c.JSON(http.StatusOK, tasks)
		return
	}

	bufferMutex.RLock()

	var jobIDs []int64

	for jid := range ffmpegJobBuffers {

		jobIDs = append(jobIDs, jid)

	}

	bufferMutex.RUnlock()

	if len(jobIDs) == 0 {

		c.JSON(http.StatusOK, tasks)

		return

	}

	for _, jid := range jobIDs {

		if len(tasks) >= req.Limit {
			break
		}

		bufferMutex.RLock()

		ch, ok := ffmpegJobBuffers[jid]

		bufferMutex.RUnlock()

		if !ok {
			continue
		}

		if len(ch) < BufferLowWater {

			triggerFfmpegRefill(jid)

		}

	loop:

		for len(tasks) < req.Limit {

			select {

			case t := <-ch:

				tasks = append(tasks, t)

			default:

				break loop

			}

		}

	}

	c.JSON(http.StatusOK, tasks)

}

func BatchUpdateFfmpeg(c *gin.Context) {
	var updates []models.FfmpegTask
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(updates) == 0 {
		c.JSON(http.StatusOK, gin.H{"status": "no updates"})
		return
	}

	ctx := context.Background()

	// 1. Fetch existing tasks to compare status
	var keys []string
	for _, u := range updates {
		keys = append(keys, fmt.Sprintf("ff:task:%d", u.ID))
	}

	existingJSONs, err := database.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch existing tasks: " + err.Error()})
		return
	}

	pipe := database.RDB.Pipeline()
	// Struct to hold context
	type updateOp struct {
		cmd          *redis.Cmd
		task         models.FfmpegTask
		shouldDelete string
		newStatus    string
	}
	var ops []updateOp

	for i, u := range updates {
		val := existingJSONs[i]
		if val == nil {
			continue
		}
		str, ok := val.(string)
		if !ok {
			continue
		}

		var existing models.FfmpegTask
		if err := json.Unmarshal([]byte(str), &existing); err != nil {
			continue
		}

		// Update fields
		if u.Status != "" {
			existing.Status = u.Status
		}
		if u.ErrorMessage != "" {
			existing.ErrorMessage = u.ErrorMessage
		}
		existing.UpdatedAt = time.Now()
		if existing.Status == "RUNNING" && existing.StartedAt.IsZero() {
			existing.StartedAt = time.Now()
		}
		if (existing.Status == "COMPLETED" || existing.Status == "FAILED") && existing.CompletedAt.IsZero() {
			existing.CompletedAt = time.Now()
		}
		if u.WorkerID != "" {
			existing.WorkerID = u.WorkerID
		}

		existingCopy := existing // Safety Copy

		// Prepare Lua Script
		data, _ := json.Marshal(existingCopy)
		taskKey := fmt.Sprintf("ff:task:%d", existingCopy.ID)
		
		shouldDelete := "0"

		// KEYS: [taskKey]
		// ARGV: [newTaskJSON, newStatus, shouldDelete]
		cmd := pipe.Eval(ctx, updateTaskScript,
			[]string{taskKey},
			data, existingCopy.Status, shouldDelete,
		)
		ops = append(ops, updateOp{
			cmd:          cmd,
			task:         existingCopy,
			shouldDelete: shouldDelete,
			newStatus:    existingCopy.Status,
		})
	}

	if len(ops) > 0 {
		_, err = pipe.Exec(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Post-process stats
		statsPipe := database.RDB.Pipeline()
		hasStatsUpdates := false

		for _, op := range ops {
			res, err := op.cmd.Result()
			if err == nil {
				oldStatus, ok := res.(string)
				if ok && oldStatus != "MISSING" {
					if oldStatus != op.newStatus {
						oldB := getStatsBucket(oldStatus)
						newB := getStatsBucket(op.newStatus)
						if oldB != newB {
							statsKey := fmt.Sprintf("ff:job:%d:stats", op.task.JobID)
							statsPipe.HIncrBy(ctx, statsKey, oldB, -1)
							statsPipe.HIncrBy(ctx, statsKey, newB, 1)
							statsPipe.SAdd(ctx, StatsDirtySetFfmpeg, op.task.JobID)
							hasStatsUpdates = true

							metrics.TaskStatusChangeTotal.WithLabelValues("ffmpeg", newB).Inc()
						}
					}
					// ZSet Logic (Ffmpeg tasks usually kept?)
					jobKey := fmt.Sprintf("ff:job:%d:tasks", op.task.JobID)
					statsPipe.ZAdd(ctx, jobKey, redis.Z{Score: float64(op.task.ID), Member: op.task.ID})
					hasStatsUpdates = true
				}
			}
		}

		if hasStatsUpdates {
			statsPipe.Exec(ctx)
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// ReconcileStats endpoint to manually fix broken job counts
func ReconcileStats(c *gin.Context) {
	type Req struct {
		JobID int64  `json:"job_id"`
		Type  string `json:"type"` // "youtube", "transfer", "ffmpeg"
		All   bool   `json:"all"`
	}
	var req Req
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Type == "" {
		req.Type = "youtube"
	}

	go func() {
		if req.All {
			switch req.Type {
			case "youtube":
				var jobs []models.YoutubeJob
				database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs)
				for _, job := range jobs {
					ReconcileYoutubeJobStats(int64(job.ID))
				}
			case "transfer":
				var jobs []models.TransferJob
				database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs)
				for _, job := range jobs {
					ReconcileTransferJobStats(int64(job.JobID))
				}
			case "ffmpeg":
				var jobs []models.FfmpegJob
				database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs)
				for _, job := range jobs {
					ReconcileFfmpegJobStats(int64(job.ID))
				}
			}
		} else {
			switch req.Type {
			case "youtube":
				ReconcileYoutubeJobStats(req.JobID)
			case "transfer":
				ReconcileTransferJobStats(req.JobID)
			case "ffmpeg":
				ReconcileFfmpegJobStats(req.JobID)
			}
		}
	}()

	c.JSON(http.StatusOK, gin.H{"message": "Reconciliation started"})
}

func ReconcileYoutubeJobStats(jobID int64) {
	reconcileGenericJobStats(jobID, "job:%d:tasks", "task:%s", "job:%d:stats", StatsDirtySetYoutube, "job:%d:completed", func(jsonStr string) string {
		var t models.YoutubeTask
		if err := json.Unmarshal([]byte(jsonStr), &t); err == nil {
			return t.Status
		}
		return ""
	})
}

func ReconcileTransferJobStats(jobID int64) {
	reconcileGenericJobStats(jobID, "tx:job:%d:tasks", "tx:task:%s", "tx:job:%d:stats", StatsDirtySetTransfer, "", func(jsonStr string) string {
		var t models.TransferTask
		if err := json.Unmarshal([]byte(jsonStr), &t); err == nil {
			return t.Status
		}
		return ""
	})
}

func ReconcileFfmpegJobStats(jobID int64) {
	reconcileGenericJobStats(jobID, "ff:job:%d:tasks", "ff:task:%s", "ff:job:%d:stats", StatsDirtySetFfmpeg, "", func(jsonStr string) string {
		var t models.FfmpegTask
		if err := json.Unmarshal([]byte(jsonStr), &t); err == nil {
			return t.Status
		}
		return ""
	})
}

func reconcileGenericJobStats(jobID int64, zsetKeyTpl, taskKeyTpl, statsKeyTpl, dirtySet, completedHashKeyTpl string, getStatus func(string) string) {
	ctx := context.Background()
	jobKey := fmt.Sprintf(zsetKeyTpl, jobID)

	stats := map[string]int64{
		"pending": 0,
		"running": 0,
		"success": 0,
		"failed":  0,
	}

	// 1. If there's a completed hash, get its count first (since we delete keys for completed tasks)
	if completedHashKeyTpl != "" {
		completedKey := fmt.Sprintf(completedHashKeyTpl, jobID)
		count, err := database.RDB.HLen(ctx, completedKey).Result()
		if err == nil {
			stats["success"] = count
		}
	}

	batchSize := 1000
	var offset int64 = 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, offset, offset+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var taskKeys []string
		for _, id := range ids {
			taskKeys = append(taskKeys, fmt.Sprintf(taskKeyTpl, id))
		}

		jsonList, err := database.RDB.MGet(ctx, taskKeys...).Result()
		if err != nil {
			break
		}

		for _, val := range jsonList {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			status := getStatus(str)
			switch status {
			case "PENDING":
				stats["pending"]++
			case "COMPLETED":
				stats["success"]++ // If some are still lingering in ZSet
			case "FAILED":
				stats["failed"]++
			default:
				stats["running"]++
			}
		}

		if len(ids) < batchSize {
			break
		}
		offset += int64(batchSize)
	}

	statsKey := fmt.Sprintf(statsKeyTpl, jobID)
	database.RDB.HSet(ctx, statsKey,
		"pending", stats["pending"],
		"running", stats["running"],
		"success", stats["success"],
		"failed", stats["failed"],
	)

	// Trigger DB Sync
	database.RDB.SAdd(ctx, dirtySet, jobID)
}
