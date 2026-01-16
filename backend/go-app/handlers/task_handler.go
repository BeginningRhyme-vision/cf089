package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/metrics"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// Global Buffer Manager
var (
	jobBuffers       = make(map[int64]chan models.YoutubeTask)
	txJobBuffers     = make(map[int64]chan models.TransferTask)
	ffmpegJobBuffers = make(map[int64]chan models.FfmpegTask)
	bufferMutex      sync.RWMutex
	fillingMap       sync.Map // prevent concurrent fills for same job

	// Stats Buffer for Postgres Sync
	statsBuffer     = make(map[int64]*JobDelta)
	statsMutex      sync.Mutex
	jobShardingMode sync.Map
)

type JobDelta struct {
	Pending int
	Running int
	Success int
	Failed  int
}

const (
	BufferSize     = 1000
	FetchBatchSize = 100
	BufferLowWater = 500 // Refill when below this
	LockExpiration = 30 * time.Second
	DedupShards    = 256   // 去重 Hash 分成 256 片
	TaskBucketSize = 50000 // 任务 ZSet 每 5 万个 ID 分一个桶
)

// 获取 job 模式
func isJobSharded(ctx context.Context, jobID int64) bool {
	// 1. 先查内存缓存
	if val, ok := jobShardingMode.Load(jobID); ok {
		return val.(bool)
	}

	// 2. 内存没有，去 Redis 探测旧 Key 是否存在
	oldKey := fmt.Sprintf("tx:job:%d:tasks", jobID)
	exists, _ := database.RDB.Exists(ctx, oldKey).Result()

	// 如果旧 Key 存在，说明是旧任务 (isSharded = false)
	// 如果旧 Key 不存在，默认为新任务 (isSharded = true)
	isSharded := (exists == 0)

	// 写入缓存
	jobShardingMode.Store(jobID, isSharded)
	return isSharded
}

// 计算去重 Key 的分片
func getDedupShardKey(jobID int64, src string) string {
	h := fnv32(src)
	shard := h % DedupShards
	return fmt.Sprintf("tx:job:%d:dedup:%d", jobID, shard)
}

// 计算任务 Bucket Key
func getTaskBucketKey(jobID int64, taskID int64) string {
	bucket := taskID / TaskBucketSize
	return fmt.Sprintf("tx:job:%d:tasks:%d", jobID, bucket)
}
func fnv32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

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
			}
		}
	}()
}

func scanStuckYoutubeTasks() {
	ctx := context.Background()
	var jobs []models.YoutubeJob
	// Only scan running/pending jobs
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusPending, models.StatusRunning}).Find(&jobs).Error; err != nil {
		return
	}

	for _, job := range jobs {
		jobKey := fmt.Sprintf("job:%d:tasks", job.ID)
		batchSize := 1000
		var offset int64 = 0

		for {
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

			pipe := database.RDB.Pipeline()
			hasUpdates := false

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
					// Check if Running AND Created > 3 hours
					if t.Status == "RUNNING" && time.Since(t.CreatedAt) > 3*time.Hour {
						t.Status = "PENDING"
						t.WorkerID = ""
						t.ErrorMessage = "Reset by stuck monitor"
						t.UpdatedAt = time.Now()
						t.IsDownloadFail = false

						data, _ := json.Marshal(t)
						pipe.Set(ctx, fmt.Sprintf("task:%d", t.ID), data, 0)
						pipe.RPush(ctx, "queue:youtube:metadata_retry", t.ID)

						trackStatusChange(t.JobID, "RUNNING", "PENDING")
						hasUpdates = true
					}
				}
			}

			if hasUpdates {
				pipe.Exec(ctx)
			}

			if len(ids) < batchSize {
				break
			}
			offset += int64(batchSize)
		}
	}
}

func flushStats() {
	statsMutex.Lock()
	if len(statsBuffer) == 0 {
		statsMutex.Unlock()
		return
	}

	// Copy and clear
	snapshot := make(map[int64]JobDelta)
	for k, v := range statsBuffer {
		snapshot[k] = *v
	}
	statsBuffer = make(map[int64]*JobDelta)
	statsMutex.Unlock()

	// Execute updates
	for jobID, delta := range snapshot {
		// Construct query
		// Using raw SQL for atomic updates
		// We also update status based on the *new* counts
		query := `
			UPDATE youtube_jobs SET 
				pending_count = pending_count + ?, 
				running_count = running_count + ?, 
				success_count = success_count + ?, 
				failed_count = failed_count + ?,
				status = CASE 
					WHEN status = 'PENDING' AND (running_count + ?) > 0 THEN 'RUNNING'
					WHEN status = 'RUNNING' AND (pending_count + ?) <= 0 AND (running_count + ?) <= 0 THEN 'COMPLETED'
					ELSE status 
				END
			WHERE id = ?
		`
		database.DB.Exec(query,
			delta.Pending, delta.Running, delta.Success, delta.Failed, // For count updates
			delta.Running,                // For PENDING->RUNNING check
			delta.Pending, delta.Running, // For RUNNING->COMPLETED check
			jobID,
		)
	}
}

func trackStatusChange(jobID int64, oldStatus, newStatus string) {
	if oldStatus == newStatus {
		return
	}

	statsMutex.Lock()
	defer statsMutex.Unlock()

	if _, ok := statsBuffer[jobID]; !ok {
		statsBuffer[jobID] = &JobDelta{}
	}
	d := statsBuffer[jobID]

	// Helper to map status to bucket
	getBucket := func(s string) string {
		switch s {
		case "PENDING":
			return "PENDING"
		case "COMPLETED":
			return "COMPLETED"
		case "FAILED":
			return "FAILED"
		default:
			return "RUNNING" // Everything else (METADATA_FETCHED, RUNNING, etc.) is running
		}
	}

	oldBucket := getBucket(oldStatus)
	newBucket := getBucket(newStatus)

	if oldBucket == newBucket {
		return
	}

	metrics.TaskStatusChangeTotal.WithLabelValues("youtube", newBucket).Inc()

	// Decrement old
	switch oldBucket {
	case "PENDING":
		d.Pending--
	case "RUNNING":
		d.Running--
	case "COMPLETED":
		d.Success--
	case "FAILED":
		d.Failed--
	}

	// Increment new
	switch newBucket {
	case "PENDING":
		d.Pending++
	case "RUNNING":
		d.Running++
	case "COMPLETED":
		d.Success++
	case "FAILED":
		d.Failed++
	}
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

		pipe.Set(ctx, taskKey, data, 0)
		pipe.ZAdd(ctx, jobKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save tasks: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"count": len(tasks)})
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

	for i, u := range updates {
		val := existingJSONs[i]
		if val == nil {
			continue // Task not found
		}

		var existing models.YoutubeTask
		if err := json.Unmarshal([]byte(val.(string)), &existing); err != nil {
			continue
		}

		oldStatus := existing.Status

		// 2. Merge fields (only update if provided/valid)
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
		// Clear error/failure flags if restarting
		if u.Status == "RUNNING" || u.Status == "PENDING" {
			existing.IsDownloadFail = false
			existing.ErrorMessage = ""
		}
		// Metadata fields
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

		// Track Status Change
		if u.Status != "" && existing.JobID != 0 {
			trackStatusChange(existing.JobID, oldStatus, existing.Status)
		}

		existing.UpdatedAt = time.Now()
		if u.Status == "RUNNING" && existing.StartedAt.IsZero() {
			existing.StartedAt = time.Now()
		}
		if (u.Status == "COMPLETED" || u.Status == "FAILED") && existing.CompletedAt.IsZero() {
			existing.CompletedAt = time.Now()
		}

		// 3. Save back
		data, err := json.Marshal(existing)
		if err != nil {
			continue
		}

		taskKey := fmt.Sprintf("task:%d", existing.ID)
		pipe.Set(ctx, taskKey, data, 0)

		// Ensure in Job ZSet (idempotent)
		if existing.JobID != 0 {
			jobKey := fmt.Sprintf("job:%d:tasks", existing.JobID)
			pipe.ZAdd(ctx, jobKey, redis.Z{
				Score:  float64(existing.ID),
				Member: existing.ID,
			})
		}

		// Status Machine Transition: METADATA_FETCHED -> Ready for Download
		if u.Status == "METADATA_FETCHED" {
			pipe.RPush(ctx, "queue:youtube:download_ready", existing.ID)
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update tasks: " + err.Error()})
		return
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
					// Ideally yes, but queue implies readiness.
					tasks = append(tasks, t)
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

func AddTransferTasksToJob(jobID int64, inputs []TransferTaskInput) (int, error) {
	if len(inputs) == 0 {
		return 0, nil
	}

	// 内存去重 (无论新旧都需要)
	uniqueInputs := make([]TransferTaskInput, 0, len(inputs))
	seen := make(map[string]bool)
	for _, input := range inputs {
		if !seen[input.Src] {
			seen[input.Src] = true
			uniqueInputs = append(uniqueInputs, input)
		}
	}
	inputs = uniqueInputs

	if isJobSharded(context.Background(), jobID) {
		return addShardedTransferTasks(jobID, inputs) // 【新】分片逻辑
	} else {
		return addLegacyTransferTasks(jobID, inputs) // 【旧】保持原有逻辑
	}
}

// 【新逻辑】分片写入
func addShardedTransferTasks(jobID int64, inputs []TransferTaskInput) (int, error) {
	ctx := context.Background()
	pipe := database.RDB.Pipeline()

	// 1. Redis 去重 (分片检查)
	// 使用 Pipeline 批量检查不同分片的 HExists
	dedupCmds := make([]*redis.BoolCmd, len(inputs))
	for i, input := range inputs {
		key := getDedupShardKey(jobID, input.Src)
		dedupCmds[i] = pipe.HExists(ctx, key, input.Src)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	var newInputs []TransferTaskInput
	for i, cmd := range dedupCmds {
		if !cmd.Val() {
			newInputs = append(newInputs, inputs[i])
		}
	}

	if len(newInputs) == 0 {
		return 0, nil
	}

	// 2. 生成 ID (使用独立的计数器 Key)
	// tx:job:11:max_id
	idCounterKey := fmt.Sprintf("tx:job:%d:max_id", jobID)
	endID, err := database.RDB.IncrBy(ctx, idCounterKey, int64(len(newInputs))).Result()
	if err != nil {
		return 0, err
	}
	startID := endID - int64(len(newInputs)) + 1

	// 3. 批量写入 (分片 ZSet + 分片 Dedup)
	pipe = database.RDB.Pipeline()
	now := time.Now()
	var tasks []models.TransferTask

	for i, input := range newInputs {
		taskID := startID + int64(i)

		task := models.TransferTask{
			ID:        taskID,
			JobID:     jobID,
			Src:       input.Src,
			Size:      input.Size,
			Status:    "PENDING",
			CreatedAt: now,
			UpdatedAt: now,
		}
		tasks = append(tasks, task)

		data, _ := json.Marshal(task)

		// 3.1 任务详情 (String) - 可以保持原样，或也分片，这里沿用原命名习惯
		taskKey := fmt.Sprintf("tx:task:%d:%d", jobID, task.ID)
		pipe.Set(ctx, taskKey, data, 0)

		// 3.2 任务队列 (分桶 ZSet)
		bucketKey := getTaskBucketKey(jobID, task.ID)
		pipe.ZAdd(ctx, bucketKey, redis.Z{
			Score:  float64(task.ID),
			Member: task.ID,
		})

		// 3.3 去重记录 (分片 Hash)
		dedupShard := getDedupShardKey(jobID, input.Src)
		pipe.HSet(ctx, dedupShard, input.Src, task.ID)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	return len(tasks), nil
}

// AddTransferTasksToJob adds new transfer tasks to an existing job in Redis with deduplication
func addLegacyTransferTasks(jobID int64, inputs []TransferTaskInput) (int, error) {
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

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(tasks), nil
}

// --- Transfer Task Buffer Logic ---

func checkAndRefillTxBuffers() {
	var jobs []models.TransferJob
	if err := database.DB.Where("status IN ?", []models.JobStatus{models.StatusRunning}).Find(&jobs).Error; err != nil {
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
	// 统一使用旧 Key 格式做锁（兼容性好）
	lockKey := fmt.Sprintf("tx:job:%d:lock", jobID)

	ok, err := database.RDB.SetNX(ctx, lockKey, 1, LockExpiration).Result()
	if err != nil || !ok {
		return
	}
	defer database.RDB.Del(ctx, lockKey)

	// 路由
	if isJobSharded(ctx, jobID) {
		fillShardedTxBuffer(ctx, jobID) // 【新】
	} else {
		fillLeagcyTxJobBuffer(jobID) // 【旧】
	}
}

// 【新逻辑】分片读取
func fillShardedTxBuffer(ctx context.Context, jobID int64) {
	// 新 Offset Key (无 {})
	offsetKey := fmt.Sprintf("tx:job:%d:offset", jobID)
	offsetStr, _ := database.RDB.Get(ctx, offsetKey).Result()
	var lastTaskID int64
	if offsetStr != "" {
		fmt.Sscanf(offsetStr, "%d", &lastTaskID)
	}

	// 下一个要读的 ID
	startID := lastTaskID + 1

	// 计算 Bucket
	bucketKey := getTaskBucketKey(jobID, startID)

	// 使用 ZRangeByScore 按 ID 范围读取
	// Min: startID, Max: +inf
	ids, err := database.RDB.ZRangeByScore(ctx, bucketKey, &redis.ZRangeBy{
		Min:    fmt.Sprintf("%d", startID),
		Max:    "+inf",
		Count:  int64(FetchBatchSize),
		Offset: 0,
	}).Result()

	if err != nil {
		return
	}

	// 如果当前 bucket 读不到数据，有可能是因为刚好跨 bucket 了
	// 尝试读下一个 bucket (简单容错)
	if len(ids) == 0 {
		nextStartID := (startID/TaskBucketSize + 1) * TaskBucketSize
		nextBucketKey := getTaskBucketKey(jobID, nextStartID)
		ids, _ = database.RDB.ZRangeByScore(ctx, nextBucketKey, &redis.ZRangeBy{
			Min:    fmt.Sprintf("%d", nextStartID),
			Max:    "+inf",
			Count:  int64(FetchBatchSize),
			Offset: 0,
		}).Result()
	}

	if len(ids) == 0 {
		return
	}

	// 获取详情 (注意：这里需要兼容 Task Key 的格式，建议 MGET 时尝试两种格式，或者统一格式)
	// 在 addShardedTransferTasks 中我们使用了 tx:task:%d:%d (无 {})
	var keys []string
	for _, id := range ids {
		keys = append(keys, fmt.Sprintf("tx:task:%d:%s", jobID, id))
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

	processed := 0
	var maxID int64 = 0

	for i, item := range jsonList {
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.TransferTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			if task.Status == "PENDING" {
				select {
				case ch <- task:
					processed++
					// 记录读取到的最大 ID
					if task.ID > maxID {
						maxID = task.ID
					}
				default:
					// Buffer full
					goto FINISH
				}
			} else {
				// 就算不是 PENDING，也算处理过了（跳过），需要更新 offset
				// 只是这里我们假设 ZRange 取出的都是有效 ID，如果这里 continue 了，
				// 我们依然需要推进 maxID，否则会死循环卡在非 PENDING 任务上。
				// 由于我们是按 ID 顺序读的，我们应该以 ids[i] 来更新 maxID
				// 简单起见，我们在 FINISH 块用 ids 的最后一个值更新
			}
		}

		// 辅助更新 maxID (以防上面的 task 解析失败或跳过)
		var currentID int64
		fmt.Sscanf(ids[i], "%d", &currentID)
		if currentID > maxID {
			maxID = currentID
		}
	}

FINISH:
	if processed > 0 || maxID > lastTaskID {
		// 如果 Buffer 满了提前退出，maxID 应该是已处理的最后一个。
		// 如果全部处理完，maxID 是 ids 的最后一个。
		if maxID > 0 {
			database.RDB.Set(ctx, offsetKey, maxID, 0)
		}
	}
}
func fillLeagcyTxJobBuffer(jobID int64) {
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
	pipe := database.RDB.Pipeline()

	for _, u := range updates {
		data, err := json.Marshal(u)
		if err != nil {
			continue
		}

		pipe.Set(ctx, fmt.Sprintf("tx:task:%d", u.ID), data, 0)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
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

		oldStatus := existing.Status

		newStatus := u.Status

		// Update fields

		existing.Status = newStatus

		existing.UpdatedAt = time.Now()

		if newStatus == "RUNNING" && existing.StartedAt.IsZero() {

			existing.StartedAt = time.Now()

		}

		if (newStatus == "COMPLETED" || newStatus == "FAILED") && existing.CompletedAt.IsZero() {

			existing.CompletedAt = time.Now()

		}

		// Save to Redis

		data, _ := json.Marshal(existing)

		pipe.Set(ctx, fmt.Sprintf("ff:task:%d", existing.ID), data, 0)

		// Sync to Postgres if status changed or progress update

		if oldStatus != newStatus || u.TotalCount > 0 || u.SuccessCount > 0 || u.FailedCount > 0 {

			jobID := existing.JobID

			// If status changed, use status logic

			// BUT if we have progress counts, use them to update counts absolutely.

			// We prioritize absolute counts if provided (from worker reporting)

			if u.TotalCount > 0 || u.SuccessCount > 0 || u.FailedCount > 0 {

				pending := u.TotalCount - (u.SuccessCount + u.FailedCount)

				if pending < 0 {
					pending = 0
				}

				query := `

	

	

	

								UPDATE ffmpeg_jobs SET 

	

	

	

									total_count = GREATEST(total_count, ?),

	

	

	

									success_count = ?, 

	

	

	

									failed_count = ?,

	

	

	

									pending_count = ?,

	

	

	

									status = ?,

	

	

	

									updated_at = NOW()

	

	

	

								WHERE id = ?

	

	

	

							`

				database.DB.Exec(query, u.TotalCount, u.SuccessCount, u.FailedCount, pending, newStatus, jobID)

			} else if oldStatus != newStatus {

				// Legacy status-based delta update (if no counts provided)

				var pendingDelta, runningDelta, successDelta, failedDelta int

				// Decrement old

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

				// Increment new

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

	

	

	

								UPDATE ffmpeg_jobs SET 

	

	

	

									pending_count = pending_count + ?, 

	

	

	

									running_count = running_count + ?, 

	

	

	

									success_count = success_count + ?, 

	

	

	

									failed_count = failed_count + ?,

	

	

	

									status = ?,

	

	

	

									updated_at = NOW()

	

	

	

								WHERE id = ?

	

	

	

							`

				database.DB.Exec(query, pendingDelta, runningDelta, successDelta, failedDelta, newStatus, jobID)

			}

		}

	}

	_, err = pipe.Exec(ctx)

	if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})

}
