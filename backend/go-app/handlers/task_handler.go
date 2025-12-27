package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	jobBuffers   = make(map[int64]chan models.YoutubeTask)
	txJobBuffers = make(map[int64]chan models.TransferTask)
	ffmpegJobBuffers = make(map[int64]chan models.FfmpegTask)
	bufferMutex  sync.RWMutex
	fillingMap   sync.Map // prevent concurrent fills for same job

	// Stats Buffer for Postgres Sync
	statsBuffer = make(map[int64]*JobDelta)
	statsMutex  sync.Mutex
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
)

// StartBufferService initializes the background pre-fetching service
func StartBufferService() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			checkAndRefillBuffers()
			checkAndRefillTxBuffers()
			checkAndRefillFfmpegBuffers()
			flushStats()
		}
	}()
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
			delta.Running, // For PENDING->RUNNING check
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

	for _, job := range jobs {
		ensureBuffer(int64(job.ID))

		bufferMutex.RLock()
		ch, exists := jobBuffers[int64(job.ID)]
		bufferMutex.RUnlock()

		if exists && len(ch) < BufferLowWater {
			triggerRefill(int64(job.ID))
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
	for _, item := range jsonList {
		if item == nil {
			continue
		}
		str, ok := item.(string)
		if !ok {
			continue
		}

		var task models.YoutubeTask
		if err := json.Unmarshal([]byte(str), &task); err == nil {
			// Non-blocking send or timeout?
			// Since we check len < LowWater, there should be space.
			// But to be safe, use select
			select {
			case ch <- task:
				count++
			default:
				// Buffer full, stop filling
				goto FINISH
			}
		}
	}

FINISH:
	// Update offset
	if count > 0 {
		newOffset := offset + int64(count)
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
		// Metadata stage: Round-robin across active job buffers
		// We need to iterate over map.
		bufferMutex.RLock()
		// Get keys
		var jobIDs []int64
		for jid := range jobBuffers {
			jobIDs = append(jobIDs, jid)
		}
		bufferMutex.RUnlock()

		if len(jobIDs) == 0 {
			c.JSON(http.StatusOK, []models.YoutubeTask{})
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

	for _, job := range jobs {
		ensureTxBuffer(int64(job.JobID))

		bufferMutex.RLock()
		ch, exists := txJobBuffers[int64(job.JobID)]
		bufferMutex.RUnlock()

		if exists && len(ch) < BufferLowWater {
			triggerTxRefill(int64(job.JobID))
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
		if item == nil { continue }
		str, ok := item.(string)
		if !ok { continue }

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
	if req.Limit <= 0 { req.Limit = 10 }

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
		if len(tasks) >= req.Limit { break }

		bufferMutex.RLock()
		ch, ok := txJobBuffers[jid]
		bufferMutex.RUnlock()
		if !ok { continue }

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
		if err != nil { continue }
		
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

				Score:  float64(task.ID),

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

	

		for _, job := range jobs {

			ensureFfmpegBuffer(int64(job.ID))

	

			bufferMutex.RLock()

			ch, exists := ffmpegJobBuffers[int64(job.ID)]

			bufferMutex.RUnlock()

	

			if exists && len(ch) < BufferLowWater {

				triggerFfmpegRefill(int64(job.ID))

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

		for _, item := range jsonList {

			if item == nil {

				continue

			}

			str, ok := item.(string)

			if !ok {

				continue

			}

	

			var task models.FfmpegTask

			if err := json.Unmarshal([]byte(str), &task); err == nil {

				select {

				case ch <- task:

					count++

				default:

					goto FINISH

				}

			}

		}

	

	FINISH:

		if count > 0 {

			newOffset := offset + int64(count)

			database.RDB.Set(ctx, offsetKey, newOffset, 0)

		}

	}

	

	func AcquireFfmpegTasks(c *gin.Context) {

		type AcquireRequest struct {

			WorkerID string `json:"worker_id"`

			Limit    int    `json:"limit"`

		}

		var req AcquireRequest

		if err := c.ShouldBindJSON(&req); err != nil {

			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

			return

		}

		if req.Limit <= 0 { req.Limit = 1 }

	

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

			if len(tasks) >= req.Limit { break }

	

			bufferMutex.RLock()

			ch, ok := ffmpegJobBuffers[jid]

			bufferMutex.RUnlock()

			if !ok { continue }

	

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

		pipe := database.RDB.Pipeline()

	

		for _, u := range updates {

			u.UpdatedAt = time.Now()

			data, err := json.Marshal(u)

			if err != nil { continue }

			

			pipe.Set(ctx, fmt.Sprintf("ff:task:%d", u.ID), data, 0)

		}

	

		_, err := pipe.Exec(ctx)

		if err != nil {

			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

			return

		}

		c.JSON(http.StatusOK, gin.H{"status": "updated"})

	}
