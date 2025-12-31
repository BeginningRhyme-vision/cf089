package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/metrics"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
)

// --- Transfer Jobs ---

type CreateTransferJobRequest struct {
	models.TransferJob
	Tasks []string `json:"tasks"`
}

func CreateTransferJob(c *gin.Context) {
	var req CreateTransferJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job := req.TransferJob
	job.Status = models.StatusPending

	// If Incremental is selected but no interval is provided, default to 60s to ensure it acts as a periodic monitor
	if job.IsIncremental && job.PeriodicInterval <= 0 {
		job.PeriodicInterval = 600
	}

	// Initial counts
	job.TotalCount = len(req.Tasks)
	job.PendingCount = len(req.Tasks)

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	metrics.JobCreatedTotal.WithLabelValues("transfer").Inc()
	metrics.ActiveJobsGauge.WithLabelValues("transfer").Inc()

	// Async add tasks
	if len(req.Tasks) > 0 {
		go func(jobID uint, tasks []string) {
			var inputs []TransferTaskInput
			for _, t := range tasks {
				inputs = append(inputs, TransferTaskInput{Src: t, Size: 0})
			}
			_, err := AddTransferTasksToJob(int64(jobID), inputs)
			if err != nil {
				fmt.Printf("Error adding tasks for transfer job %d: %v\n", jobID, err)
			}
		}(job.JobID, req.Tasks)
	}

	c.JSON(http.StatusCreated, job)
}

func ListTransferJobs(c *gin.Context) {
	var jobs []models.TransferJob
	// Pagination params
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Preload("Metadata").Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

func GetTransferJob(c *gin.Context) {
	id := c.Param("id")
	var job models.TransferJob
	if err := database.DB.Preload("Metadata").First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

func StartTransferJob(c *gin.Context) {
	id := c.Param("id")
	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	updates := map[string]interface{}{
		"status":           models.StatusPending,
		"start_time":       nil,
		"end_time":         nil,
		"duration_seconds": 0,
		"result_message":   "",
	}

	if err := database.DB.Model(&job).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, job)
}

func StopTransferJob(c *gin.Context) {
	id := c.Param("id")
	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	job.Status = models.StatusStopped
	if err := database.DB.Save(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, job)
}

func ListPendingTransferJobs(c *gin.Context) {
	var jobs []models.TransferJob
	if err := database.DB.Preload("Metadata").
		Where("status = ?", models.StatusPending).
		Or("status = ? AND periodic_interval > 0 AND (last_scan_time IS NULL OR last_scan_time < NOW() - make_interval(secs => periodic_interval))", models.StatusRunning).
		Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

// --- Reusable Retry Logic ---

func RetryTransferTasksLogic(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()
	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)

	batchSize := 1000
	var cursor int64 = 0
	resetCount := 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, cursor, cursor+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, tid := range ids {
			keys = append(keys, fmt.Sprintf("tx:task:%s", tid))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			cursor += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		hasUpdates := false

		for i, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.TransferTask
			if err := json.Unmarshal([]byte(str), &task); err == nil {
				if task.Status == "FAILED" {
					task.Status = "PENDING"
					task.UpdatedAt = time.Now()
					task.ErrorMessage = ""

					data, _ := json.Marshal(task)
					pipe.Set(ctx, keys[i], data, 0)
					hasUpdates = true
					resetCount++
				}
			}
		}

		if hasUpdates {
			pipe.Exec(ctx)
		}

		cursor += int64(batchSize)
		if len(ids) < batchSize {
			break
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE transfer_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE job_id = ?", resetCount, resetCount, jobID)

		database.RDB.Set(ctx, fmt.Sprintf("tx:job:%d:offset", jobID), 0, 0)

		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.TransferJob{JobID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}

func RetryYoutubeTasksLogic(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)

	batchSize := 1000
	var cursor int64 = 0
	resetCount := 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, cursor, cursor+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, tid := range ids {
			keys = append(keys, fmt.Sprintf("task:%s", tid))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			cursor += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		hasUpdates := false

		for i, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.YoutubeTask
			if err := json.Unmarshal([]byte(str), &task); err == nil {
				// Retry if FAILED and (IsDownloadFail is true OR it's a bot detection error)
				isBotError := strings.Contains(task.ErrorMessage, "Sign in to confirm you’re not a bot")
				if task.Status == "FAILED" && (task.IsDownloadFail || isBotError) {
					task.Status = "PENDING"
					task.IsDownloadFail = false
					task.ErrorMessage = ""
					task.UpdatedAt = time.Now()

					data, _ := json.Marshal(task)
					pipe.Set(ctx, keys[i], data, 0)
					// Push back to download queue
					pipe.RPush(ctx, "queue:youtube:download_ready", task.ID)

					hasUpdates = true
					resetCount++
				}
			}
		}

		if hasUpdates {
			pipe.Exec(ctx)
		}

		cursor += int64(batchSize)
		if len(ids) < batchSize {
			break
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE youtube_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE id = ?", resetCount, resetCount, jobID)

		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.YoutubeJob{ID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}

func RetryFfmpegTasksLogic(jobID int, initialStatus models.JobStatus) {
	ctx := context.Background()
	jobKey := fmt.Sprintf("ff:job:%d:tasks", jobID)

	batchSize := 1000
	var cursor int64 = 0
	resetCount := 0

	for {
		ids, err := database.RDB.ZRange(ctx, jobKey, cursor, cursor+int64(batchSize)-1).Result()
		if err != nil || len(ids) == 0 {
			break
		}

		var keys []string
		for _, tid := range ids {
			keys = append(keys, fmt.Sprintf("ff:task:%s", tid))
		}

		results, err := database.RDB.MGet(ctx, keys...).Result()
		if err != nil {
			cursor += int64(batchSize)
			continue
		}

		pipe := database.RDB.Pipeline()
		hasUpdates := false

		for i, val := range results {
			if val == nil {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}

			var task models.FfmpegTask
			if err := json.Unmarshal([]byte(str), &task); err == nil {
				if task.Status == "FAILED" {
					task.Status = "PENDING"
					task.UpdatedAt = time.Now()
					task.ErrorMessage = ""

					data, _ := json.Marshal(task)
					pipe.Set(ctx, keys[i], data, 0)
					hasUpdates = true
					resetCount++
				}
			}
		}

		if hasUpdates {
			pipe.Exec(ctx)
		}

		cursor += int64(batchSize)
		if len(ids) < batchSize {
			break
		}
	}

	if resetCount > 0 {
		database.DB.Exec("UPDATE ffmpeg_jobs SET failed_count = failed_count - ?, pending_count = pending_count + ? WHERE id = ?", resetCount, resetCount, jobID)
		
		// Reset offset to rescan if needed? Usually for incremental we want to re-check.
		// For Ffmpeg, the scanner finds tasks. If we reset tasks to PENDING, they are in Redis but not in any queue unless we put them there?
		// Ffmpeg architecture: Scanner -> Redis (PENDING) -> Worker acquires.
		// If we just change status to PENDING in Redis, the worker won't find them unless they are in a list/queue or the worker scans keys.
		// In Youtube logic, we pushed to "queue:youtube:download_ready".
		// In Transfer logic, we didn't push anywhere. Wait, Transfer worker calls `AcquireTransferTasks` which likely does ZRANGE or similar on `tx:job:...`.
		// Let's check `AcquireTransferTasks` implementation later. Assuming standard pattern.
		// For Ffmpeg, we probably need to ensure they are pickable.
		
		if initialStatus == models.StatusCompleted || initialStatus == models.StatusFailed {
			database.DB.Model(&models.FfmpegJob{ID: uint(jobID)}).Update("status", models.StatusPending)
		}
	}
}

func RetryFailedTransferTasks(c *gin.Context) {
	id := c.Param("id")
	jobID, _ := strconv.Atoi(id)

	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	go RetryTransferTasksLogic(jobID, job.Status)

	c.JSON(http.StatusOK, gin.H{"message": "Retry initiated in background"})
}

type UpdateJobStatusRequest struct {
	Status        models.JobStatus `json:"status"`
	LastScanTime  *time.Time       `json:"last_scan_time"`
	ResultMessage string           `json:"result_message"`
	IncSuccess    int              `json:"inc_success"`
	IncFailed     int              `json:"inc_failed"`
	StartTime     *time.Time       `json:"start_time"`
	EndTime       *time.Time       `json:"end_time"`
	TotalCount    *int             `json:"total_count"`
	IncExecution  bool             `json:"inc_execution"`
}

func UpdateTransferJobStatus(c *gin.Context) {
	id := c.Param("id")
	var req UpdateJobStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	updates := make(map[string]interface{})
	if req.Status != "" {
		updates["status"] = req.Status
	}
	if req.LastScanTime != nil {
		updates["last_scan_time"] = req.LastScanTime
	}
	if req.ResultMessage != "" {
		updates["result_message"] = req.ResultMessage
	}
	if req.StartTime != nil {
		updates["start_time"] = req.StartTime
	}
	if req.TotalCount != nil {
		updates["total_count"] = *req.TotalCount
	}
	if req.EndTime != nil {
		updates["end_time"] = req.EndTime
		// Calculate duration
		var startTime time.Time
		if req.StartTime != nil {
			startTime = *req.StartTime
		} else if job.StartTime != nil {
			startTime = *job.StartTime
		}

		if !startTime.IsZero() {
			duration := req.EndTime.Sub(startTime)
			updates["duration_seconds"] = int(duration.Seconds())
		}
	}

	if len(updates) > 0 {
		if err := database.DB.Model(&job).Updates(updates).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	// Atomic counter updates
	if req.IncSuccess > 0 || req.IncFailed > 0 {
		totalDec := req.IncSuccess + req.IncFailed
		err := database.DB.Exec("UPDATE transfer_jobs SET success_count = success_count + ?, failed_count = failed_count + ?, pending_count = GREATEST(0, pending_count - ?) WHERE job_id = ?", req.IncSuccess, req.IncFailed, totalDec, id).Error
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	if req.IncExecution {
		if err := database.DB.Exec("UPDATE transfer_jobs SET execution_count = execution_count + 1 WHERE job_id = ?", id).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	// Reload job to return latest state
	database.DB.First(&job, id)
	c.JSON(http.StatusOK, job)
}

func cleanupTransferJobRedis(ctx context.Context, jobID uint) {
	// 1. Get all task IDs
	jobKey := fmt.Sprintf("tx:job:%d:tasks", jobID)

	taskIDs, err := database.RDB.ZRange(ctx, jobKey, 0, -1).Result()
	if err == nil && len(taskIDs) > 0 {
		pipe := database.RDB.Pipeline()
		for _, tid := range taskIDs {
			pipe.Del(ctx, fmt.Sprintf("tx:task:%s", tid))
		}
		pipe.Exec(ctx)
	}

	// 2. Delete Job Key, metadata and dedup
	database.RDB.Del(ctx, jobKey)
	database.RDB.Del(ctx, fmt.Sprintf("tx:job:%d:dedup", jobID))
	database.RDB.Del(ctx, fmt.Sprintf("tx:job:%d:lock", jobID))
	database.RDB.Del(ctx, fmt.Sprintf("tx:job:%d:offset", jobID))
}

func DeleteTransferJob(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	if err := database.DB.Delete(&models.TransferJob{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	go cleanupTransferJobRedis(context.Background(), uint(id))

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

type AddTransferTasksRequest struct {
	Tasks []TransferTaskInput `json:"tasks"`
}

func AddTasksToTransferJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Job ID"})
		return
	}

	var req AddTransferTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Verify Job
	var job models.TransferJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// Add tasks with dedup
	count, err := AddTransferTasksToJob(int64(id), req.Tasks)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
		return
	}

	// Update Job stats
	if count > 0 {
		database.DB.Exec("UPDATE transfer_jobs SET total_count = total_count + ?, pending_count = pending_count + ? WHERE job_id = ?", count, count, id)
	}

	c.JSON(http.StatusOK, gin.H{"added": count, "job_id": id})
}

// --- Youtube Jobs ---

type CreateYoutubeJobRequest struct {
	R2Prefix string   `json:"r2_prefix" form:"r2_prefix"`
	FileUrl  string   `json:"file_url" form:"file_url"`
	Tasks    []string `json:"tasks" form:"-"` // List of URLs
}

func CreateYoutubeJob(c *gin.Context) {
	var req CreateYoutubeJobRequest

	contentType := c.GetHeader("Content-Type")

	if strings.Contains(contentType, "application/json") {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else if strings.Contains(contentType, "multipart/form-data") {
		req.R2Prefix = c.PostForm("r2_prefix")
		req.FileUrl = c.PostForm("file_url")

		// Handle file upload
		file, err := c.FormFile("file")
		if err == nil {
			f, err := file.Open()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open file"})
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line != "" {
					req.Tasks = append(req.Tasks, line)
				}
			}
			if err := scanner.Err(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file: " + err.Error()})
				return
			}
		}

		// Handle manual tasks from form field
		manualTasks := c.PostForm("tasks")
		if manualTasks != "" {
			lines := strings.Split(manualTasks, "\n")
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if trimmed != "" {
					req.Tasks = append(req.Tasks, trimmed)
				}
			}
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported Content-Type"})
		return
	}

	// Handle File URL if provided
	if req.FileUrl != "" {
		resp, err := http.Get(req.FileUrl)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to download file from URL: " + err.Error()})
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to download file from URL, status: %d", resp.StatusCode)})
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				req.Tasks = append(req.Tasks, line)
			}
		}
		if err := scanner.Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read downloaded file: " + err.Error()})
			return
		}
	}

	// 1. Create Job in PG
	job := models.YoutubeJob{
		R2Prefix:     req.R2Prefix,
		Status:       models.StatusPending,
		TotalCount:   len(req.Tasks),
		PendingCount: len(req.Tasks),
	}

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job in PG: " + err.Error()})
		return
	}

	metrics.JobCreatedTotal.WithLabelValues("youtube").Inc()
	metrics.ActiveJobsGauge.WithLabelValues("youtube").Inc()

	// 2. Create Tasks in Redis (Async)
	if len(req.Tasks) > 0 {
		go func(jobID int64, tasks []string) {
			_, err := AddTasksToJob(jobID, tasks)
			if err != nil {
				fmt.Printf("Error adding tasks to Redis for job %d: %v\n", jobID, err)
			}
		}(int64(job.ID), req.Tasks)
	}

	c.JSON(http.StatusCreated, job)
}

func ListYoutubeJobs(c *gin.Context) {
	var jobs []models.YoutubeJob
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

func GetYoutubeJob(c *gin.Context) {
	id := c.Param("id")
	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

func cleanupYoutubeJobRedis(ctx context.Context, jobID uint) {
	// 1. Get all task IDs
	jobKey := fmt.Sprintf("job:%d:tasks", jobID)

	// We might have many tasks, so we should scan or handle in chunks if huge,
	// but ZRange is okay for reasonable sizes.
	taskIDs, err := database.RDB.ZRange(ctx, jobKey, 0, -1).Result()
	if err == nil && len(taskIDs) > 0 {
		pipe := database.RDB.Pipeline()
		for _, tid := range taskIDs {
			pipe.Del(ctx, fmt.Sprintf("task:%s", tid))
		}
		pipe.Exec(ctx)
	}

	// 2. Delete Job Key and metadata
	database.RDB.Del(ctx, jobKey)
	database.RDB.Del(ctx, fmt.Sprintf("job:%d:lock", jobID))
	database.RDB.Del(ctx, fmt.Sprintf("job:%d:offset", jobID))
}

func DeleteYoutubeJob(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	// Delete from PG
	if err := database.DB.Delete(&models.YoutubeJob{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Delete from Redis (Async)
	go cleanupYoutubeJobRedis(context.Background(), uint(id))

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}

type AddTasksRequest struct {
	Tasks []string `json:"tasks"`
}

func AddTasksToYoutubeJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid Job ID"})
		return
	}

	var req AddTasksRequest
	contentType := c.GetHeader("Content-Type")

	if strings.Contains(contentType, "application/json") {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else if strings.Contains(contentType, "multipart/form-data") {
		// Handle file upload
		file, err := c.FormFile("file")
		if err == nil {
			f, err := file.Open()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open file"})
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line != "" {
					req.Tasks = append(req.Tasks, line)
				}
			}
			if err := scanner.Err(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file: " + err.Error()})
				return
			}
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported Content-Type"})
		return
	}

	// 1. Verify Job exists
	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// 2. Add tasks to Redis
	count, err := AddTasksToJob(int64(id), req.Tasks)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
		return
	}

	// 3. Update Job stats in Postgres
	if count > 0 {
		// Use raw SQL to avoid needing GORM imports for expressions
		database.DB.Exec("UPDATE youtube_jobs SET total_count = total_count + ?, pending_count = pending_count + ? WHERE id = ?", count, count, id)
	}

	c.JSON(http.StatusOK, gin.H{"added": count, "job_id": id})
}

func DeletePendingYoutubeJobs(c *gin.Context) {
	var jobs []models.YoutubeJob
	if err := database.DB.Where("status = ?", models.StatusPending).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(jobs) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No pending jobs found"})
		return
	}

	// Delete from PG
	if err := database.DB.Delete(&models.YoutubeJob{}, "status = ?", models.StatusPending).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Delete from Redis (Async)
	go func(jobs []models.YoutubeJob) {
		ctx := context.Background()
		for _, job := range jobs {
			cleanupYoutubeJobRedis(ctx, job.ID)
		}
	}(jobs)

	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Deleted %d pending jobs", len(jobs))})
}

func RetryFailedYoutubeTasks(c *gin.Context) {
	id := c.Param("id")
	jobID, _ := strconv.Atoi(id)

	var job models.YoutubeJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	go RetryYoutubeTasksLogic(jobID, job.Status)

	c.JSON(http.StatusOK, gin.H{"message": "Retry initiated in background"})
}

// --- Ffmpeg Jobs ---

type CreateFfmpegJobRequest struct {
	models.FfmpegJob
}

func CreateFfmpegJob(c *gin.Context) {
	var req CreateFfmpegJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job := req.FfmpegJob
	job.Status = models.StatusPending

	if job.IsIncremental && job.PeriodicInterval <= 0 {
		job.PeriodicInterval = 600
	}

	// Assuming 1 task per job initially (the job itself is the task context)
	job.TotalCount = 0 // Scanner will find tasks
	job.PendingCount = 0

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Preload Metadata to get credentials
	if err := database.DB.Preload("Metadata").First(&job, job.ID).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load metadata: " + err.Error()})
		return
	}

	metrics.JobCreatedTotal.WithLabelValues("ffmpeg").Inc()
	metrics.ActiveJobsGauge.WithLabelValues("ffmpeg").Inc()

	// We don't create an initial task here anymore. The scanner will create tasks.

	c.JSON(http.StatusCreated, job)
}

func ListFfmpegJobs(c *gin.Context) {
	var jobs []models.FfmpegJob
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Preload("Metadata").Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

func ListPendingFfmpegJobs(c *gin.Context) {
	var jobs []models.FfmpegJob
	if err := database.DB.Preload("Metadata").
		Where("status = ?", models.StatusPending).
		Or("status = ? AND periodic_interval > 0 AND (last_scan_time IS NULL OR last_scan_time < NOW() - make_interval(secs => periodic_interval))", models.StatusRunning).
		Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

func GetFfmpegJob(c *gin.Context) {
	id := c.Param("id")
	var job models.FfmpegJob
	if err := database.DB.Preload("Metadata").First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

func UpdateFfmpegJobStatus(c *gin.Context) {
	id := c.Param("id")
	var req UpdateJobStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var job models.FfmpegJob
	if err := database.DB.First(&job, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	updates := make(map[string]interface{})
	if req.Status != "" {
		updates["status"] = req.Status
	}
	if req.LastScanTime != nil {
		updates["last_scan_time"] = req.LastScanTime
	}
	if req.TotalCount != nil {
		updates["total_count"] = *req.TotalCount
	}
	if req.ResultMessage != "" {
		// Store result message somewhere? FfmpegJob doesn't have ResultMessage.
		// Ignoring for now or I should add it.
	}

	// Atomic counter updates if provided
	if req.IncSuccess > 0 || req.IncFailed > 0 {
		totalDec := req.IncSuccess + req.IncFailed
		database.DB.Exec("UPDATE ffmpeg_jobs SET success_count = success_count + ?, failed_count = failed_count + ?, pending_count = GREATEST(0, pending_count - ?) WHERE id = ?", req.IncSuccess, req.IncFailed, totalDec, id)
	}

	if len(updates) > 0 {
		if err := database.DB.Model(&job).Updates(updates).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	database.DB.First(&job, id)
	c.JSON(http.StatusOK, job)
}

func DeleteFfmpegJob(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	if err := database.DB.Delete(&models.FfmpegJob{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Cleanup Redis
	go func(jid uint) {
		ctx := context.Background()
		jobKey := fmt.Sprintf("ff:job:%d:tasks", jid)

		taskIDs, err := database.RDB.ZRange(ctx, jobKey, 0, -1).Result()
		if err == nil && len(taskIDs) > 0 {
			pipe := database.RDB.Pipeline()
			for _, tid := range taskIDs {
				pipe.Del(ctx, fmt.Sprintf("ff:task:%s", tid))
			}
			pipe.Exec(ctx)
		}

		database.RDB.Del(ctx, jobKey)
		database.RDB.Del(ctx, fmt.Sprintf("ff:job:%d:lock", jid))
		database.RDB.Del(ctx, fmt.Sprintf("ff:job:%d:offset", jid))
	}(uint(id))

	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
