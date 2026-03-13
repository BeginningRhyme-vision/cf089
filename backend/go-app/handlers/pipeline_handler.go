package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
)

type CreatePipelineRequest struct {
	Name        string   `json:"name"`
	YoutubeURLs []string `json:"youtube_urls"`
	MetadataID  uint     `json:"metadata_id"` // Target storage for Transfer/FFmpeg
}

func CreatePipelineJob(c *gin.Context) {
	var req CreatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.YoutubeURLs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No Youtube URLs provided"})
		return
	}

	// 1. Create Pipeline Job Record (to get ID)

pipeline := models.PipelineJob{
		Name:        req.Name,
		Status:      models.StatusRunning,
		YoutubeURLs: strings.Join(req.YoutubeURLs, "\n"),
	}

	if err := database.DB.Create(&pipeline).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create pipeline: " + err.Error()})
		return
	}

	// 2. Define Paths
	// Youtube downloads to: pipeline/<id>/raw/
	// Transfer moves to: pipeline/<id>/process/ (in Metadata bucket)
	// FFmpeg scans: pipeline/<id>/process/
	
	rawPath := fmt.Sprintf("pipeline/%d/raw/", pipeline.ID)
	processPath := fmt.Sprintf("pipeline/%d/process/", pipeline.ID)

	// 3. Create Youtube Job

ytJob := models.YoutubeJob{
		R2Prefix:     rawPath,
		Status:       models.StatusPending,
		TotalCount:   len(req.YoutubeURLs),
		PendingCount: len(req.YoutubeURLs),
	}
	if err := database.DB.Create(&ytJob).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create youtube job: " + err.Error()})
		return
	}
	// Add tasks
	go func(jid int64, tasks []string) {
		AddTasksToJob(jid, tasks)
	}(int64(ytJob.ID), req.YoutubeURLs)

	// 4. Create Transfer Job
	// We need to fetch the metadata to ensure it exists? No, DB constraints handle it usually, 
	// but we need it to know if we should error early. 
	// Assuming MetadataID is valid.

	txJob := models.TransferJob{
		MetadataID:       req.MetadataID,
		SrcDir:           rawPath,
		DstDir:           processPath,
		DeleteSource:     true, // Move files
		IsIncremental:    true,
		PeriodicInterval: 60, // Check every minute
		Status:           models.StatusPending,
	}
	if err := database.DB.Create(&txJob).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create transfer job: " + err.Error()})
		return
	}

	// 5. Create Ffmpeg Job
	ffJob := models.FfmpegJob{
		MetadataID:       req.MetadataID,
		S3Prefix:         processPath,
		IsIncremental:    true,
		PeriodicInterval: 60,
		Status:           models.StatusPending,
	}
	if err := database.DB.Create(&ffJob).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create ffmpeg job: " + err.Error()})
		return
	}

	// 6. Link Jobs to Pipeline

pipeline.YoutubeJobID = ytJob.ID
pipeline.TransferJobID = txJob.JobID
pipeline.FfmpegJobID = ffJob.ID
	
	if err := database.DB.Save(&pipeline).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update pipeline: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, pipeline)
}

func ListPipelineJobs(c *gin.Context) {
	var jobs []models.PipelineJob
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	if err := database.DB.Order("created_at desc").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

func GetPipelineJob(c *gin.Context) {

	id := c.Param("id")

	var job models.PipelineJob

	if err := database.DB.First(&job, id).Error; err != nil {

		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})

		return

	}

	

	c.JSON(http.StatusOK, job)

}



func RetryPipelineJob(c *gin.Context) {

	id := c.Param("id")

	var job models.PipelineJob

	if err := database.DB.First(&job, id).Error; err != nil {

		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})

		return

	}



	// 1. Retry Youtube Job

	if job.YoutubeJobID != 0 {

		var ytJob models.YoutubeJob

		if err := database.DB.First(&ytJob, job.YoutubeJobID).Error; err == nil {

			go RetryYoutubeTasksLogic(int(job.YoutubeJobID), ytJob.Status)

		}

	}



	// 2. Retry Transfer Job

		if job.TransferJobID != 0 {

			var txJob models.TransferJob

			if err := database.DB.First(&txJob, job.TransferJobID).Error; err == nil {

				go RetryTransferTasksLogic(int(job.TransferJobID), txJob.Status)

			}

		}

	

		// NOTE: Ffmpeg retry is handled by the ffmpeg worker service itself.

		// We do not trigger it from here.

	

		// Reset Pipeline Status if it was Failed/Completed?

		// The pipeline status logic isn't fully defined yet (monitoring loop needed),

		// but if we are retrying tasks, we should probably ensure the pipeline is considered "RUNNING".

		if job.Status == models.StatusFailed || job.Status == models.StatusCompleted {

			job.Status = models.StatusRunning

			database.DB.Save(&job)

		}

	



	c.JSON(http.StatusOK, gin.H{"message": "Pipeline retry initiated"})

}


