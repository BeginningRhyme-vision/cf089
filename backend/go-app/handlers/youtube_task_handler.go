package handlers

import (
	"net/http"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// UpdateYoutubeTaskRecord 更新或创建 YouTube 任务记录
// POST /api/youtube-tasks/update
// 使用 job_id + id 作为唯一标识，如果存在则更新，不存在则创建
func UpdateYoutubeTaskRecord(c *gin.Context) {
	var task models.YoutubeTaskRecord
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 验证必需字段
	if task.JobID == 0 || task.ID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and id are required"})
		return
	}

	// 使用 job_id + id 查找现有记录
	var existing models.YoutubeTaskRecord
	err := database.DB.Where("job_id = ? AND id = ?", task.JobID, task.ID).First(&existing).Error

	if err == gorm.ErrRecordNotFound {
		// 记录不存在，创建新记录
		if err := database.DB.Create(&task).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create task record: " + err.Error()})
			return
		}
		c.JSON(http.StatusCreated, task)
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query task record: " + err.Error()})
		return
	} else {
		// 记录存在，更新现有记录
		// 更新所有字段（包括空字符串和零值，因为这些都是有效的状态）
		updates := map[string]interface{}{
			"status":       task.Status,
			"worker_id":    task.WorkerID,
			"title":        task.Title,
			"video_id":     task.VideoID,
			"audio_url":    task.AudioURL,
			"audio_size":   task.AudioSize,
			"video_url":    task.VideoURL,
			"video_size":   task.VideoSize,
			"error_message": task.ErrorMessage,
		}

		if err := database.DB.Model(&existing).Updates(updates).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update task record: " + err.Error()})
			return
		}

		// 重新加载记录以返回最新数据
		database.DB.First(&existing, existing.ID)
		c.JSON(http.StatusOK, existing)
	}
}
