package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lancedb/lancedb-go/pkg/contracts"
	lancedb "github.com/lancedb/lancedb-go/pkg/lancedb"
	"unbound-future-backend/database"
	"unbound-future-backend/models"
)

// --- Transfer Jobs ---

func CreateTransferJob(c *gin.Context) {
	var job models.TransferJob
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job.Status = models.StatusPending
	// If metadata provided inside, GORM handles it, or we assume ID provided.

	if err := database.DB.Create(&job).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, job)
}

func ListTransferJobs(c *gin.Context) {
	var jobs []models.TransferJob
    // Pagination params
    page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
    limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
    offset := (page - 1) * limit

	if err := database.DB.Preload("Metadata").Offset(offset).Limit(limit).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

// --- Youtube Jobs ---

type CreateYoutubeJobRequest struct {
    R2Prefix string   `json:"r2_prefix"`
    Tasks    []string `json:"tasks"` // List of URLs
}

func CreateYoutubeJob(c *gin.Context) {
    var req CreateYoutubeJobRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }
    
    // 1. Create Job in PG
    job := models.YoutubeJob{
        R2Prefix: req.R2Prefix,
        Status: models.StatusPending,
        TotalCount: len(req.Tasks),
        PendingCount: len(req.Tasks),
    }
    
    if err := database.DB.Create(&job).Error; err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job in PG: " + err.Error()})
        return
    }
    
    // 2. Create Tasks in LanceDB
    // Map URLs to Task structs
    var tasks []models.YoutubeTask
    now := time.Now()
    for i, url := range req.Tasks {
        tasks = append(tasks, models.YoutubeTask{
            ID: int64(job.ID)*1000000 + int64(i), // Simple ID generation
            JobID: int64(job.ID),
            URL: url,
            Status: "PENDING",
            CreatedAt: now,
            UpdatedAt: now,
        })
    }
    
    // Insert into LanceDB
    if len(tasks) > 0 {
         db := database.LanceDB
         ctx := context.Background()
         
         rec, err := models.ToArrowRecord(tasks)
         if err != nil {
             c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create arrow record: " + err.Error()})
             return
         }
         defer rec.Release()
         
         // Check if table exists
         names, err := db.TableNames(ctx)
         found := false
         if err == nil {
             for _, n := range names {
                 if n == "youtube_tasks" { // Constant name
                     found = true
                     break
                 }
             }
         }
         
         var tbl contracts.ITable
         if !found {
             // Create
             sch, err := lancedb.NewSchema(models.TaskArrowSchema)
             if err != nil {
                 c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create schema: " + err.Error()})
                 return
             }
             
             tbl, err = db.CreateTable(ctx, "youtube_tasks", sch)
             if err != nil {
                 c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create LanceDB table: " + err.Error()})
                 return
             }
             if err := tbl.Add(ctx, rec, nil); err != nil {
                  c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
                  return
             }
         } else {
             tbl, err = db.OpenTable(ctx, "youtube_tasks")
             if err != nil {
                  c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open LanceDB table: " + err.Error()})
                  return
             }
             if err := tbl.Add(ctx, rec, nil); err != nil {
                  c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
                  return
             }
         }
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
