package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/lancedb/lancedb-go/pkg/contracts"
	lancedb "github.com/lancedb/lancedb-go/pkg/lancedb"
	"unbound-future-backend/database"
	"unbound-future-backend/models"
)

const TableName = "youtube_tasks"

func getTable(ctx context.Context) (contracts.ITable, error) {
	db := database.LanceDB
    names, err := db.TableNames(ctx)
    if err != nil {
        return nil, err
    }
    
    found := false
    for _, n := range names {
        if n == TableName {
            found = true
            break
        }
    }
    
    if found {
        return db.OpenTable(ctx, TableName)
    }
    return nil, fmt.Errorf("table not found")
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

	db := database.LanceDB
    ctx := context.Background()

    rec, err := models.ToArrowRecord(tasks)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create arrow record: " + err.Error()})
        return
    }
    defer rec.Release()

    tbl, err := getTable(ctx)
    if err != nil {
        // Table doesn't exist, create it
        sch, err := lancedb.NewSchema(models.TaskArrowSchema)
        if err != nil {
             c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to convert schema: " + err.Error()})
             return
        }
        
        _, err = db.CreateTable(ctx, TableName, sch)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create table: " + err.Error()})
            return
        }
        // Re-open
        tbl, err = getTable(ctx)
        if err != nil {
             c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open created table: " + err.Error()})
             return
        }
        
        if err := tbl.Add(ctx, rec, nil); err != nil {
             c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
             return
        }
    } else {
        if err := tbl.Add(ctx, rec, nil); err != nil {
             c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add tasks: " + err.Error()})
             return
        }
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
    tbl, err := getTable(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Table not found"})
        return
    }
    
    var ids []string
    for _, u := range updates {
        ids = append(ids, fmt.Sprintf("%d", u.ID))
    }
    
    filter := fmt.Sprintf("id IN (%s)", strings.Join(ids, ","))
    
    if err := tbl.Delete(ctx, filter); err != nil {
         c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete old versions: " + err.Error()})
         return
    }
    
    rec, err := models.ToArrowRecord(updates)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create arrow record: " + err.Error()})
        return
    }
    defer rec.Release()
    
    if err := tbl.Add(ctx, rec, nil); err != nil {
         c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add updated tasks: " + err.Error()})
         return
    }
    
	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func BatchFetch(c *gin.Context) {
    type FetchRequest struct {
        JobID  int64  `json:"job_id"`
        Limit  int    `json:"limit"`
        Offset int    `json:"offset"`
    }
    
    var req FetchRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }
    
    ctx := context.Background()
    tbl, err := getTable(ctx)
    if err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "Table not found"})
        return
    }
    
    filter := fmt.Sprintf("job_id = %d", req.JobID)
    
    results, err := tbl.SelectWithFilter(ctx, filter)
    if err != nil {
         c.JSON(http.StatusInternalServerError, gin.H{"error": "Query failed: " + err.Error()})
         return
    }
    
    start := req.Offset
    end := start + req.Limit
    if start > len(results) {
        start = len(results)
    }
    if end > len(results) {
        end = len(results)
    }
    
    c.JSON(http.StatusOK, gin.H{"tasks": results[start:end], "total": len(results)})
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
    tbl, err := getTable(ctx)
    if err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "Table not found"})
        return
    }
    
    var ids []string
    for _, id := range req.IDs {
        ids = append(ids, fmt.Sprintf("%d", id))
    }
    filter := fmt.Sprintf("id IN (%s)", strings.Join(ids, ","))
    
    if err := tbl.Delete(ctx, filter); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Delete failed: " + err.Error()})
        return
    }
    
    c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
