package routes

import (
	"github.com/gin-gonic/gin"
	"unbound-future-backend/handlers"
)

func SetupRouter() *gin.Engine {
	r := gin.Default()

	api := r.Group("/api")
	{
        // Transfer Jobs
		jobs := api.Group("/jobs")
		{
			jobs.POST("/", handlers.CreateTransferJob)
			jobs.GET("/", handlers.ListTransferJobs)
		}

        // Youtube Jobs
        ytJobs := api.Group("/youtube-jobs")
        {
            ytJobs.POST("/", handlers.CreateYoutubeJob)
            ytJobs.GET("/", handlers.ListYoutubeJobs)
        }
        
        // Metadata
        meta := api.Group("/metadata")
        {
            meta.POST("/", handlers.CreateMetadata)
            meta.GET("/", handlers.ListMetadata)
            meta.GET("/:id", handlers.GetMetadata)
            meta.PUT("/:id", handlers.UpdateMetadata)
            meta.DELETE("/:id", handlers.DeleteMetadata)
        }
        
        // Tasks (New Batch Interface)
        tasks := api.Group("/tasks")
        {
            tasks.POST("/batch", handlers.BatchInsert) // Or just POST / for insert? Plan said "batch_insert" interface.
            // Using sub-paths to distinguish operations clearly as they are distinct logical "batch" ops
            tasks.POST("/insert", handlers.BatchInsert)
            tasks.POST("/update", handlers.BatchUpdate)
            tasks.POST("/fetch", handlers.BatchFetch)
            tasks.POST("/delete", handlers.BatchDelete)
        }
	}

	return r
}