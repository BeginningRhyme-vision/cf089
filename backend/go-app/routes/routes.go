package routes

import (
	"github.com/gin-gonic/gin"
	"unbound-future-backend/handlers"
)

func SetupRouter() *gin.Engine {
	r := gin.Default()

	api := r.Group("/api")
	{
		// Auth
		auth := api.Group("/auth")
		{
			auth.GET("/feishu/login_url", handlers.GetFeishuLoginURL)
			auth.POST("/feishu/callback", handlers.FeishuCallback)
		}

        // Transfer Jobs
		jobs := api.Group("/jobs")
		{
			jobs.POST("/", handlers.CreateTransferJob)
			jobs.GET("/", handlers.ListTransferJobs)
			jobs.GET("/pending", handlers.ListPendingTransferJobs)
			jobs.GET("/:id", handlers.GetTransferJob)
			jobs.POST("/:id/start", handlers.StartTransferJob)
			jobs.POST("/:id/stop", handlers.StopTransferJob)
			jobs.PATCH("/:id/status", handlers.UpdateTransferJobStatus)
			jobs.POST("/:id/tasks", handlers.AddTasksToTransferJob)
			jobs.DELETE("/:id", handlers.DeleteTransferJob)
		}

        // Youtube Jobs
        ytJobs := api.Group("/youtube-jobs")
        {
            ytJobs.POST("/", handlers.CreateYoutubeJob)
            ytJobs.GET("/", handlers.ListYoutubeJobs)
            ytJobs.GET("/:id", handlers.GetYoutubeJob)
            ytJobs.POST("/:id/tasks", handlers.AddTasksToYoutubeJob)
            ytJobs.DELETE("/pending", handlers.DeletePendingYoutubeJobs)
            ytJobs.DELETE("/:id", handlers.DeleteYoutubeJob)
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
            tasks.POST("/insert", handlers.BatchInsert)
            tasks.POST("/update", handlers.BatchUpdate)
            tasks.POST("/fetch", handlers.BatchFetch)
            tasks.POST("/acquire", handlers.AcquireTasks)
            tasks.POST("/delete", handlers.BatchDelete)
        }

        // Transfer Tasks
        txTasks := api.Group("/transfer-tasks")
        {
            txTasks.POST("/acquire", handlers.AcquireTransferTasks)
            txTasks.POST("/update", handlers.BatchUpdateTransfer)
        }
	}

	return r
}