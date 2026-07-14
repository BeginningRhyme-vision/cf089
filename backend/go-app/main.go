package main

import (
	"log"

	"unbound-future-backend/config"
	"unbound-future-backend/database"
	"unbound-future-backend/handlers"
	"unbound-future-backend/routes"
)

func main() {
	log.Println("=== Starting Unbound Future Admin Backend ===")

	log.Println("Loading configuration...")
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Println("Configuration loaded successfully")

	log.Println("Initializing PostgreSQL connection...")
	if err := database.InitPostgres(cfg); err != nil {
		log.Fatalf("Failed to init postgres: %v", err)
	}

	log.Println("Initializing Redis connection...")
	if err := database.InitRedis(cfg); err != nil {
		log.Fatalf("Failed to init redis: %v", err)
	}
	log.Println("Redis connected successfully")

	log.Println("Starting background services...")
	// Start background task buffer service
	handlers.StartBufferService()
	log.Println("  - Task buffer service started")

	// Start periodic redis cleanup
	handlers.StartPeriodicCleanup()
	log.Println("  - Periodic cleanup service started")

	// Start transfer job completion monitor.
	handlers.StartTransferJobMonitor()
	log.Println("  - Transfer job monitor started")

	handlers.StartTransferCompletionReconciler()
	log.Println("  - Transfer completion reconciler started")

	log.Println("Setting up routes...")
	r := routes.SetupRouter()
	log.Println("Routes configured")

	log.Println("=== Server starting on :8080 ===")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
