package main

import (
	"log"

	"unbound-future-backend/config"
	"unbound-future-backend/database"
	"unbound-future-backend/routes"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := database.InitPostgres(cfg); err != nil {
		log.Fatalf("Failed to init postgres: %v", err)
	}

	if err := database.InitLanceDB(cfg); err != nil {
		log.Fatalf("Failed to init lancedb: %v", err)
	}

	r := routes.SetupRouter()

	log.Println("Starting server on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
