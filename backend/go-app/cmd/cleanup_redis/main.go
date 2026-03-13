package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"unbound-future-backend/config"
	"unbound-future-backend/database"
	"unbound-future-backend/models"
)

func main() {
	// 1. Load Config
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Connect to DBs
	if err := database.InitPostgres(cfg); err != nil {
		log.Fatalf("Failed to init postgres: %v", err)
	}
	if err := database.InitRedis(cfg); err != nil {
		log.Fatalf("Failed to init redis: %v", err)
	}

	ctx := context.Background()

	// 3. Get all valid Youtube Job IDs from Postgres
	log.Println("Fetching valid Youtube Jobs from Postgres...")
	var jobs []models.YoutubeJob
	// Only need IDs
	if err := database.DB.Select("id").Find(&jobs).Error; err != nil {
		log.Fatalf("Failed to fetch jobs: %v", err)
	}

	validJobIDs := make(map[uint]bool)
	for _, j := range jobs {
		validJobIDs[j.ID] = true
	}
	log.Printf("Found %d valid jobs in Postgres", len(validJobIDs))

	// 4. Scan Redis for Job Keys
	log.Println("Scanning Redis for orphan jobs...")
	// Note: We scan for keys like "job:{id}:tasks"
	iter := database.RDB.Scan(ctx, 0, "job:*:tasks", 0).Iterator()
	
	orphanCount := 0
	for iter.Next(ctx) {
		key := iter.Val()
		// Key format: job:{id}:tasks
		parts := strings.Split(key, ":")
		if len(parts) != 3 {
			continue
		}
		
		var jobID uint
		_, err := fmt.Sscanf(parts[1], "%d", &jobID)
		if err != nil {
			log.Printf("Skipping invalid key format: %s", key)
			continue
		}
		
		if !validJobIDs[jobID] {
			orphanCount++
			log.Printf("Found orphan job key: %s (ID: %d)", key, jobID)
            
            // Cleanup Logic
            deleteJob(ctx, jobID)
		}
	}
	
	if err := iter.Err(); err != nil {
		log.Fatalf("Error iterating redis keys: %v", err)
	}
	
	log.Printf("Cleanup complete. Removed %d orphan jobs.", orphanCount)
}

func deleteJob(ctx context.Context, jobID uint) {
    jobKey := fmt.Sprintf("job:%d:tasks", jobID)
    
    batchSize := int64(10000)
    for {
        // Get batch of task IDs
        ids, err := database.RDB.ZRange(ctx, jobKey, 0, batchSize-1).Result()
        if err != nil || len(ids) == 0 {
            break
        }
        
        // Delete task keys
        pipe := database.RDB.Pipeline()
        for _, id := range ids {
            pipe.Del(ctx, fmt.Sprintf("task:%s", id))
        }
        pipe.Exec(ctx)
        
        // Remove from ZSet to advance
        database.RDB.ZRemRangeByRank(ctx, jobKey, 0, batchSize-1)
    }
    
    // Finally delete job keys
    database.RDB.Del(ctx, jobKey)
    database.RDB.Del(ctx, fmt.Sprintf("job:%d:lock", jobID))
    database.RDB.Del(ctx, fmt.Sprintf("job:%d:offset", jobID))
}
