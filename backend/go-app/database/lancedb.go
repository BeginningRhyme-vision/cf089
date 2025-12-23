package database

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/lancedb/lancedb-go/pkg/contracts"
	lancedb "github.com/lancedb/lancedb-go/pkg/lancedb"
	"unbound-future-backend/config"
)

var LanceDB contracts.IConnection

func InitLanceDB(cfg *config.Config) error {
    uri := cfg.LanceDB.URI
    if uri == "" {
        // Fallback or error?
        // S3 URI format: s3://bucket/path
        return fmt.Errorf("lancedb uri is required")
    }

    // Set environment variables for object_store as a fallback/primary method
    // This addresses issues where options might be ignored by the underlying library
    if cfg.LanceDB.Endpoint != "" {
        os.Setenv("AWS_ENDPOINT_URL", cfg.LanceDB.Endpoint)
    }
    if cfg.LanceDB.Region != "" {
        os.Setenv("AWS_REGION", cfg.LanceDB.Region)
    }
    if cfg.LanceDB.AccessKey != "" {
        os.Setenv("AWS_ACCESS_KEY_ID", cfg.LanceDB.AccessKey)
    }
    if cfg.LanceDB.SecretKey != "" {
        os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.LanceDB.SecretKey)
    }
    
    // If ForcePathStyle is false (default for COS/S3-compatible that require virtual host),
    // we want virtual hosted style.
    // If ForcePathStyle is true, we force path style.
    if cfg.LanceDB.ForcePathStyle {
         os.Setenv("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
    } else {
         os.Setenv("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true")
    }

    opts := &contracts.ConnectionOptions{
        StorageOptions: &contracts.StorageOptions{
            S3Config: &contracts.S3Config{
                Region:          &cfg.LanceDB.Region,
                AccessKeyID:     &cfg.LanceDB.AccessKey,
                SecretAccessKey: &cfg.LanceDB.SecretKey,
                Endpoint:        &cfg.LanceDB.Endpoint,
                ForcePathStyle:  &cfg.LanceDB.ForcePathStyle,
            },
            AllowHTTP: &cfg.LanceDB.AllowHTTP,
        },
    }

	db, err := lancedb.Connect(context.Background(), uri, opts)
	if err != nil {
		return fmt.Errorf("failed to connect to lancedb: %w", err)
	}

	LanceDB = db
	log.Println("Connected to LanceDB at", uri)
	return nil
}
