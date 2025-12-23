package database

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"unbound-future-backend/config"
	"unbound-future-backend/models"
)

var DB *gorm.DB

func InitPostgres(cfg *config.Config) error {
	var err error
	dsn := cfg.Database.URL

	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	// Auto migrate
	err = DB.AutoMigrate(
		&models.User{},
		&models.TransferMetadata{},
		&models.TransferJob{},
		&models.YoutubeJob{},
	)
	if err != nil {
		return fmt.Errorf("failed to migrate postgres schema: %w", err)
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	log.Println("Connected to PostgreSQL")
	return nil
}
