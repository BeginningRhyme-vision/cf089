package models

import (
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusPaused    JobStatus = "PAUSED"
	StatusStopped   JobStatus = "STOPPED"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
)

type User struct {
	ID           uint      `gorm:"primaryKey"`
	FeishuOpenID string    `gorm:"uniqueIndex;size:255;not null"`
	Name         string    `gorm:"size:255"`
	Email        string    `gorm:"size:255"`
	AvatarURL    string    `gorm:"size:1024"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}

type TransferMetadata struct {
	ID          uint           `gorm:"primaryKey"`
	ClientName  string         `gorm:"size:255;not null"`
	Endpoint    string         `gorm:"size:1024;not null"`
	AK          string         `gorm:"size:255;not null"`
	SKEncrypted string         `gorm:"column:sk_encrypted;size:1024;not null"`
	CreatedAt   time.Time      `gorm:"autoCreateTime"`
	UpdatedAt   time.Time      `gorm:"autoUpdateTime"`
	Jobs        []TransferJob  `gorm:"foreignKey:MetadataID"`
}

func (TransferMetadata) TableName() string {
	return "transfer_metadata"
}

type TransferJob struct {
	JobID           uint      `gorm:"primaryKey;column:job_id"`
	MetadataID      uint      `gorm:"index"`
	SrcDir          string    `gorm:"size:1024;not null"`
	DstDir          string    `gorm:"size:1024;not null"`
	Include         string    `gorm:"size:1024"`
	Exclude         string    `gorm:"size:1024"`
	DeleteSource    bool      `gorm:"default:false"`
	IsIncremental   bool      `gorm:"default:false"`
	Status          JobStatus `gorm:"type:varchar(50);default:'PENDING'"`
	StartTime       *time.Time
	EndTime         *time.Time
	DurationSeconds int
	ExecutionCount  int
	ResultMessage   string    `gorm:"type:text"`
	CreatedAt       time.Time `gorm:"autoCreateTime"`
	UpdatedAt       time.Time `gorm:"autoUpdateTime"`
    
    Metadata        TransferMetadata `gorm:"foreignKey:MetadataID"`
}

func (TransferJob) TableName() string {
	return "transfer_jobs"
}

type YoutubeJob struct {
	ID           uint      `gorm:"primaryKey"`
	R2Prefix     string    `gorm:"size:1024;not null"`
	Status       JobStatus `gorm:"type:varchar(50);default:'PENDING'"`
	TotalCount   int       `gorm:"default:0"`
	PendingCount int       `gorm:"default:0"`
	RunningCount int       `gorm:"default:0"`
	SuccessCount int       `gorm:"default:0"`
	FailedCount  int       `gorm:"default:0"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}

func (YoutubeJob) TableName() string {
	return "youtube_jobs"
}
