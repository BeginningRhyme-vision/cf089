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
	ID          uint           `gorm:"primaryKey" json:"id"`
	ClientName  string         `gorm:"size:255;not null" json:"client_name"`
	Endpoint    string         `gorm:"size:1024;not null" json:"endpoint"`
	AK          string         `gorm:"size:255;not null" json:"ak"`
	SKEncrypted string         `gorm:"column:sk_encrypted;size:1024;not null" json:"sk_encrypted"`
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
	Jobs        []TransferJob  `gorm:"foreignKey:MetadataID" json:"jobs"`
}

func (TransferMetadata) TableName() string {
	return "transfer_metadata"
}

type TransferJob struct {
	JobID           uint      `gorm:"primaryKey;column:job_id" json:"job_id"`
	MetadataID      uint      `gorm:"index" json:"metadata_id"`
	SrcDir          string    `gorm:"size:1024;not null" json:"src_dir"`
	DstDir          string    `gorm:"size:1024;not null" json:"dst_dir"`
	Include         string    `gorm:"size:1024" json:"include"`
	Exclude         string    `gorm:"size:1024" json:"exclude"`
	DeleteSource    bool      `gorm:"default:false" json:"delete_source"`
	IsIncremental   bool      `gorm:"default:false" json:"is_incremental"`
	PeriodicInterval int      `gorm:"default:0" json:"periodic_interval"` // In seconds. 0 = not periodic
	LastScanTime    *time.Time `json:"last_scan_time"`
	Status          JobStatus `gorm:"type:varchar(50);default:'PENDING'" json:"status"`
	StartTime       *time.Time `json:"start_time"`
	EndTime         *time.Time `json:"end_time"`
	DurationSeconds int        `json:"duration_seconds"`
	ExecutionCount  int        `json:"execution_count"`
	TotalCount      int        `gorm:"default:0" json:"total_count"`
	PendingCount    int        `gorm:"default:0" json:"pending_count"`
	RunningCount    int        `gorm:"default:0" json:"running_count"`
	SuccessCount    int        `gorm:"default:0" json:"success_count"`
	FailedCount     int        `gorm:"default:0" json:"failed_count"`
	ResultMessage   string     `gorm:"type:text" json:"result_message"`
	CreatedAt       time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
    
    Metadata        TransferMetadata `gorm:"foreignKey:MetadataID" json:"metadata"`
}

func (TransferJob) TableName() string {
	return "transfer_jobs"
}

type YoutubeJob struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	R2Prefix     string    `gorm:"size:1024;not null" json:"r2_prefix"`
		DownloadMode   string    `gorm:"type:varchar(20);default:'both'" json:"download_mode"` // 'both', 'audio', 'video'
		AudioExtension string    `gorm:"type:varchar(10);default:'m4a'" json:"audio_extension"` // e.g. 'm4a', 'webm', 'mp3'
		VideoExtension string    `gorm:"type:varchar(10);default:'mp4'" json:"video_extension"` // e.g. 'mp4', 'webm', 'mkv'
		FilenameTemplate string  `gorm:"type:varchar(1024);default:''" json:"filename_template"` // e.g. "$(date +%Y%m%d)/%(title).%(ext)"
		Status         JobStatus `gorm:"type:varchar(50);default:'PENDING'" json:"status"`
	
	TotalCount   int       `gorm:"default:0" json:"total_count"`
	PendingCount int       `gorm:"default:0" json:"pending_count"`
	RunningCount int       `gorm:"default:0" json:"running_count"`
	SuccessCount int       `gorm:"default:0" json:"success_count"`
	FailedCount  int       `gorm:"default:0" json:"failed_count"`
	CreatedAt    time.Time `gorm:"autoCreateTime" json:"created_at"`
}

func (YoutubeJob) TableName() string {
	return "youtube_jobs"
}

type FfmpegJob struct {
	ID           uint             `gorm:"primaryKey" json:"id"`
	MetadataID   uint             `gorm:"index" json:"metadata_id"`
	S3Prefix     string           `gorm:"size:1024;not null" json:"s3_prefix"` // e.g., "s3://bucket/path/"
	S3UploadPrefix string         `gorm:"size:1024" json:"s3_upload_prefix"`   // e.g., "s3://bucket/upload_path/"
	IsIncremental bool            `gorm:"default:false" json:"is_incremental"`
	PeriodicInterval int          `gorm:"default:0" json:"periodic_interval"` // In seconds
	LastScanTime    *time.Time    `json:"last_scan_time"`
	Status       JobStatus        `gorm:"type:varchar(50);default:'PENDING'" json:"status"`
	TotalCount   int              `gorm:"default:0" json:"total_count"`
	PendingCount int              `gorm:"default:0" json:"pending_count"`
	RunningCount int              `gorm:"default:0" json:"running_count"`
	SuccessCount int              `gorm:"default:0" json:"success_count"`
	FailedCount  int              `gorm:"default:0" json:"failed_count"`
	CreatedAt    time.Time        `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time        `gorm:"autoUpdateTime" json:"updated_at"`

	Metadata TransferMetadata `gorm:"foreignKey:MetadataID" json:"metadata"`
}

func (FfmpegJob) TableName() string {
	return "ffmpeg_jobs"
}

type PipelineJob struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	Name          string    `gorm:"size:255" json:"name"`
	Status        JobStatus `gorm:"default:'RUNNING'" json:"status"`
	YoutubeJobID  uint      `json:"youtube_job_id"`
	TransferJobID uint      `json:"transfer_job_id"`
	FfmpegJobID   uint      `json:"ffmpeg_job_id"`
	YoutubeURLs   string    `gorm:"type:text" json:"youtube_urls"`
	CreatedAt     time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (PipelineJob) TableName() string {
	return "pipeline_jobs"
}
