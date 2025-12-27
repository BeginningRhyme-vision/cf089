package models

import (
	"time"
)

// YoutubeTask represents a task
type YoutubeTask struct {
	ID           int64     `json:"id"`
	JobID        int64     `json:"job_id"`
	URL          string    `json:"url"`
	AudioURL     string    `json:"audio_url"`
	AudioSize    int64     `json:"audio_size"`
	VideoURL     string    `json:"video_url"`
	VideoSize    int64     `json:"video_size"`
	Status       string    `json:"status"` // PENDING, RUNNING, COMPLETED, FAILED
	Title        string    `json:"title"`
	VideoID      string    `json:"video_id"`
	ErrorMessage string    `json:"error_message"`
	WorkerID     string    `json:"worker_id"`
	IsDownloadFail bool      `json:"is_download_fail"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type TransferTask struct {
	ID           int64     `json:"id"`
	JobID        int64     `json:"job_id"`
	Src          string    `json:"src"`
	Size         int64     `json:"size"`
	Status       string    `json:"status"` // PENDING, RUNNING, COMPLETED, FAILED
	ErrorMessage string    `json:"error_message"`
	WorkerID     string    `json:"worker_id"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type FfmpegTask struct {
	ID           int64     `json:"id"`
	JobID        int64     `json:"job_id"`
	S3Endpoint   string    `json:"s3_endpoint"`
	S3Bucket     string    `json:"s3_bucket"`
	S3Prefix     string    `json:"s3_prefix"`
	S3AK         string    `json:"s3_ak"`
	S3SK         string    `json:"s3_sk"`
	Region       string    `json:"region"`
	Status       string    `json:"status"` // PENDING, RUNNING, COMPLETED, FAILED
	ErrorMessage string    `json:"error_message"`
	WorkerID     string    `json:"worker_id"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
