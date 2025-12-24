package models

import (
	"time"
)

// YoutubeTask represents a task
type YoutubeTask struct {
	ID           int64     `json:"id"`
	JobID        int64     `json:"job_id"`
	URL          string    `json:"url"`
	Status       string    `json:"status"` // PENDING, RUNNING, COMPLETED, FAILED
	Title        string    `json:"title"`
	VideoID      string    `json:"video_id"`
	ErrorMessage string    `json:"error_message"`
	WorkerID     string    `json:"worker_id"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type TransferTask struct {
	ID           int64     `json:"id"`
	JobID        int64     `json:"job_id"`
	Src          string    `json:"src"`
	Status       string    `json:"status"` // PENDING, RUNNING, COMPLETED, FAILED
	ErrorMessage string    `json:"error_message"`
	WorkerID     string    `json:"worker_id"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
