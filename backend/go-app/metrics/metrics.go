package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpReqDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds",
		Help: "Duration of HTTP requests in seconds",
	}, []string{"method", "path", "status"})

	HttpReqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "path", "status"})

	JobCreatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "job_created_total",
		Help: "Total number of jobs created",
	}, []string{"type"}) // "transfer" or "youtube"

	TaskStatusChangeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "task_status_change_total",
		Help: "Total number of task status transitions",
	}, []string{"job_type", "status"}) // job_type: "transfer", "youtube"; status: "PENDING", "RUNNING", "COMPLETED", "FAILED"

	ActiveJobsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "active_jobs_count",
		Help: "Number of currently active jobs",
	}, []string{"type"})
)
