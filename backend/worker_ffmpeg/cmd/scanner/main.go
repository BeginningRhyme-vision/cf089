package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"worker_ffmpeg/common"
)

const (
	TaskQueue   = "queue:ffmpeg:pending"
	DedupPrefix = "queue:ffmpeg:dedup:"
)

var (
	apiBaseURL string
	rdb        *redis.Client

	PagesScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ffmpeg_scanner_pages_scanned_total",
		Help: "Total number of S3 pages scanned",
	})

	TasksDiscovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ffmpeg_scanner_tasks_discovered_total",
		Help: "Total number of tasks discovered",
	})
)

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server listening on :9093")
		http.ListenAndServe(":9093", nil)
	}()

	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}

	initRedis()

	log.Println("FFmpeg Scanner Started")

	var activeJobs sync.Map

	for {
		jobs, err := getPendingJobs()
		if err != nil {
			log.Printf("Error getting pending jobs: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(jobs) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, job := range jobs {
			if _, loaded := activeJobs.LoadOrStore(job.ID, true); loaded {
				continue
			}

			go func(j common.FfmpegJob) {
				defer activeJobs.Delete(j.ID)
				processJob(j)
			}(job)
		}

		time.Sleep(5 * time.Second)
	}
}

func initRedis() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		// Fallback for local dev
		redisURL = "redis://localhost:6379/0"
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Invalid Redis URL: %v", err)
	}
	rdb = redis.NewClient(opt)
	
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Printf("Warning: Failed to ping Redis at startup: %v", err)
	}
}

func getPendingJobs() ([]common.FfmpegJob, error) {
	resp, err := http.Get(apiBaseURL + "/ffmpeg-jobs/pending")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var jobs []common.FfmpegJob
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func updateJobStatus(jobID int64, status string, lastScanTime *time.Time, msg string, totalCount *int) error {
	req := common.UpdateJobStatusRequest{
		Status:        status,
		LastScanTime:  lastScanTime,
		ResultMessage: msg,
	}
	if totalCount != nil {
		req.TotalCount = totalCount
	}

	data, _ := json.Marshal(req)

	reqObj, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/ffmpeg-jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
	reqObj.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(reqObj)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to update status: %d", resp.StatusCode)
	}
	return nil
}

type FilePair struct {
	Video     string
	Audio     string
	VideoSize int64
	AudioSize int64
}

func processJob(job common.FfmpegJob) {
	startTime := time.Now()
	log.Printf("Processing Job %d: %s (Incremental: %v)", job.ID, job.S3Prefix, job.IsIncremental)

	if err := updateJobStatus(job.ID, "RUNNING", nil, "", nil); err != nil {
		if strings.Contains(err.Error(), "status 404") {
			cleanUpDedup(job.ID)
		}
		log.Printf("Failed to set RUNNING for job %d: %v", job.ID, err)
		return
	}

	s3Client, err := createS3Client(job.Metadata.Endpoint, job.Metadata.AK, job.Metadata.SKEncrypted)
	if err != nil {
		log.Printf("Failed to init S3 for job %d: %v", job.ID, err)
		updateJobStatus(job.ID, "FAILED", nil, fmt.Sprintf("Init S3 failed: %v", err), nil)
		return
	}

	bucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	prefix := job.S3Prefix

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	pairs := make(map[string]*FilePair)
	pages := 0
	count := 0

	for paginator.HasMorePages() {
		pages++
		PagesScanned.Inc()
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("List failed for job %d: %v", job.ID, err)
			updateJobStatus(job.ID, "FAILED", nil, err.Error(), nil)
			return
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			name := filepath.Base(key)
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}

			if strings.Contains(name, "_video.") {
				id := strings.Split(name, "_video.")[0]
				if _, ok := pairs[id]; !ok {
					pairs[id] = &FilePair{}
				}
				pairs[id].Video = key
				pairs[id].VideoSize = size
			} else if strings.Contains(name, "_audio.") {
				id := strings.Split(name, "_audio.")[0]
				if _, ok := pairs[id]; !ok {
					pairs[id] = &FilePair{}
				}
				pairs[id].Audio = key
				pairs[id].AudioSize = size
			}
		}

		// Check for complete pairs
		var candidates []struct {
			id     string
			pair   *FilePair
			taskID int64
		}

		for id, p := range pairs {
			if p.Video != "" && p.Audio != "" {
				candidates = append(candidates, struct {
					id     string
					pair   *FilePair
					taskID int64
				}{id, p, generateTaskID(id)})
			}
		}

		var batch []interface{}
		if len(candidates) > 0 {
			pipe := rdb.Pipeline()
			dedupKey := fmt.Sprintf("%s%d", DedupPrefix, job.ID)

			for _, c := range candidates {
				pipe.SAdd(context.Background(), dedupKey, c.taskID)
			}

			cmders, err := pipe.Exec(context.Background())
			if err != nil {
				log.Printf("Dedup pipeline failed for job %d: %v", job.ID, err)
			} else {
				for i, cmder := range cmders {
					if cmd, ok := cmder.(*redis.IntCmd); ok {
						if cmd.Val() > 0 {
							// New task (SAdd returned 1)
							c := candidates[i]
							task := common.FfmpegTask{
								ID:        c.taskID,
								JobID:     job.ID,
								VideoKey:  c.pair.Video,
								AudioKey:  c.pair.Audio,
								VideoSize: c.pair.VideoSize,
								AudioSize: c.pair.AudioSize,
								Status:    "PENDING",
							}

							data, _ := json.Marshal(task)
							batch = append(batch, data)

							TasksDiscovered.Inc()
						}
					}
					// Always cleanup completed pairs
					delete(pairs, candidates[i].id)
				}
			}
		}

		if len(batch) > 0 {
			var err error
			// Retry up to 3 times
			for i := 0; i < 3; i++ {
				err = rdb.RPush(context.Background(), TaskQueue, batch...).Err()
				if err == nil {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			
			if err != nil {
				log.Printf("Failed to push batch for job %d: %v", job.ID, err)
			}
			count += len(batch)
			// Update total count
			if err := updateJobStatus(job.ID, "RUNNING", nil, "", &count); err != nil {
				if strings.Contains(err.Error(), "status 404") {
					cleanUpDedup(job.ID)
					return
				}
				log.Printf("Warning: failed to update status for job %d: %v", job.ID, err)
			}
		}
	}

	resultMsg := fmt.Sprintf("Scanned %d pages. Tasks Discovered: %d", pages, count)
	log.Printf("Job %d scan completed. %s", job.ID, resultMsg)

	var errFinal error
	if job.IsIncremental {
		// Keep running, update scan time
		errFinal = updateJobStatus(job.ID, "RUNNING", &startTime, resultMsg, nil)
	} else {
		errFinal = updateJobStatus(job.ID, "COMPLETED", &startTime, resultMsg, nil)
	}

	if errFinal != nil {
		if strings.Contains(errFinal.Error(), "status 404") {
			cleanUpDedup(job.ID)
		}
		log.Printf("Final status update failed for job %d: %v", job.ID, errFinal)
	}
}

func createS3Client(endpoint, ak, sk string) (*s3.Client, error) {
	if strings.HasPrefix(sk, "enc_") {
		sk = strings.TrimPrefix(sk, "enc_")
	}

	normalized := endpoint
	isS3 := strings.HasPrefix(normalized, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(normalized, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return nil, err
	}

	host := u.Host
	if isS3 {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}

	baseEndpoint := fmt.Sprintf("%s://%s", u.Scheme, host)

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		awsconfig.WithRegion("auto"),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(c, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = false
	}), nil
}

func getBucketFromEndpoint(endpoint string) string {
	if strings.HasPrefix(endpoint, "s3://") {
		host := strings.TrimPrefix(endpoint, "s3://")
		parts := strings.Split(host, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
		return strings.Trim(u.Path, "/")
	}
	
	func generateTaskID(s string) int64 {
		h := fnv.New64a()
		h.Write([]byte(s))
		// Ensure positive ID
		return int64(h.Sum64() & 0x7fffffffffffffff)
	}
	
	func cleanUpDedup(jobID int64) {
		key := fmt.Sprintf("%s%d", DedupPrefix, jobID)
		if err := rdb.Del(context.Background(), key).Err(); err != nil {
			log.Printf("Failed to cleanup dedup key %s: %v", key, err)
		} else {
			log.Printf("Cleaned up dedup key %s for deleted job %d", key, jobID)
		}
	}
	