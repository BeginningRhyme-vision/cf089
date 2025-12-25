package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"
)

// Scanner Worker
// 1. Polls Backend for PENDING TransferJobs
// 2. Lists Source S3/R2
// 3. Batches inserts to Backend
// 4. Updates Status to RUNNING

type Config struct {
	Storage StorageConfig `yaml:"storage"`
}

type StorageConfig struct {
	Src SrcConfig `yaml:"src"`
}

type SrcConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type TransferJob struct {
	JobID            uint             `json:"job_id"`
	SrcDir           string           `json:"src_dir"`
	DstDir           string           `json:"dst_dir"`
	Metadata         TransferMetadata `json:"metadata"`
	Status           string           `json:"status"`
	PeriodicInterval int              `json:"periodic_interval"`
	IsIncremental    bool             `json:"is_incremental"`
	LastScanTime     *time.Time       `json:"last_scan_time"`
}

type TransferMetadata struct {
	ID          uint   `json:"id"`
	Endpoint    string `json:"endpoint"`
	AK          string `json:"ak"`
	SKEncrypted string `json:"sk_encrypted"`
}

type UpdateStatusRequest struct {
	Status       string     `json:"status"`
	LastScanTime *time.Time `json:"last_scan_time,omitempty"`
}

var (
	cfg        *Config
	apiBaseURL string
)

func main() {
	loadConfig()
	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}

	log.Println("Scanner Worker Started")

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
			processJob(job)
		}
	}
}

func loadConfig() {
	paths := []string{"../../config.yaml", "../config.yaml", "config.yaml"}
	var data []byte
	var err error
	for _, p := range paths {
		data, err = os.ReadFile(p)
		if err == nil {
			break
		}
	}
	if data == nil {
		// Fallback or just empty if we rely on DB for source creds?
		// Note: The previous r2s3 implementation read Source creds from CLI or Env, OR DB?
		// The Scanner needs Source Creds. 
		// The Job has Metadata which is for DESTINATION.
		// Source is usually global in config.yaml?
		// Checking `config.yaml` content from memory: `storage.src` has endpoint, access_key, secret_key.
		// Yes, Source is global.
		log.Fatal("Could not find config.yaml")
	}

	cfg = &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
}

func getPendingJobs() ([]TransferJob, error) {
	resp, err := http.Get(apiBaseURL + "/jobs/pending")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var jobs []TransferJob
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func updateJobStatus(jobID uint, status string, lastScanTime *time.Time) error {
	req := UpdateStatusRequest{Status: status, LastScanTime: lastScanTime}
	data, _ := json.Marshal(req)
	
	reqObj, _ := http.NewRequest("PATCH", fmt.Sprintf("%s/jobs/%d/status", apiBaseURL, jobID), bytes.NewBuffer(data))
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

func processJob(job TransferJob) {
	startTime := time.Now()
	jobJSON, _ := json.Marshal(job)
	log.Printf("Processing Job: %s", string(jobJSON))

	// 1. Update status to RUNNING
	if err := updateJobStatus(job.JobID, "RUNNING", nil); err != nil {
		log.Printf("Failed to set RUNNING for job %d: %v", job.JobID, err)
		return
	}

	// 2. Init S3 Source Client
	s3Client, err := initSourceS3()
	if err != nil {
		log.Printf("Failed to init S3 for job %d: %v", job.JobID, err)
		updateJobStatus(job.JobID, "FAILED", nil)
		return
	}

	// 3. List and Batch Insert
	bucketName := getBucketFromEndpoint(cfg.Storage.Src.Endpoint)
	prefix := strings.TrimSpace(job.SrcDir)
	log.Printf("Listing objects for job %d in bucket '%s' with prefix '%s'", job.JobID, bucketName, prefix)

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	var batch []string
	count := 0
	skipped := 0
	pages := 0

	for paginator.HasMorePages() {
		pages++
		log.Printf("Requesting page %d for job %d...", pages, job.JobID)
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("ListObjectsV2 failed for job %d on page %d: %v", job.JobID, pages, err)
			updateJobStatus(job.JobID, "FAILED", nil) // Mark as failed on list error
			return
		}

		log.Printf("Page %d for job %d contained %d objects.", pages, job.JobID, len(page.Contents))
		for _, obj := range page.Contents {
			key := *obj.Key
			if strings.HasSuffix(key, "/") {
				continue
			}

			if job.IsIncremental && job.LastScanTime != nil && obj.LastModified != nil {
				if !obj.LastModified.After(*job.LastScanTime) {
					skipped++
					continue
				}
			}

			batch = append(batch, key)
			if len(batch) >= 1000 {
				if err := sendBatch(job.JobID, batch); err != nil {
					log.Printf("Failed to send batch for job %d: %v", job.JobID, err)
				}
				count += len(batch)
				batch = nil
			}
		}
	}

	if len(batch) > 0 {
		if err := sendBatch(job.JobID, batch); err != nil {
			log.Printf("Failed to send final batch for job %d: %v", job.JobID, err)
		}
		count += len(batch)
	}

	log.Printf("Job %d scanned. Total pages: %d. New tasks: %d, Skipped (old): %d", job.JobID, pages, count, skipped)

	if job.PeriodicInterval > 0 {
		updateJobStatus(job.JobID, "RUNNING", &startTime)
		log.Printf("Job %d is periodic. Next scan in %d seconds.", job.JobID, job.PeriodicInterval)
	} else {
		updateJobStatus(job.JobID, "COMPLETED", &startTime)
	}
}

func sendBatch(jobID uint, tasks []string) error {
	payload := map[string]interface{}{
		"tasks": tasks,
	}
	data, _ := json.Marshal(payload)
	resp, err := http.Post(fmt.Sprintf("%s/jobs/%d/tasks", apiBaseURL, jobID), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func initSourceS3() (*s3.Client, error) {
	// Normalize endpoint to http/https as AWS SDK BaseEndpoint requires a web URI
	normalized := cfg.Storage.Src.Endpoint
	if strings.HasPrefix(normalized, "s3://") {
		normalized = "http://" + strings.TrimPrefix(normalized, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return nil, err
	}
	baseEndpoint := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Storage.Src.AccessKey,
			cfg.Storage.Src.SecretKey,
			"",
		)),
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
	// Handle s3:// scheme specifically before normalization
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
