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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gopkg.in/yaml.v3"
)

// Transfer Worker
// 1. Acquire Tasks (Transfer)
// 2. Transfer using external service

type Config struct {
	Storage StorageConfig `yaml:"storage"`
}

type StorageConfig struct {
	Src                SrcConfig `yaml:"src"`
	TransferServiceURL string    `yaml:"transfer_service_url"`
}

type SrcConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type TransferTask struct {
	ID     int64  `json:"id"`
	JobID  int64  `json:"job_id"`
	Src    string `json:"src"`
	Size   int64  `json:"size"`
	Status string `json:"status"`
}

type JobInfo struct {
	JobID        uint             `json:"job_id"`
	Metadata     TransferMetadata `json:"metadata"`
	DstDir       string           `json:"dst_dir"`
	SrcDir       string           `json:"src_dir"`
	DeleteSource bool             `json:"delete_source"`
}

type TransferMetadata struct {
	Endpoint    string `json:"endpoint"`
	AK          string `json:"ak"`
	SKEncrypted string `json:"sk_encrypted"`
}

var (
	cfg        *Config
	apiBaseURL string
	jobCache   sync.Map // JobID -> JobInfo
	
	defaultPartSize int64 = 16 * 1024 * 1024
	s3Clients  sync.Map // Endpoint -> *s3.Client (Cache for Destinations)
	srcClient  *s3.Client
)

const WorkerID = "go-transfer-1"

func main() {
	loadConfig()
	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}

	initSourceClient()

	log.Println("Transfer Worker Started")

	for {
		tasks, err := acquireTasks()
		if err != nil {
			log.Printf("Error acquiring tasks: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(tasks) == 0 {
			time.Sleep(60 * time.Second)
			continue
		}

		log.Printf("Acquired %d tasks", len(tasks))
		var wg sync.WaitGroup
		for _, t := range tasks {
			wg.Add(1)
			go func(task TransferTask) {
				defer wg.Done()
				processTask(task)
			}(t)
		}
		wg.Wait()
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
		log.Fatal("Could not find config.yaml")
	}

	cfg = &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
}

func initSourceClient() {
	var err error
	srcClient, err = createS3Client(cfg.Storage.Src.Endpoint, cfg.Storage.Src.AccessKey, cfg.Storage.Src.SecretKey)
	if err != nil {
		log.Fatalf("Failed to init source client: %v", err)
	}
}

func createS3Client(endpoint, ak, sk string) (*s3.Client, error) {
	// Normalize endpoint to http/https as AWS SDK BaseEndpoint requires a web URI
	normalized := endpoint
	isS3 := strings.HasPrefix(endpoint, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(endpoint, "s3://")
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

func calculatePartSize(size int64) int64 {
	partSize := defaultPartSize
	// Max parts is 10000 for S3
	if size > partSize*10000 {
		partSize = size / 10000
		partSize = ((partSize-1)>>20 + 1) << 20 // align to MB
	}
	return partSize
}

func acquireTasks() ([]TransferTask, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"worker_id": WorkerID,
		"limit":     1024,
	})

	resp, err := http.Post(apiBaseURL+"/transfer-tasks/acquire", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []TransferTask
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func getJobInfo(jobID int64) (*JobInfo, error) {
	if val, ok := jobCache.Load(jobID); ok {
		j := val.(JobInfo)
		return &j, nil
	}

	resp, err := http.Get(fmt.Sprintf("%s/jobs/%d", apiBaseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("job fetch failed: %d", resp.StatusCode)
	}

	var job JobInfo
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}

	jobCache.Store(jobID, job)
	return &job, nil
}

func updateTaskStatus(id int64, status, msg string) {
	req := TransferTask{
		ID: id,
		Status: status,
	}
	// Error message handling not in basic struct, but we can assume backend might handle extended fields if added.
	// For now just Status.
	
	wrapper := []TransferTask{req}
	data, _ := json.Marshal(wrapper)
	
	http.Post(apiBaseURL+"/transfer-tasks/update", "application/json", bytes.NewBuffer(data))
}

func processTask(t TransferTask) {
	job, err := getJobInfo(t.JobID)
	if err != nil {
		log.Printf("Failed to get job info for task %d: %v", t.ID, err)
		updateTaskStatus(t.ID, "FAILED", err.Error())
		return
	}

	srcDir := strings.TrimSpace(job.SrcDir)
	dstDir := strings.TrimSpace(job.DstDir)

	log.Printf("Processing Task %d (Job %d): %s -> %s", t.ID, t.JobID, t.Src, dstDir)

	// 1. Resolve Dst Client
	sk := job.Metadata.SKEncrypted
	if strings.HasPrefix(sk, "enc_") {
		sk = strings.TrimPrefix(sk, "enc_")
	}
	
	dstClient, err := createS3Client(job.Metadata.Endpoint, job.Metadata.AK, sk)
	if err != nil {
		log.Printf("Dst client init failed for task %d: %v", t.ID, err)
		updateTaskStatus(t.ID, "FAILED", "Dst client init failed")
		return
	}
	
	// 2. Resolve Paths
	srcKey := t.Src
	var relKey string
	if srcDir != "" {
		if srcKey == srcDir {
			relKey = ""
		} else if strings.HasPrefix(srcKey, srcDir) {
			relKey = strings.TrimPrefix(srcKey, srcDir)
			relKey = strings.TrimPrefix(relKey, "/")
		} else {
			relKey = srcKey
		}
	} else {
		relKey = srcKey
	}
	
	dstKey := relKey
	if dstDir != "" {
		if dstKey == "" {
			dstKey = dstDir
		} else {
			dstKey = strings.TrimSuffix(dstDir, "/") + "/" + dstKey
		}
	}
	
	// 3. Get Object Info (Size) from Src
	srcBucket := getBucketFromEndpoint(cfg.Storage.Src.Endpoint)
	size := t.Size
	
	if size == 0 {
		head, err := srcClient.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(srcKey),
		})
		if err != nil {
			log.Printf("HeadObject failed for %s/%s: %v", srcBucket, srcKey, err)
			updateTaskStatus(t.ID, "FAILED", "HeadObject failed: "+err.Error())
			return
		}
		size = *head.ContentLength
	}
	
	// 4. Construct Public/Virtual-Hosted URLs for Transfer Service (Matches r2s3.go logic)
	srcUrl, err := constructVirtualHostURL(cfg.Storage.Src.Endpoint, srcBucket, srcKey)
	if err != nil {
		log.Printf("Failed to construct Src URL for task %d: %v", t.ID, err)
		updateTaskStatus(t.ID, "FAILED", "Construct Src URL failed")
		return
	}
	
	dstBucket := getBucketFromEndpoint(job.Metadata.Endpoint)
	log.Printf("Task %d: Transferring %d bytes to bucket '%s' key '%s'", t.ID, size, dstBucket, dstKey)
	
	// 5. Transfer Loop
	err = transferFile(srcUrl, dstClient, dstBucket, dstKey, size, job.Metadata.Endpoint)
	if err != nil {
		log.Printf("Transfer failed for task %d: %v", t.ID, err)
		updateTaskStatus(t.ID, "FAILED", err.Error())
	} else {
		log.Printf("Task %d completed successfully", t.ID)
		
		if job.DeleteSource {
			_, err := srcClient.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(srcBucket),
				Key:    aws.String(srcKey),
			})
			if err != nil {
				log.Printf("Failed to delete source object %s for task %d: %v", srcKey, t.ID, err)
			} else {
				log.Printf("Deleted source object %s for task %d", srcKey, t.ID)
			}
		}

		updateTaskStatus(t.ID, "COMPLETED", "")
	}
}

func transferFile(srcURL string, dstClient *s3.Client, dstBucket, dstKey string, size int64, dstEndpoint string) error {
	dstUrl, err := constructVirtualHostURL(dstEndpoint, dstBucket, dstKey)
	if err != nil {
		return err
	}

	if size < defaultPartSize {
		_, err = callTransferService(srcURL, dstUrl, size, 0, "", -1)
		return err
	}
	
	// Multipart
	createOut, err := dstClient.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(dstBucket),
		Key:    aws.String(dstKey),
	})
	if err != nil { return err }
	uploadID := *createOut.UploadId
	
	partSize := calculatePartSize(size)
	numParts := int((size-1)/partSize) + 1

	var completedParts []types.CompletedPart
	var mu sync.Mutex
	var wg sync.WaitGroup
	errAbort := make(chan error, 1)
	
	sem := make(chan struct{}, 4)
	
	for i := 0; i < numParts; i++ {
		start := int64(i) * partSize
		end := start + partSize - 1
		if end >= size { end = size - 1 }
		partNum := int32(i + 1)
		
		wg.Add(1)
		go func(pNum int32, s, e int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			
			// Use clean URLs, no presigning
			etag, err := callTransferService(srcURL, dstUrl, e-s+1, s, uploadID, int(pNum))
			if err != nil {
				select { case errAbort <- err: default: }; return
			}
			
			mu.Lock()
			completedParts = append(completedParts, types.CompletedPart{
				ETag: aws.String(etag),
				PartNumber: aws.Int32(pNum),
			})
			mu.Unlock()
		}(partNum, start, end)
	}
	
	wg.Wait()
	
	select {
	case err := <-errAbort:
		dstClient.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		})
		return err
	default:
	}
	
	// Sort
	for i := 0; i < len(completedParts); i++ {
		for j := i + 1; j < len(completedParts); j++ {
			if *completedParts[i].PartNumber > *completedParts[j].PartNumber {
				completedParts[i], completedParts[j] = completedParts[j], completedParts[i]
			}
		}
	}
	
	_, err = dstClient.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	return err
}

func callTransferService(srcUrl, dstUrl string, size, offset int64, uploadID string, partNum int) (string, error) {
	// Payload matches r2s3 / downloader logic
	// But `r2s3` sent `r2Key` and `s3Url`.
	// We are sending Presigned URLs for both.
	
	payload := map[string]interface{}{
		"r2Key":      srcUrl,
		"s3Url":      dstUrl,
		"size":       size,
		"offset":     offset,
		"uploadId":   uploadID,
		"partNumber": partNum,
	}
	
	body, _ := json.Marshal(payload)
	
	// Retry
	for i:=0; i<3; i++ {
		resp, err := http.Post(cfg.Storage.TransferServiceURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			time.Sleep(1*time.Second)
			continue
		}
		defer resp.Body.Close()
		
		if resp.StatusCode == 200 {
			var res map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&res)
			if etag, ok := res["etag"].(string); ok {
				return etag, nil
			}
		}
		time.Sleep(1*time.Second)
	}
	return "", fmt.Errorf("service call failed")
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

func constructVirtualHostURL(endpointStr, bucket, key string) (string, error) {
	// Mimic r2s3.go logic for creating scheme://bucket.host/key
	
	// Normalize just to parse host/scheme cleanly if needed, but r2s3 uses simple parsing
	normalized := endpointStr
	isS3 := strings.HasPrefix(endpointStr, "s3://")
	if isS3 {
		normalized = "http://" + strings.TrimPrefix(endpointStr, "s3://")
	}
	if !strings.Contains(normalized, "://") {
		normalized = "http://" + normalized
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}

	host := u.Host
	if isS3 {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}
	
	// r2s3 style: fmt.Sprintf("%s://%s.%s/%s", u.Scheme, srcCfg.Bucket, u.Host, obj.Key)
	return fmt.Sprintf("%s://%s.%s/%s", u.Scheme, bucket, host, key), nil
}