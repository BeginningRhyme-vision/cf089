package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	TempDir = "/dev/shm" // Fallback to /tmp if not exists
)

var (
	apiBaseURL string
	workerID   string
)

type FfmpegTask struct {
	ID             int64  `json:"id"`
	JobID          int64  `json:"job_id"`
	S3Endpoint     string `json:"s3_endpoint"`
	S3Bucket       string `json:"s3_bucket"`
	S3Prefix       string `json:"s3_prefix"`
	S3UploadPrefix string `json:"s3_upload_prefix"`
	S3AK           string `json:"s3_ak"`
	S3SK           string `json:"s3_sk"`
	Status         string `json:"status"`
}

func main() {
	workerID = fmt.Sprintf("ffmpeg-worker-%d", rand.Intn(1000000))
	apiBaseURL = os.Getenv("BACKEND_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "http://localhost:8080/api"
	}

	// Check /dev/shm
	if _, err := os.Stat(TempDir); os.IsNotExist(err) {
		log.Println("/dev/shm not found, using /tmp")
	}

	log.Printf("FFmpeg Worker %s Started\n", workerID)

	for {
		tasks, err := acquireTasks()
		if err != nil {
			log.Printf("Error acquiring tasks: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(tasks) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, task := range tasks {
			processTask(task)
		}
	}
}

func acquireTasks() ([]FfmpegTask, error) {
	payload := map[string]interface{}{
		"worker_id": workerID,
		"limit":     1, // Process one big task (folder) at a time
	}
	data, _ := json.Marshal(payload)

	resp, err := http.Post(apiBaseURL+"/ffmpeg-tasks/acquire", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []FfmpegTask
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func updateTaskStatus(task FfmpegTask, status string) {
	task.Status = status
	payload := []FfmpegTask{task}
	data, _ := json.Marshal(payload)

	// We use fire-and-forget for simplicity or log error
	resp, err := http.Post(apiBaseURL+"/ffmpeg-tasks/update", "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Failed to update status: %v", err)
		return
	}
	defer resp.Body.Close()
}

func processTask(t FfmpegTask) {
	log.Printf("Processing Task %d: %s", t.ID, t.S3Prefix)
	updateTaskStatus(t, "RUNNING")

	s3Client, err := createS3Client(t.S3Endpoint, t.S3AK, t.S3SK)
	if err != nil {
		log.Printf("Failed to create S3 client: %v", err)
		updateTaskStatus(t, "FAILED")
		return
	}

	bucket := getBucketFromEndpoint(t.S3Endpoint)
	prefix := t.S3Prefix
	uploadPrefix := t.S3UploadPrefix
	if uploadPrefix == "" {
		uploadPrefix = prefix // Fallback
	}

	// List objects
	pairs, err := listPairs(s3Client, bucket, prefix)
	if err != nil {
		log.Printf("List failed: %v", err)
		updateTaskStatus(t, "FAILED")
		return
	}

	log.Printf("Found %d pairs to process", len(pairs))

	var successCount int32
	var failCount int32
	var wg sync.WaitGroup

	// Limit concurrency to 4
	sem := make(chan struct{}, 32)

	for id, files := range pairs {
		if files.Audio == "" || files.Video == "" {
			log.Printf("Incomplete pair for %s: %+v", id, files)
			continue
		}

		wg.Add(1)
		go func(id string, files *FilePair) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			err := processPair(s3Client, bucket, uploadPrefix, id, files.Video, files.Audio)
			if err != nil {
				log.Printf("Failed to process %s: %v", id, err)
				atomic.AddInt32(&failCount, 1)
			} else {
				atomic.AddInt32(&successCount, 1)
				// Delete source files on success
				if len(files.AllKeys) > 0 {
					if err := deleteObjects(s3Client, bucket, files.AllKeys); err != nil {
						log.Printf("Failed to delete source files for %s: %v", id, err)
					} else {
						log.Printf("Deleted source files for %s", id)
					}
				}
			}
		}(id, files)
	}

	wg.Wait()

	log.Printf("Task %d completed. Success: %d, Failed: %d", t.ID, successCount, failCount)
	if failCount > 0 && successCount == 0 {
		updateTaskStatus(t, "FAILED")
	} else {
		updateTaskStatus(t, "COMPLETED")
	}
}

type FilePair struct {
	Video   string
	Audio   string
	AllKeys []string
}

func listPairs(client *s3.Client, bucket, prefix string) (map[string]*FilePair, error) {
	pairs := make(map[string]*FilePair)
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			name := filepath.Base(key)

			// Expected format: {id}_video.{ext} or {id}_audio.{ext}
			// Example: dQw4w9WgXcQ_video.mp4, dQw4w9WgXcQ_audio.m4a

			if strings.Contains(name, "_video.") {
				id := strings.Split(name, "_video.")[0]
				if _, ok := pairs[id]; !ok {
					pairs[id] = &FilePair{}
				}
				pairs[id].Video = key
				pairs[id].AllKeys = append(pairs[id].AllKeys, key)
			} else if strings.Contains(name, "_audio.") {
				id := strings.Split(name, "_audio.")[0]
				if _, ok := pairs[id]; !ok {
					pairs[id] = &FilePair{}
				}
				pairs[id].Audio = key
				pairs[id].AllKeys = append(pairs[id].AllKeys, key)
			}
		}
	}
	return pairs, nil
}

func deleteObjects(client *s3.Client, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	var objects []types.ObjectIdentifier
	for _, k := range keys {
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(k)})
	}

	_, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	})
	return err
}

func processPair(client *s3.Client, bucket, uploadPrefix, id, videoKey, audioKey string) error {
	// Output: {uploadPrefix}/{id}.mp4
	outputKey := strings.TrimRight(uploadPrefix, "/") + "/" + id + ".mp4"
	if strings.HasPrefix(outputKey, "/") {
		outputKey = strings.TrimPrefix(outputKey, "/")
	}

	// Check existence
	_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(outputKey),
	})
	if err == nil {
		log.Printf("Output %s already exists, skipping.", outputKey)
		return nil
	}

	// Download
	workDir := TempDir
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		workDir = os.TempDir()
	}

	localVideo := filepath.Join(workDir, filepath.Base(videoKey))
	localAudio := filepath.Join(workDir, filepath.Base(audioKey))
	localOutput := filepath.Join(workDir, id+".mp4")

	defer os.Remove(localVideo)
	defer os.Remove(localAudio)
	defer os.Remove(localOutput)

	log.Printf("Downloading %s and %s...", videoKey, audioKey)
	if err := downloadFile(client, bucket, videoKey, localVideo); err != nil {
		return fmt.Errorf("download video failed: %w", err)
	}
	if err := downloadFile(client, bucket, audioKey, localAudio); err != nil {
		return fmt.Errorf("download audio failed: %w", err)
	}

	// Merge
	// ffmpeg -i video -i audio -c copy -map 0:v:0 -map 1:a:0 output.mp4
	log.Printf("Merging %s...", id)
	cmd := exec.Command("ffmpeg", "-y", "-i", localVideo, "-i", localAudio, "-c", "copy", "-map", "0:v:0", "-map", "1:a:0", localOutput)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg failed: %s, %w", string(output), err)
	}

	// Upload
	log.Printf("Uploading %s to %s...", localOutput, outputKey)
	f, err := os.Open(localOutput)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(outputKey),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	return nil
}

func downloadFile(client *s3.Client, bucket, key, dest string) error {
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	return err
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
