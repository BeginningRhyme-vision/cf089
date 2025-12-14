package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sirupsen/logrus"
)

var (
	serviceURL      string
	defaultPartSize int64 = 16 << 20 // 16MB
	ctx                   = context.Background()
	logger                = logrus.New()

	// Stats
	statsTotal       int64
	statsTransferred int64
	statsSkipped     int64
	statsDeleted     int64
	statsBytes       int64
)

type Object struct {
	Key    string
	RelKey string
	Size   int64
	ETag   string
}

type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
}

type ProgressReport struct {
	Type        string `json:"type"`
	Total       int64  `json:"total"`
	Transferred int64  `json:"transferred"`
	Skipped     int64  `json:"skipped"`
	Deleted     int64  `json:"deleted"`
	Bytes       int64  `json:"bytes"`
}

func main() {
	var transSrc string
	var transDest string
	var threads int
	var deleteSrc bool

	flag.StringVar(&transSrc, "trans_src", "", "Source R2 address (e.g. https://<account>.r2.cloudflarestorage.com/<bucket>/prefix)")
	flag.StringVar(&transDest, "trans_dest", "", "Destination S3 address (e.g. s3://<bucket>/prefix)")
	flag.StringVar(&serviceURL, "service-url", "http://localhost:8787/initiate-copy", "External HTTP service URL")
	flag.IntVar(&threads, "threads", 512, "Number of concurrent threads")
	flag.BoolVar(&deleteSrc, "delete_src", false, "Delete source file if it exists in destination with same size and etag")
	flag.Parse()

	if transSrc == "" || transDest == "" {
		fmt.Println("Usage: r2s3 -trans_src <R2_URL> -trans_dest <S3_URL> [-service-url <URL>] [-threads <N>] [-part_threads <N>]")
		os.Exit(1)
	}

	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	logger.SetLevel(logrus.InfoLevel)
	logger.SetOutput(os.Stderr) // Logs to stderr, Progress to stdout

	srcCfg, err := parseLocation(transSrc, "SOURCE_ACCESS_KEY_ID", "SOURCE_SECRET_ACCESS_KEY")
	if err != nil {
		logger.Fatalf("Parse source: %v", err)
	}

	dstCfg, err := parseLocation(transDest, "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
	if err != nil {
		logger.Fatalf("Parse dest: %v", err)
	}

	srcClient, err := newS3Client(ctx, srcCfg)
	if err != nil {
		logger.Fatalf("Create src client: %v", err)
	}
	dstClient, err := newS3Client(ctx, dstCfg)
	if err != nil {
		logger.Fatalf("Create dst client: %v", err)
	}

	// Start progress reporter
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			report := ProgressReport{
				Type:        "progress",
				Total:       atomic.LoadInt64(&statsTotal),
				Transferred: atomic.LoadInt64(&statsTransferred),
				Skipped:     atomic.LoadInt64(&statsSkipped),
				Deleted:     atomic.LoadInt64(&statsDeleted),
				Bytes:       atomic.LoadInt64(&statsBytes),
			}
			jsonBytes, _ := json.Marshal(report)
			fmt.Println(string(jsonBytes))
		}
	}()

	// Worker pool
	var wg sync.WaitGroup
	// limitCh is the global semaphore for active data transfers (small files or parts)
	limitCh := make(chan struct{}, threads*16)
	fileCh := make(chan struct{}, threads)

	// List objects
	srcCh := make(chan Object, 1000)
	dstCh := make(chan Object, 1000)
	go listObjects(ctx, srcClient, srcCfg, srcCh)
	go listObjects(ctx, dstClient, dstCfg, dstCh)
	jobs := make(chan Object, 1000)
	go func() {
		defer close(jobs)
		var dstObj *Object
		var ok bool

		// Initial read for dst
		val, open := <-dstCh
		if open {
			dstObj = &val
			ok = true
		}

		for srcObj := range srcCh {
			atomic.AddInt64(&statsTotal, 1)
			// We need to advance dstCh until dstObj.RelKey >= srcObj.RelKey
			for ok && dstObj.RelKey < srcObj.RelKey {
				// dstObj is extra (exists in dst but not src). We ignore it.
				val, open := <-dstCh
				if !open {
					ok = false
					dstObj = nil
				} else {
					dstObj = &val
				}
			}

			if ok && dstObj != nil && dstObj.RelKey == srcObj.RelKey {
				if dstObj.Size == srcObj.Size {
					logger.Debugf("Skipping %s: already exists", srcObj.RelKey)
					atomic.AddInt64(&statsSkipped, 1)
					if deleteSrc {
						wg.Add(1)
						fileCh <- struct{}{}
						go func(key string) {
							defer wg.Done()
							defer func() { <-fileCh }()
							deleteObject(srcClient, srcCfg.Bucket, key)
							atomic.AddInt64(&statsDeleted, 1)
						}(srcObj.Key)
					}
				} else {
					// Update needed
					jobs <- srcObj
				}
				// Move to next dst
				val, open := <-dstCh
				if !open {
					ok = false
					dstObj = nil
				} else {
					dstObj = &val
				}
			} else {
				// dstObj > srcObj OR dstCh exhausted.
				// srcObj is new.
				jobs <- srcObj
			}
		}
	}()

	for {
		obj, ok := <-jobs
		if !ok {
			break
		}
		wg.Add(1)
		go func(o Object) {
			defer wg.Done()
			fileCh <- struct{}{}
			defer func() { <-fileCh }()
			err := processObject(srcClient, dstClient, srcCfg, dstCfg, o, limitCh, deleteSrc)
			if err != nil {
				logger.Errorf("Failed to process %s: %v", o.Key, err)
			} else {
				atomic.AddInt64(&statsTransferred, 1)
				atomic.AddInt64(&statsBytes, o.Size)
				if deleteSrc {
					atomic.AddInt64(&statsDeleted, 1)
				}
			}
		}(obj)
	}
	wg.Wait()
}

func parseLocation(uri string, akEnv, skEnv string) (*S3Config, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	cfg := &S3Config{
		Region: "auto", // Default
	}

	// Handle User Info for credentials
	if u.User != nil {
		cfg.AccessKeyID = u.User.Username()
		cfg.SecretAccessKey, _ = u.User.Password()
	}

	if cfg.AccessKeyID == "" {
		cfg.AccessKeyID = os.Getenv(akEnv)
	}
	if cfg.SecretAccessKey == "" {
		cfg.SecretAccessKey = os.Getenv(skEnv)
	}

	if u.Scheme == "s3" {
		parts := strings.SplitN(u.Host, ".", 2)
		if len(parts) == 2 {
			cfg.Bucket = parts[0]
			cfg.Endpoint = "https://" + parts[1]
		} else {
			cfg.Bucket = u.Host
		}
		cfg.Prefix = strings.TrimPrefix(u.Path, "/")
	} else if u.Scheme == "http" || u.Scheme == "https" {
		// https://endpoint/bucket/prefix
		// For R2: https://<account>.r2.cloudflarestorage.com/<bucket>
		// We need to separate bucket from endpoint if possible, or use Virtual Host style.
		// A simple heuristic: first part of path is bucket?
		// Or the endpoint includes the bucket?
		// R2 pattern: <account>.r2.cloudflarestorage.com is endpoint. Path starts with bucket.

		cfg.Endpoint = u.Scheme + "://" + u.Host
		parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
		if len(parts) > 0 {
			cfg.Bucket = parts[0]
		}
		if len(parts) > 1 {
			cfg.Prefix = parts[1]
		}
	} else {
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	return cfg, nil
}

func newS3Client(ctx context.Context, cfg *S3Config) (*s3.Client, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = false // Often safer for generic S3/MinIO/R2
	})

	return client, nil
}

func listObjects(ctx context.Context, client *s3.Client, cfg *S3Config, out chan<- Object) {
	defer close(out)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.Bucket),
	}
	if cfg.Prefix != "" {
		input.Prefix = aws.String(cfg.Prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			logger.Errorf("List objects failed: %v", err)
			return
		}

		for _, item := range page.Contents {
			if strings.HasSuffix(*item.Key, "/") {
				continue // Skip directories if needed
			}

			etag := ""
			if item.ETag != nil {
				etag = strings.ToLower(strings.Trim(*item.ETag, "\""))
			}

			relKey := *item.Key
			if cfg.Prefix != "" {
				if *item.Key == cfg.Prefix {
					parts := strings.Split(*item.Key, "/")
					relKey = parts[len(parts)-1]
				} else if strings.HasPrefix(*item.Key, cfg.Prefix) {
					relKey = strings.TrimPrefix(*item.Key, cfg.Prefix)
					relKey = strings.TrimPrefix(relKey, "/")
				}
			}

			out <- Object{
				Key:    *item.Key,
				RelKey: relKey,
				Size:   *item.Size,
				ETag:   etag,
			}
		}
	}
}

func processObject(src, dst *s3.Client, srcCfg, dstCfg *S3Config, obj Object, limitCh chan struct{}, deleteSrc bool) error {
	dstKey := obj.RelKey
	if dstCfg.Prefix != "" {
		dstKey = fmt.Sprintf("%s%s", dstCfg.Prefix, dstKey)
		dstKey = strings.TrimPrefix(dstKey, "/")
	}

	var r2Key string
	if srcCfg.Endpoint != "" {
		u, _ := url.Parse(srcCfg.Endpoint)
		r2Key = fmt.Sprintf("%s://%s.%s/%s", u.Scheme, srcCfg.Bucket, u.Host, obj.Key)
	} else {
		panic("no src endpoint")
	}

	var s3Url string
	if dstCfg.Endpoint != "" {
		u, _ := url.Parse(dstCfg.Endpoint)
		s3Url = fmt.Sprintf("%s://%s.%s/%s", u.Scheme, dstCfg.Bucket, u.Host, dstKey)
	} else {
		panic("no dst endpoint")
	}

	logger.Debugf("Transferring %s -> %s(%s) (%d bytes)", obj.Key, dstKey, s3Url, obj.Size)
	if obj.Size < defaultPartSize {
		limitCh <- struct{}{}
		defer func() { <-limitCh }()
		// var newETag string
		err := try(3, func() error {
			_, err := callExternalService(r2Key, s3Url, obj.Size, 0, "", -1)
			return err
		})
		if err != nil {
			return fmt.Errorf("direct copy failed: %v", err)
		}
		if deleteSrc {
			deleteObject(src, srcCfg.Bucket, obj.Key)
		}
		logger.Debugf("Completed %s", dstKey)
		return nil
	}

	// Initiate Multipart Upload on Destination
	createOutput, err := dst.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(dstCfg.Bucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %v", err)
	}
	uploadID := *createOutput.UploadId

	// Calculate parts
	partSize := calculatePartSize(obj.Size)
	numParts := int((obj.Size-1)/partSize) + 1

	parts := make([]types.CompletedPart, numParts)
	errs := make(chan error, numParts)
	var wg sync.WaitGroup

	for i := 0; i < numParts; i++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			limitCh <- struct{}{}
			defer func() { <-limitCh }()

			off := int64(partNum) * partSize
			pSize := partSize
			if partNum == numParts-1 {
				pSize = obj.Size - off
			}

			// Call external service
			var etag string
			err := try(3, func() error {
				var err error
				etag, err = callExternalService(r2Key, s3Url, pSize, off, uploadID, partNum+1)
				return err
			})
			if err != nil {
				errs <- fmt.Errorf("part %d: %v", partNum+1, err)
				return
			}

			parts[partNum] = types.CompletedPart{
				PartNumber: aws.Int32(int32(partNum + 1)),
				ETag:       aws.String(etag),
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	if len(errs) > 0 {
		_, _ = dst.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(dstCfg.Bucket),
			Key:      aws.String(dstKey),
			UploadId: aws.String(uploadID),
		})
		return <-errs
	}

	// Complete Multipart Upload
	_, err = dst.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(dstCfg.Bucket),
		Key:      aws.String(dstKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return fmt.Errorf("complete upload: %v", err)
	}
	if deleteSrc {
		deleteObject(src, srcCfg.Bucket, obj.Key)
	}

	logger.Debugf("Completed %s", dstKey)
	return nil
}

// Helpers

func calculatePartSize(size int64) int64 {
	partSize := defaultPartSize
	// Max parts is 10000 for S3
	if size > partSize*10000 {
		partSize = size / 10000
		partSize = ((partSize-1)>>20 + 1) << 20 // align to MB
	}
	return partSize
}

func try(n int, f func() error) (err error) {
	for i := 0; i < n; i++ {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(i+1))
	}
	return
}

type ServiceRequest struct {
	R2Key      string `json:"r2Key"`
	S3Url      string `json:"s3Url"`
	Size       int64  `json:"size"`
	Offset     int64  `json:"offset"`
	UploadID   string `json:"uploadId"`
	PartNumber int    `json:"partNumber"`
}

type ServiceResponse struct {
	Message string `json:"message"`
	ETag    string `json:"etag"`
}

func callExternalService(r2Key, s3Url string, size, offset int64, uploadID string, partNumber int) (string, error) {
	reqBody := ServiceRequest{
		R2Key:      r2Key,
		S3Url:      s3Url,
		Size:       size,
		Offset:     offset,
		UploadID:   uploadID,
		PartNumber: partNumber,
	}

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	resp, err := http.Post(serviceURL, "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("service status %d: %s", resp.StatusCode, string(body))
	}

	var serviceResp ServiceResponse
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(body, &serviceResp); err != nil {
		return "", fmt.Errorf("decode response: %v", err)
	}

	if serviceResp.ETag == "" {
		return "", fmt.Errorf("no etag in response: %s", string(body))
	}

	return serviceResp.ETag, nil
}

func deleteObject(client *s3.Client, bucket, key string) {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		logger.Errorf("Failed to delete source object %s: %v", key, err)
	} else {
		logger.Debugf("Deleted source object %s as it matches destination", key)
	}
}
