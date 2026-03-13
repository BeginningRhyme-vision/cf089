package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config from config.yaml
const (
	Endpoint  = "https://6d18700b4bd3fff2b330035c35b0bbeb.r2.cloudflarestorage.com/tmp-transfer2"
	AccessKey = "6e17dce5e6699ec405f7ec07deecf321"
	SecretKey = "4916026eb627351979f06b898b4f8f67137caba46c39bab83934eb29a97f92ad"
	Region    = "auto"
	Prefix    = "video/videotestytbup1215a/c21125ytbups_newpath/"
)

func main() {
	// 1. Parse Endpoint to separate BaseURL and Bucket
	u, err := http.NewRequest("GET", Endpoint, nil)
	if err != nil {
		log.Fatalf("Invalid endpoint: %v", err)
	}

	// Logic from scanner.go (new)
	baseEndpoint := fmt.Sprintf("%s://%s", u.URL.Scheme, u.URL.Host)
	bucketName := strings.Trim(u.URL.Path, "/")

	fmt.Printf("Base Endpoint: %s\n", baseEndpoint)
	fmt.Printf("Bucket: %s\n", bucketName)
	fmt.Printf("Prefix: %s\n", Prefix)

	// 2. Init S3 Client
	c, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			AccessKey,
			SecretKey,
			"",
		)),
		awsconfig.WithRegion(Region),
	)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	s3Client := s3.NewFromConfig(c, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = false // As per recent change
	})

	// 3. List Objects
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(Prefix),
	})

	// Test Filter Pattern
	includePattern := "*.mp4"
	excludePattern := ""

	fmt.Printf("Testing Filters - Include: '%s', Exclude: '%s'\n", includePattern, excludePattern)

	count := 0
	matchedCount := 0
	
	// Helper function for matching (PROPOSED FIX)
	match := func(pattern, name string) (bool, error) {
		if strings.Contains(pattern, "/") {
			return path.Match(pattern, name)
		}
		return path.Match(pattern, path.Base(name))
	}

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("List failed: %v", err)
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			
			// Logic from scanner.go
			isIncluded := true
			
			if includePattern != "" {
				matched, err := match(includePattern, key)
				if err == nil && !matched {
					isIncluded = false
				}
			}
			if excludePattern != "" {
				matched, err := match(excludePattern, key)
				if err == nil && matched {
					isIncluded = false
				}
			}

			if count < 20 { // Limit output
				fmt.Printf("Found: %s (Size: %d) -> Included: %v\n", key, obj.Size, isIncluded)
			}
			count++
			if isIncluded {
				matchedCount++
			}
		}
		// Stop after first page for testing
		break 
	}

	fmt.Printf("Total objects checked (first page): %d. Matched: %d\n", count, matchedCount)
}
