package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildRelativeKey(t *testing.T) {
	tests := []struct {
		name   string
		srcDir string
		srcKey string
		want   string
	}{
		{
			name:   "empty src dir returns src key",
			srcDir: "",
			srcKey: "foo/bar/baz.mp4",
			want:   "foo/bar/baz.mp4",
		},
		{
			name:   "exact directory match returns empty key",
			srcDir: "foo/bar",
			srcKey: "foo/bar",
			want:   "",
		},
		{
			name:   "child path trims strict directory prefix",
			srcDir: "foo/bar",
			srcKey: "foo/bar/baz.mp4",
			want:   "baz.mp4",
		},
		{
			name:   "non child path is left untouched",
			srcDir: "foo/bar",
			srcKey: "foo/bar2/baz.mp4",
			want:   "foo/bar2/baz.mp4",
		},
		{
			name:   "leading and trailing slashes are normalized",
			srcDir: "/foo/bar/",
			srcKey: "/foo/bar/baz.mp4",
			want:   "baz.mp4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildRelativeKey(tt.srcDir, tt.srcKey)
			if got != tt.want {
				t.Fatalf("buildRelativeKey(%q, %q) = %q, want %q", tt.srcDir, tt.srcKey, got, tt.want)
			}
		})
	}
}

func TestClassifyTransferResponseError(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		retryable bool
	}{
		{
			name:      "429 is retryable",
			status:    http.StatusTooManyRequests,
			body:      "rate limited",
			retryable: true,
		},
		{
			name:      "generic 502 is retryable",
			status:    http.StatusBadGateway,
			body:      "upstream failed",
			retryable: true,
		},
		{
			name:      "403 is fatal",
			status:    http.StatusForbidden,
			body:      "AccessDenied",
			retryable: false,
		},
		{
			name:      "500 missing env is fatal",
			status:    http.StatusInternalServerError,
			body:      "Error processing copy: Missing required environment variables",
			retryable: false,
		},
		{
			name:      "500 no such upload is fatal",
			status:    http.StatusInternalServerError,
			body:      "Error processing copy: Failed to upload to S3: 404 NoSuchUpload",
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyTransferResponseError(tt.status, tt.body)
			if got := isRetryableTransferError(err); got != tt.retryable {
				t.Fatalf("retryable = %v, want %v (err=%v)", got, tt.retryable, err)
			}
		})
	}
}

func TestClassifyTransferResponseError_StructuredJSON(t *testing.T) {
	body := `{"error":{"code":"DestNoSuchUpload","stage":"dest_put","message":"The specified upload does not exist.","retryable":false,"status_code":404}}`
	err := classifyTransferResponseError(http.StatusNotFound, body)
	if isRetryableTransferError(err) {
		t.Fatalf("expected structured JSON error to be fatal, got retryable: %v", err)
	}
}

func TestClassifyTransferResponseError_StructuredRetryableJSON(t *testing.T) {
	body := `{"error":{"code":"NetworkConnectionLost","stage":"copy_pipeline","message":"Network connection lost.","retryable":true,"status_code":502}}`
	err := classifyTransferResponseError(http.StatusBadGateway, body)
	if !isRetryableTransferError(err) {
		t.Fatalf("expected structured JSON error to be retryable, got: %v", err)
	}
}

func TestUploadTransferPartWithRetry_RetryableSecondChanceSucceeds(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			http.Error(w, "temporary upstream error", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"etag":"ok-etag"}`)
	}))
	defer server.Close()

	cfg = &Config{Storage: StorageConfig{TransferServiceURL: server.URL}}
	transferClient = server.Client()
	transferClient.Timeout = 2 * time.Second

	etag, err := uploadTransferPartWithRetry(context.Background(), "https://src.example/object", "https://dst.example/object", 123, 0, "upload", 1)
	if err != nil {
		t.Fatalf("uploadTransferPartWithRetry returned error: %v", err)
	}
	if etag != "ok-etag" {
		t.Fatalf("etag = %q, want ok-etag", etag)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("call count = %d, want 2", got)
	}
}

func TestUploadTransferPartWithRetry_FatalErrorStopsImmediately(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "Error processing copy: Missing required environment variables", http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg = &Config{Storage: StorageConfig{TransferServiceURL: server.URL}}
	transferClient = server.Client()
	transferClient.Timeout = 2 * time.Second

	_, err := uploadTransferPartWithRetry(context.Background(), "https://src.example/object", "https://dst.example/object", 123, 0, "upload", 1)
	if err == nil {
		t.Fatal("expected fatal error, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("call count = %d, want 1", got)
	}
}

func TestBuildSrcIdentityIncludesETagWhenAvailable(t *testing.T) {
	got := buildSrcIdentity("https://src.example/object", 123, `"etag-1"`)
	want := "https://src.example/object|123|etag-1"
	if got != want {
		t.Fatalf("buildSrcIdentity with etag = %q, want %q", got, want)
	}
}
