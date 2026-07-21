package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestIsRetryableListPartsError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "context deadline exceeded is retryable",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "wrapped timeout is retryable",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.com",
				Err: context.DeadlineExceeded,
			},
			want: true,
		},
		{
			name: "503 response is retryable",
			err:  errors.New("operation error S3: ListParts, https response error StatusCode: 503"),
			want: true,
		},
		{
			name: "connection reset is retryable",
			err:  errors.New("read tcp 10.0.0.1:12345->10.0.0.2:443: connection reset by peer"),
			want: true,
		},
		{
			name: "403 access denied is not retryable",
			err:  errors.New("operation error S3: ListParts, https response error StatusCode: 403, api error AccessDenied"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableListPartsError(tt.err); got != tt.want {
				t.Fatalf("isRetryableListPartsError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestListPartsRetryBackoff(t *testing.T) {
	tests := []struct {
		retryNumber int
		want        time.Duration
	}{
		{retryNumber: 1, want: 3 * time.Second},
		{retryNumber: 2, want: 9 * time.Second},
		{retryNumber: 0, want: 3 * time.Second},
	}

	for _, tt := range tests {
		if got := listPartsRetryBackoff(tt.retryNumber); got != tt.want {
			t.Fatalf("listPartsRetryBackoff(%d) = %s, want %s", tt.retryNumber, got, tt.want)
		}
	}
}

func TestTransferAcquireEmptyBackoffRange(t *testing.T) {
	minBackoff, maxBackoff := getTransferAcquireEmptyBackoffRange()

	for i := 0; i < 200; i++ {
		got := transferAcquireEmptyBackoff()
		if got < minBackoff {
			t.Fatalf("backoff=%s below min=%s", got, minBackoff)
		}
		if got >= maxBackoff {
			t.Fatalf("backoff=%s should stay below max=%s", got, maxBackoff)
		}
	}
}

func TestTransferAcquireEmptyBackoffRangeFromEnv(t *testing.T) {
	t.Setenv("TRANSFER_ACQUIRE_EMPTY_BACKOFF_MIN_MS", "300")
	t.Setenv("TRANSFER_ACQUIRE_EMPTY_BACKOFF_MAX_MS", "1000")

	minBackoff, maxBackoff := getTransferAcquireEmptyBackoffRange()
	if minBackoff != 300*time.Millisecond {
		t.Fatalf("min backoff=%s, want %s", minBackoff, 300*time.Millisecond)
	}
	if maxBackoff != 1000*time.Millisecond {
		t.Fatalf("max backoff=%s, want %s", maxBackoff, 1000*time.Millisecond)
	}

	for i := 0; i < 200; i++ {
		got := transferAcquireEmptyBackoff()
		if got < minBackoff {
			t.Fatalf("env backoff=%s below min=%s", got, minBackoff)
		}
		if got >= maxBackoff {
			t.Fatalf("env backoff=%s should stay below max=%s", got, maxBackoff)
		}
	}
}

func TestTransferAcquireErrorBackoffFromEnv(t *testing.T) {
	if got := transferAcquireErrorBackoff(); got != 2*time.Second {
		t.Fatalf("default error backoff=%s, want %s", got, 2*time.Second)
	}

	t.Setenv("TRANSFER_ACQUIRE_ERROR_BACKOFF_MS", "2500")
	if got := transferAcquireErrorBackoff(); got != 2500*time.Millisecond {
		t.Fatalf("env error backoff=%s, want %s", got, 2500*time.Millisecond)
	}
}

func TestGetEnvBoolTransferAllowZeroSizeFile(t *testing.T) {
	t.Setenv("TRANSFER_ALLOW_ZERO_SIZE_FILE", "false")
	if got := getEnvBool("TRANSFER_ALLOW_ZERO_SIZE_FILE", true); got {
		t.Fatal("expected false when TRANSFER_ALLOW_ZERO_SIZE_FILE=false")
	}

	t.Setenv("TRANSFER_ALLOW_ZERO_SIZE_FILE", "true")
	if got := getEnvBool("TRANSFER_ALLOW_ZERO_SIZE_FILE", false); !got {
		t.Fatal("expected true when TRANSFER_ALLOW_ZERO_SIZE_FILE=true")
	}

	t.Setenv("TRANSFER_ALLOW_ZERO_SIZE_FILE", "invalid")
	if got := getEnvBool("TRANSFER_ALLOW_ZERO_SIZE_FILE", true); !got {
		t.Fatal("expected invalid boolean env to fall back to default=true")
	}
}
