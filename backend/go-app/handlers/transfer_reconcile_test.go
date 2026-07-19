package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func TestParseTransferCompletionPendingMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		member    string
		wantJobID int64
		wantTask  int64
		wantToken string
		wantErr   bool
	}{
		{name: "valid", member: "12:34:token-1", wantJobID: 12, wantTask: 34, wantToken: "token-1"},
		{name: "missing token", member: "12:34:", wantErr: true},
		{name: "bad job id", member: "x:34:token", wantErr: true},
		{name: "bad task id", member: "12:y:token", wantErr: true},
		{name: "too short", member: "12:34", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobID, taskID, runToken, err := parseTransferCompletionPendingMember(tt.member)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for member %q", tt.member)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if jobID != tt.wantJobID || taskID != tt.wantTask || runToken != tt.wantToken {
				t.Fatalf("unexpected parse result: got (%d,%d,%q)", jobID, taskID, runToken)
			}
		})
	}
}

func TestBuildTransferDestinationKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		srcDir string
		dstDir string
		srcKey string
		want   string
	}{
		{name: "nested child", srcDir: "foo/bar", dstDir: "backup", srcKey: "foo/bar/a.txt", want: "backup/a.txt"},
		{name: "exact dir maps to dst dir", srcDir: "foo/bar", dstDir: "backup", srcKey: "foo/bar", want: "backup"},
		{name: "no dst dir", srcDir: "foo/bar", dstDir: "", srcKey: "foo/bar/a/b.txt", want: "a/b.txt"},
		{name: "prefix lookalike should not trim", srcDir: "foo/bar", dstDir: "backup", srcKey: "foo/bar2/a.txt", want: "backup/foo/bar2/a.txt"},
		{name: "empty src dir", srcDir: "", dstDir: "backup", srcKey: "a/b.txt", want: "backup/a/b.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildTransferDestinationKey(tt.dstDir, tt.srcDir, tt.srcKey)
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestBuildTransferReconcileBaseEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "s3 bucket host strips bucket prefix",
			endpoint: "s3://crawl-data.tos-s3-cn-shanghai.volces.com",
			want:     "http://tos-s3-cn-shanghai.volces.com",
		},
		{
			name:     "https endpoint preserved",
			endpoint: "https://tos-s3-cn-shanghai.volces.com/bucket-name",
			want:     "https://tos-s3-cn-shanghai.volces.com",
		},
		{
			name:     "bare host defaults to http",
			endpoint: "tos-s3-cn-shanghai.volces.com",
			want:     "http://tos-s3-cn-shanghai.volces.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildTransferReconcileBaseEndpoint(tt.endpoint)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestGetBucketFromEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "s3 bucket host",
			endpoint: "s3://crawl-data.tos-s3-cn-shanghai.volces.com",
			want:     "crawl-data",
		},
		{
			name:     "https path style bucket",
			endpoint: "https://tos-s3-cn-shanghai.volces.com/crawl-data",
			want:     "crawl-data",
		},
		{
			name:     "invalid endpoint returns empty",
			endpoint: "://bad-endpoint",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBucketFromEndpoint(tt.endpoint); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestIsTransferReconcileNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errText string
		want    bool
	}{
		{errText: "operation error S3: HeadObject, https response error StatusCode: 404", want: true},
		{errText: "NoSuchKey: object missing", want: true},
		{errText: "random timeout", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.errText, func(t *testing.T) {
			err := testError(tt.errText)
			if got := isTransferReconcileNotFound(err); got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestUpdateTransferTaskProgressDoesNotCreateLastSeenWithoutActivate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       21,
		JobID:    8,
		Src:      "foo/bar/a.mp4",
		RunToken: "run-activate-1",
		Status:   "RUNNING",
		WorkerID: "worker-a",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	reqBody := map[string]interface{}{
		"job_id":    task.JobID,
		"task_id":   task.ID,
		"run_token": task.RunToken,
	}
	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/progress", mustJSONBody(t, reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	UpdateTransferTaskProgress(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	member := transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken)
	if client.ZScore(ctx, transferRunningLastSeenKey(), member).Err() != redis.Nil {
		t.Fatal("progress should not create last_seen member before activation")
	}
}

func TestTransferResumeCandidateHelpers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	pipe := client.Pipeline()
	setTransferResumeCandidate(pipe, ctx, 501, 601)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("set resume candidate: %v", err)
	}

	hasCandidate, err := hasTransferResumeCandidate(ctx, 501, 601)
	if err != nil {
		t.Fatalf("has candidate: %v", err)
	}
	if !hasCandidate {
		t.Fatal("expected resume candidate to exist")
	}

	count, err := countPendingTransferResumeCandidates(ctx)
	if err != nil {
		t.Fatalf("count pending candidates: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected pending candidate count: got %d want 1", count)
	}

	pipe = client.Pipeline()
	clearTransferResumeCandidate(pipe, ctx, 501, 601)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("clear resume candidate: %v", err)
	}

	count, err = countPendingTransferResumeCandidates(ctx)
	if err != nil {
		t.Fatalf("count pending candidates after clear: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected cleared resume candidate to disappear from backlog count, got %d", count)
	}
}

func TestTransferAutoRetryScheduleHelpers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	dueAt := time.Now().Add(2 * time.Minute)
	pipe := client.Pipeline()
	setTransferAutoRetryScheduled(pipe, ctx, 700, 800, dueAt)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("set auto retry schedule: %v", err)
	}

	scheduled, err := isTransferAutoRetryScheduled(ctx, 700, 800)
	if err != nil {
		t.Fatalf("check auto retry scheduled: %v", err)
	}
	if !scheduled {
		t.Fatal("expected task to be marked as auto retry scheduled")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(700, 800)).Err() != nil {
		t.Fatal("expected task to be present in auto retry due queue")
	}

	pipe = client.Pipeline()
	clearTransferAutoRetrySchedule(pipe, ctx, 700, 800)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("clear auto retry schedule: %v", err)
	}

	scheduled, err = isTransferAutoRetryScheduled(ctx, 700, 800)
	if err != nil {
		t.Fatalf("re-check auto retry scheduled: %v", err)
	}
	if scheduled {
		t.Fatal("expected auto retry scheduled marker to be removed")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(700, 800)).Err() != redis.Nil {
		t.Fatal("expected auto retry due member to be removed")
	}
}

func TestReleaseTransferAutoRetryLockKeepsForeignOwner(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	lockKey := transferAutoRetryLockKey()
	if err := client.Set(ctx, lockKey, "owner-b", time.Minute).Err(); err != nil {
		t.Fatalf("seed foreign lock owner: %v", err)
	}

	if err := releaseTransferAutoRetryLock(ctx, "owner-a"); err != nil {
		t.Fatalf("release foreign lock owner: %v", err)
	}
	value, err := client.Get(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("load lock after foreign release: %v", err)
	}
	if value != "owner-b" {
		t.Fatalf("foreign lock owner should be preserved, got %q", value)
	}

	if err := releaseTransferAutoRetryLock(ctx, "owner-b"); err != nil {
		t.Fatalf("release matching lock owner: %v", err)
	}
	if client.Exists(ctx, lockKey).Val() != 0 {
		t.Fatal("matching lock owner should be able to release lock")
	}
}

func TestResolveTransferAutoRetryManualStateClearsScheduledMarkerWhenDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("TRANSFER_AUTO_RETRY_FAILED_ENABLED", "false")

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	pipe := client.Pipeline()
	setTransferAutoRetryScheduled(pipe, ctx, 710, 810, time.Now().Add(time.Minute))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed auto retry schedule: %v", err)
	}

	pipe = client.Pipeline()
	skip, err := resolveTransferAutoRetryManualState(ctx, 710, 810, pipe)
	if err != nil {
		t.Fatalf("resolve manual auto retry state: %v", err)
	}
	if skip {
		t.Fatal("manual retry should not be skipped when auto retry is disabled")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("clear disabled auto retry schedule: %v", err)
	}

	scheduled, err := isTransferAutoRetryScheduled(ctx, 710, 810)
	if err != nil {
		t.Fatalf("check scheduled marker after disabled resolution: %v", err)
	}
	if scheduled {
		t.Fatal("disabled auto retry should clear stale scheduled marker for manual retry")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(710, 810)).Err() != redis.Nil {
		t.Fatal("disabled auto retry should clear stale due member for manual retry")
	}
}

func TestRunTransferAutoRetryBatchReleasesSchedulerLock(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("TRANSFER_AUTO_RETRY_FAILED_ENABLED", "true")

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	runTransferAutoRetryBatch()

	if client.Exists(ctx, transferAutoRetryLockKey()).Val() != 0 {
		t.Fatal("scheduler lock should be released after batch completes")
	}
}

func TestResolveTransferAutoRetryManualStateSkipsScheduledTaskWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("TRANSFER_AUTO_RETRY_FAILED_ENABLED", "true")

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	pipe := client.Pipeline()
	setTransferAutoRetryScheduled(pipe, ctx, 903, 904, time.Now().Add(time.Minute))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed auto retry schedule: %v", err)
	}

	pipe = client.Pipeline()
	skip, err := resolveTransferAutoRetryManualState(ctx, 903, 904, pipe)
	if err != nil {
		t.Fatalf("resolve enabled auto retry manual state: %v", err)
	}
	if !skip {
		t.Fatal("manual retry should skip task when auto retry remains enabled and scheduled")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("exec no-op pipeline for enabled auto retry: %v", err)
	}

	scheduled, err := isTransferAutoRetryScheduled(ctx, 903, 904)
	if err != nil {
		t.Fatalf("check scheduled marker after enabled resolution: %v", err)
	}
	if !scheduled {
		t.Fatal("manual retry should not clear auto retry schedule for skipped task")
	}
}

func TestTransferAcquireCapacityRules(t *testing.T) {
	t.Parallel()

	if got := computeTransferDefaultCap(512, 128, 10); got != 384 {
		t.Fatalf("default cap with resume backlog: got %d want 384", got)
	}
	if got := computeTransferDefaultCap(512, 128, 0); got != 512 {
		t.Fatalf("default cap without resume backlog: got %d want 512", got)
	}
	if canClaimTransferTask(TransferPoolDefault, 512, 384, 0, 10, 128) {
		t.Fatal("default pool should stop at reserved boundary while resume backlog exists")
	}
	if !canClaimTransferTask(TransferPoolResume, 512, 250, 200, 10, 128) {
		t.Fatal("resume pool should be able to borrow idle slots")
	}
	if canClaimTransferTask(TransferPoolResume, 512, 300, 212, 10, 128) {
		t.Fatal("resume pool should stop when total inflight reaches max workers")
	}
}

func TestShouldStopTransferAcquireAfterDeferred(t *testing.T) {
	t.Parallel()

	if shouldStopTransferAcquireAfterDeferred(127, 1024) {
		t.Fatal("deferred streak below the soft limit should keep scanning")
	}
	if !shouldStopTransferAcquireAfterDeferred(128, 1024) {
		t.Fatal("deferred streak at the soft limit should stop scanning")
	}
	if shouldStopTransferAcquireAfterDeferred(9, 10) {
		t.Fatal("small request limits should not stop before reaching the request size")
	}
	if !shouldStopTransferAcquireAfterDeferred(10, 10) {
		t.Fatal("request limit smaller than the soft limit should clamp the stop threshold")
	}
	if shouldStopTransferAcquireAfterDeferred(127, 0) {
		t.Fatal("non-positive request limits should fall back to the global soft limit")
	}
	if !shouldStopTransferAcquireAfterDeferred(128, 0) {
		t.Fatal("global soft limit should still apply when request limit is non-positive")
	}
}

func TestTransferTaskCanUseCheckpoint(t *testing.T) {
	t.Setenv("TRANSFER_MULTIPART_THRESHOLD_MB", "8")
	t.Setenv("TRANSFER_MIN_PART_SIZE_MB", "5")

	job := models.TransferJob{
		JobID:  70,
		SrcDir: "src/root",
		DstDir: "dst/root",
		Metadata: models.TransferMetadata{
			Endpoint: "s3://target-bucket.tos-s3-cn-shanghai.volces.com",
		},
	}
	task := models.TransferTask{
		ID:    101,
		JobID: 70,
		Src:   "src/root/a/b.bin",
		Size:  64 * 1024 * 1024,
	}

	partSize := calculateTransferPartSize(task.Size)
	checkpoint := &transferMultipartCheckpoint{
		JobID:     task.JobID,
		TaskID:    task.ID,
		Src:       task.Src,
		Size:      task.Size,
		UploadID:  "upload-1",
		DstBucket: "target-bucket",
		DstKey:    "dst/root/a/b.bin",
		PartSize:  partSize,
		NumParts:  int((task.Size-1)/partSize) + 1,
	}

	if !transferTaskCanUseCheckpoint(job, task, checkpoint) {
		t.Fatal("expected matching checkpoint to remain eligible for resume pool")
	}

	mismatch := *checkpoint
	mismatch.PartSize = checkpoint.PartSize + 1
	if transferTaskCanUseCheckpoint(job, task, &mismatch) {
		t.Fatal("checkpoint with mismatched part size should not stay in resume pool")
	}

	mismatch = *checkpoint
	mismatch.DstKey = "dst/root/other.bin"
	if transferTaskCanUseCheckpoint(job, task, &mismatch) {
		t.Fatal("checkpoint with mismatched destination key should not stay in resume pool")
	}
}

func TestTransferPoolInFlightRemovalKeepsOtherRunToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	poolKey := transferPoolInFlightKey(TransferPoolResume)
	if err := client.SAdd(ctx, poolKey,
		transferLegacyPoolInFlightMember(88, 99),
		transferPoolInFlightMember(88, 99, "run-old"),
		transferPoolInFlightMember(88, 99, "run-new"),
	).Err(); err != nil {
		t.Fatalf("seed inflight members: %v", err)
	}

	pipe := client.Pipeline()
	removeTransferTaskFromAllPoolInFlight(pipe, ctx, 88, 99, "run-old")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("remove old inflight member: %v", err)
	}

	if client.SIsMember(ctx, poolKey, transferPoolInFlightMember(88, 99, "run-old")).Val() {
		t.Fatal("old run_token inflight member should be removed")
	}
	if client.SIsMember(ctx, poolKey, transferLegacyPoolInFlightMember(88, 99)).Val() {
		t.Fatal("legacy inflight member should be removed for compatibility cleanup")
	}
	if !client.SIsMember(ctx, poolKey, transferPoolInFlightMember(88, 99, "run-new")).Val() {
		t.Fatal("new run_token inflight member should be preserved")
	}
}

func TestRollbackClaimedTransferTaskRestoresPendingState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	original := models.TransferTask{
		ID:       91,
		JobID:    81,
		Src:      "foo/bar/restore.bin",
		Status:   "PENDING",
		RunToken: "",
		WorkerID: "",
	}
	claimed := original
	claimed.Status = "RUNNING"
	claimed.RunToken = "run-restore-1"
	claimed.WorkerID = "worker-restore"

	taskKey := fmt.Sprintf("tx:task:%d:%d", original.JobID, original.ID)
	claimedJSON, err := json.Marshal(claimed)
	if err != nil {
		t.Fatalf("marshal claimed task: %v", err)
	}
	if err := client.Set(ctx, taskKey, claimedJSON, 0).Err(); err != nil {
		t.Fatalf("seed claimed task: %v", err)
	}
	if err := client.SAdd(ctx, transferPoolInFlightKey(TransferPoolResume), transferPoolInFlightMember(claimed.JobID, claimed.ID, claimed.RunToken)).Err(); err != nil {
		t.Fatalf("seed resume inflight: %v", err)
	}
	if err := client.ZAdd(ctx, transferClaimedRunningKey(), redis.Z{
		Score:  1,
		Member: transferClaimedRunningMember(claimed.JobID, claimed.ID, claimed.RunToken),
	}).Err(); err != nil {
		t.Fatalf("seed claimed running: %v", err)
	}

	if err := rollbackClaimedTransferTask(ctx, taskKey, original, claimed, true); err != nil {
		t.Fatalf("rollback claim: %v", err)
	}

	raw, err := client.Get(ctx, taskKey).Result()
	if err != nil {
		t.Fatalf("load restored task: %v", err)
	}
	var restored models.TransferTask
	if err := json.Unmarshal([]byte(raw), &restored); err != nil {
		t.Fatalf("decode restored task: %v", err)
	}
	if restored.Status != "PENDING" {
		t.Fatalf("expected restored status PENDING, got %s", restored.Status)
	}
	if restored.RunToken != "" {
		t.Fatalf("expected restored run token to be empty, got %q", restored.RunToken)
	}
	if restored.WorkerID != "" {
		t.Fatalf("expected restored worker id to be empty, got %q", restored.WorkerID)
	}
	if client.SIsMember(ctx, transferPoolInFlightKey(TransferPoolResume), transferPoolInFlightMember(claimed.JobID, claimed.ID, claimed.RunToken)).Val() {
		t.Fatal("rollback should remove claimed inflight member")
	}
	if client.ZScore(ctx, transferClaimedRunningKey(), transferClaimedRunningMember(claimed.JobID, claimed.ID, claimed.RunToken)).Err() != redis.Nil {
		t.Fatal("rollback should remove claimed running member")
	}
	hasCandidate, err := hasTransferResumeCandidate(ctx, original.JobID, original.ID)
	if err != nil {
		t.Fatalf("check restored resume candidate: %v", err)
	}
	if !hasCandidate {
		t.Fatal("rollback should restore resume candidate for resume pool claims")
	}
}

func TestCleanupTransferJobRuntimeStateRemovesRuntimeMarkers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	jobID := int64(77)
	taskID := int64(88)
	runToken := "run-77"

	if err := client.Set(ctx, transferResumeCandidateKey(jobID, taskID), "1", time.Hour).Err(); err != nil {
		t.Fatalf("seed resume candidate key: %v", err)
	}
	if err := client.ZAdd(ctx, transferResumeCandidateIndexKey(), redis.Z{
		Score:  float64(time.Now().Add(time.Hour).Unix()),
		Member: transferResumeCandidateMember(jobID, taskID),
	}).Err(); err != nil {
		t.Fatalf("seed resume candidate index: %v", err)
	}
	if err := client.ZAdd(ctx, transferClaimedRunningKey(), redis.Z{Score: 1, Member: transferClaimedRunningMember(jobID, taskID, runToken)}).Err(); err != nil {
		t.Fatalf("seed claimed running: %v", err)
	}
	if err := client.ZAdd(ctx, transferRunningLastSeenKey(), redis.Z{Score: 1, Member: transferRunningLastSeenMember(jobID, taskID, runToken)}).Err(); err != nil {
		t.Fatalf("seed running last seen: %v", err)
	}
	if err := client.ZAdd(ctx, transferCompletionPendingKey(), redis.Z{Score: 1, Member: transferCompletionPendingMember(jobID, taskID, runToken)}).Err(); err != nil {
		t.Fatalf("seed completion pending: %v", err)
	}
	if err := client.Set(ctx, transferCompletionCompensationKey(jobID, taskID, runToken), "{}", time.Hour).Err(); err != nil {
		t.Fatalf("seed completion comp: %v", err)
	}
	if err := client.Set(ctx, transferAutoRetryScheduledKey(jobID, taskID), "1", time.Hour).Err(); err != nil {
		t.Fatalf("seed auto retry scheduled: %v", err)
	}
	if err := client.ZAdd(ctx, transferAutoRetryDueKey(), redis.Z{
		Score:  float64(time.Now().Add(time.Hour).Unix()),
		Member: transferAutoRetryMember(jobID, taskID),
	}).Err(); err != nil {
		t.Fatalf("seed auto retry due: %v", err)
	}
	if err := client.SAdd(ctx, transferPoolInFlightKey(TransferPoolDefault), transferPoolInFlightMember(jobID, taskID, runToken)).Err(); err != nil {
		t.Fatalf("seed default inflight: %v", err)
	}
	if err := client.SAdd(ctx, transferPoolInFlightKey(TransferPoolResume), transferLegacyPoolInFlightMember(jobID, taskID)).Err(); err != nil {
		t.Fatalf("seed legacy resume inflight: %v", err)
	}

	pipe := client.Pipeline()
	cleanupTransferJobRuntimeState(ctx, pipe, jobID, []int64{taskID})
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("cleanup runtime state: %v", err)
	}

	if client.Exists(ctx, transferResumeCandidateKey(jobID, taskID)).Val() != 0 {
		t.Fatal("resume candidate key should be removed")
	}
	if client.ZScore(ctx, transferResumeCandidateIndexKey(), transferResumeCandidateMember(jobID, taskID)).Err() != redis.Nil {
		t.Fatal("resume candidate index member should be removed")
	}
	if client.ZScore(ctx, transferClaimedRunningKey(), transferClaimedRunningMember(jobID, taskID, runToken)).Err() != redis.Nil {
		t.Fatal("claimed running member should be removed")
	}
	if client.ZScore(ctx, transferRunningLastSeenKey(), transferRunningLastSeenMember(jobID, taskID, runToken)).Err() != redis.Nil {
		t.Fatal("running last seen member should be removed")
	}
	if client.ZScore(ctx, transferCompletionPendingKey(), transferCompletionPendingMember(jobID, taskID, runToken)).Err() != redis.Nil {
		t.Fatal("completion pending member should be removed")
	}
	if client.Exists(ctx, transferCompletionCompensationKey(jobID, taskID, runToken)).Val() != 0 {
		t.Fatal("completion compensation key should be removed")
	}
	if client.Exists(ctx, transferAutoRetryScheduledKey(jobID, taskID)).Val() != 0 {
		t.Fatal("auto retry scheduled key should be removed")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(jobID, taskID)).Err() != redis.Nil {
		t.Fatal("auto retry due member should be removed")
	}
	if client.SIsMember(ctx, transferPoolInFlightKey(TransferPoolDefault), transferPoolInFlightMember(jobID, taskID, runToken)).Val() {
		t.Fatal("default inflight run-token member should be removed")
	}
	if client.SIsMember(ctx, transferPoolInFlightKey(TransferPoolResume), transferLegacyPoolInFlightMember(jobID, taskID)).Val() {
		t.Fatal("legacy inflight member should be removed")
	}
}

func TestRecoverDeferredTransferTasksRewindsOffsetToEarliestContendedTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	jobID := int64(55)
	offsetKey := fmt.Sprintf("tx:job:%d:offset", jobID)
	if err := client.Set(ctx, offsetKey, "321", 0).Err(); err != nil {
		t.Fatalf("seed offset: %v", err)
	}

	bucketKey := getTaskBucketKey(jobID, 1)
	if err := client.ZAdd(ctx, bucketKey,
		redis.Z{Score: 321, Member: "321"},
		redis.Z{Score: 322, Member: "322"},
	).Err(); err != nil {
		t.Fatalf("seed sharded bucket: %v", err)
	}

	recoverDeferredTransferTasks(ctx, jobID, []models.TransferTask{
		{ID: 330, JobID: jobID},
		{ID: 322, JobID: jobID},
		{ID: 400, JobID: jobID},
	})

	offset, err := client.Get(ctx, offsetKey).Result()
	if err != nil {
		t.Fatalf("load rewound offset: %v", err)
	}
	if offset != "321" {
		t.Fatalf("expected offset rewind to earliest contended task, got %s", offset)
	}
}

func TestProcessTransferAutoRetryMemberClearsScheduleForNonFailedTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       901,
		JobID:    902,
		Status:   "PENDING",
		Src:      "foo/bar.mp4",
		Size:     123,
		WorkerID: "",
	}
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	taskKey := fmt.Sprintf("tx:task:%d:%d", task.JobID, task.ID)
	if err := client.Set(ctx, taskKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	pipe := client.Pipeline()
	setTransferAutoRetryScheduled(pipe, ctx, task.JobID, task.ID, time.Now().Add(-time.Minute))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed auto retry schedule: %v", err)
	}

	if err := processTransferAutoRetryMember(ctx, transferAutoRetryMember(task.JobID, task.ID)); err != nil {
		t.Fatalf("process auto retry member: %v", err)
	}

	scheduled, err := isTransferAutoRetryScheduled(ctx, task.JobID, task.ID)
	if err != nil {
		t.Fatalf("check scheduled marker after processing: %v", err)
	}
	if scheduled {
		t.Fatal("expected scheduled marker to be cleared for non-failed task")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(task.JobID, task.ID)).Err() != redis.Nil {
		t.Fatal("expected due member to be cleared for non-failed task")
	}
}

func TestProcessTransferAutoRetryMemberSkipsFutureRescheduledTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("TRANSFER_AUTO_RETRY_FAILED_ENABLED", "true")
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:           905,
		JobID:        906,
		Status:       "FAILED",
		Src:          "foo/bar-future.bin",
		ErrorMessage: "temporary upstream 500",
	}
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	taskKey := fmt.Sprintf("tx:task:%d:%d", task.JobID, task.ID)
	if err := client.Set(ctx, taskKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	pipe := client.Pipeline()
	setTransferAutoRetryScheduled(pipe, ctx, task.JobID, task.ID, time.Now().Add(2*time.Minute))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed future auto retry schedule: %v", err)
	}

	if err := processTransferAutoRetryMember(ctx, transferAutoRetryMember(task.JobID, task.ID)); err != nil {
		t.Fatalf("process future auto retry member: %v", err)
	}

	storedRaw, err := client.Get(ctx, taskKey).Result()
	if err != nil {
		t.Fatalf("load task after processing future due member: %v", err)
	}
	var stored models.TransferTask
	if err := json.Unmarshal([]byte(storedRaw), &stored); err != nil {
		t.Fatalf("decode stored task: %v", err)
	}
	if stored.Status != "FAILED" {
		t.Fatalf("future rescheduled task should remain FAILED, got %s", stored.Status)
	}

	scheduled, err := isTransferAutoRetryScheduled(ctx, task.JobID, task.ID)
	if err != nil {
		t.Fatalf("check scheduled marker after future processing: %v", err)
	}
	if !scheduled {
		t.Fatal("future rescheduled task should keep scheduled marker")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(task.JobID, task.ID)).Err() != nil {
		t.Fatal("future rescheduled task should keep due member")
	}
}

func TestMarkTransferTaskActiveCreatesLastSeenMember(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       22,
		JobID:    9,
		Src:      "foo/bar/b.mp4",
		RunToken: "run-activate-2",
		Status:   "RUNNING",
		WorkerID: "worker-b",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)
	claimedMember := transferClaimedRunningMember(task.JobID, task.ID, task.RunToken)
	if err := client.ZAdd(ctx, transferClaimedRunningKey(), redis.Z{Score: 1, Member: claimedMember}).Err(); err != nil {
		t.Fatalf("seed claimed running: %v", err)
	}

	reqBody := map[string]interface{}{
		"job_id":    task.JobID,
		"task_id":   task.ID,
		"run_token": task.RunToken,
		"worker_id": task.WorkerID,
	}
	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/activate", mustJSONBody(t, reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	MarkTransferTaskActive(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	member := transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken)
	score, err := client.ZScore(ctx, transferRunningLastSeenKey(), member).Result()
	if err != nil {
		t.Fatalf("expected last_seen member after activate: %v", err)
	}
	if score <= 0 {
		t.Fatalf("invalid last_seen score: %v", score)
	}
	if client.ZScore(ctx, transferClaimedRunningKey(), claimedMember).Err() != redis.Nil {
		t.Fatal("activate should remove claimed member")
	}
}

func TestMarkTransferTaskActiveIgnoredOnRunTokenMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       23,
		JobID:    9,
		Src:      "foo/bar/c.mp4",
		RunToken: "run-activate-3",
		Status:   "RUNNING",
		WorkerID: "worker-c",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)
	claimedMember := transferClaimedRunningMember(task.JobID, task.ID, task.RunToken)
	if err := client.ZAdd(ctx, transferClaimedRunningKey(), redis.Z{Score: 1, Member: claimedMember}).Err(); err != nil {
		t.Fatalf("seed claimed running: %v", err)
	}

	reqBody := map[string]interface{}{
		"job_id":    task.JobID,
		"task_id":   task.ID,
		"run_token": "wrong-token",
		"worker_id": task.WorkerID,
	}
	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/activate", mustJSONBody(t, reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	MarkTransferTaskActive(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ignored" || body["reason"] != "run_token_mismatch" {
		t.Fatalf("unexpected response body: %s", rec.Body.String())
	}

	member := transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken)
	if client.ZScore(ctx, transferRunningLastSeenKey(), member).Err() != redis.Nil {
		t.Fatal("ignored activate should not create last_seen member")
	}
	if client.ZScore(ctx, transferClaimedRunningKey(), claimedMember).Err() != nil {
		t.Fatal("ignored activate should keep claimed member")
	}
}

func TestRecordTransferWorkerHeartbeatStoresTTL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/heartbeat", mustJSONBody(t, map[string]interface{}{
		"worker_id": "worker-heartbeat-1",
	}))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	RecordTransferWorkerHeartbeat(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	ctx := context.Background()
	key := transferWorkerHeartbeatKey("worker-heartbeat-1")
	if client.Exists(ctx, key).Val() != 1 {
		t.Fatal("heartbeat key should exist")
	}
	ttl := client.TTL(ctx, key).Val()
	if ttl <= 0 || ttl > TransferWorkerHeartbeatTTL {
		t.Fatalf("unexpected heartbeat ttl: %v", ttl)
	}
}

func TestTransferMultipartCheckpointSaveLoadAndClear(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	saveReq := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/checkpoint/save", mustJSONBody(t, map[string]interface{}{
		"job_id":                    101,
		"task_id":                   202,
		"src":                       "https://src.example.com/a.bin",
		"size":                      12345,
		"source_etag":               "etag-1",
		"src_identity":              "src|12345",
		"dst_bucket":                "dst-bucket",
		"dst_key":                   "backup/a.bin",
		"upload_id":                 "upload-1",
		"part_size":                 1024,
		"num_parts":                 13,
		"attempt_count":             2,
		"last_run_token":            "run-1",
		"resume_fail_streak":        1,
		"last_known_uploaded_parts": 7,
	}))
	saveReq.Header.Set("Content-Type", "application/json")
	saveRec := httptest.NewRecorder()
	saveCtx, _ := gin.CreateTestContext(saveRec)
	saveCtx.Request = saveReq

	SaveTransferMultipartCheckpoint(saveCtx)
	if saveRec.Code != http.StatusOK {
		t.Fatalf("save status=%d body=%s", saveRec.Code, saveRec.Body.String())
	}

	ctx := context.Background()
	key := transferMultipartCheckpointKey(101, 202)
	ttl := client.TTL(ctx, key).Val()
	if ttl <= 0 || ttl > DefaultTransferMultipartCheckpointTTL {
		t.Fatalf("unexpected checkpoint ttl: %v", ttl)
	}

	pipe := client.Pipeline()
	setTransferResumeCandidate(pipe, ctx, 101, 202)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed resume candidate: %v", err)
	}

	loadReq := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/checkpoint/load", mustJSONBody(t, map[string]interface{}{
		"job_id":  101,
		"task_id": 202,
	}))
	loadReq.Header.Set("Content-Type", "application/json")
	loadRec := httptest.NewRecorder()
	loadCtx, _ := gin.CreateTestContext(loadRec)
	loadCtx.Request = loadReq

	LoadTransferMultipartCheckpoint(loadCtx)
	if loadRec.Code != http.StatusOK {
		t.Fatalf("load status=%d body=%s", loadRec.Code, loadRec.Body.String())
	}

	var loadBody struct {
		Found      bool                        `json:"found"`
		Checkpoint transferMultipartCheckpoint `json:"checkpoint"`
	}
	if err := json.Unmarshal(loadRec.Body.Bytes(), &loadBody); err != nil {
		t.Fatalf("decode load response: %v", err)
	}
	if !loadBody.Found {
		t.Fatal("expected checkpoint to be found")
	}
	if loadBody.Checkpoint.UploadID != "upload-1" || loadBody.Checkpoint.LastKnownUploadedParts != 7 || loadBody.Checkpoint.SourceETag != "etag-1" {
		t.Fatalf("unexpected checkpoint payload: %+v", loadBody.Checkpoint)
	}

	clearReq := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/checkpoint/clear", mustJSONBody(t, map[string]interface{}{
		"job_id":  101,
		"task_id": 202,
	}))
	clearReq.Header.Set("Content-Type", "application/json")
	clearRec := httptest.NewRecorder()
	clearCtx, _ := gin.CreateTestContext(clearRec)
	clearCtx.Request = clearReq

	ClearTransferMultipartCheckpoint(clearCtx)
	if clearRec.Code != http.StatusOK {
		t.Fatalf("clear status=%d body=%s", clearRec.Code, clearRec.Body.String())
	}
	if client.Exists(ctx, key).Val() != 0 {
		t.Fatal("checkpoint key should be deleted")
	}
	hasCandidate, err := hasTransferResumeCandidate(ctx, 101, 202)
	if err != nil {
		t.Fatalf("check resume candidate after clear: %v", err)
	}
	if hasCandidate {
		t.Fatal("resume candidate should be cleared together with checkpoint")
	}
}

func TestTransferMultipartCheckpointSaveRejectsRunTokenMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       202,
		JobID:    101,
		Status:   "RUNNING",
		RunToken: "run-current",
		WorkerID: "worker-a",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	saveReq := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/checkpoint/save", mustJSONBody(t, map[string]interface{}{
		"job_id":         task.JobID,
		"task_id":        task.ID,
		"src":            "https://src.example.com/a.bin",
		"size":           12345,
		"src_identity":   "src|12345",
		"dst_bucket":     "dst-bucket",
		"dst_key":        "backup/a.bin",
		"upload_id":      "upload-stale",
		"part_size":      1024,
		"num_parts":      13,
		"last_run_token": "run-stale",
	}))
	saveReq.Header.Set("Content-Type", "application/json")
	saveRec := httptest.NewRecorder()
	saveCtx, _ := gin.CreateTestContext(saveRec)
	saveCtx.Request = saveReq

	SaveTransferMultipartCheckpoint(saveCtx)
	if saveRec.Code != http.StatusConflict {
		t.Fatalf("expected conflict on stale checkpoint save, got %d body=%s", saveRec.Code, saveRec.Body.String())
	}
	if client.Exists(ctx, transferMultipartCheckpointKey(task.JobID, task.ID)).Val() != 0 {
		t.Fatal("stale checkpoint save should not persist checkpoint")
	}
}

func TestTransferMultipartCheckpointClearRejectsRunTokenMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       202,
		JobID:    101,
		Status:   "RUNNING",
		RunToken: "run-current",
		WorkerID: "worker-a",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	checkpoint := transferMultipartCheckpoint{
		JobID:        task.JobID,
		TaskID:       task.ID,
		Src:          "https://src.example.com/a.bin",
		Size:         12345,
		SrcIdentity:  "src|12345",
		DstBucket:    "dst-bucket",
		DstKey:       "backup/a.bin",
		UploadID:     "upload-current",
		PartSize:     1024,
		NumParts:     13,
		LastRunToken: task.RunToken,
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	if err := client.Set(ctx, transferMultipartCheckpointKey(task.JobID, task.ID), data, time.Hour).Err(); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	clearReq := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/checkpoint/clear", mustJSONBody(t, map[string]interface{}{
		"job_id":    task.JobID,
		"task_id":   task.ID,
		"run_token": "run-stale",
	}))
	clearReq.Header.Set("Content-Type", "application/json")
	clearRec := httptest.NewRecorder()
	clearCtx, _ := gin.CreateTestContext(clearRec)
	clearCtx.Request = clearReq

	ClearTransferMultipartCheckpoint(clearCtx)
	if clearRec.Code != http.StatusConflict {
		t.Fatalf("expected conflict on stale checkpoint clear, got %d body=%s", clearRec.Code, clearRec.Body.String())
	}
	if client.Exists(ctx, transferMultipartCheckpointKey(task.JobID, task.ID)).Val() != 1 {
		t.Fatal("stale checkpoint clear should keep checkpoint intact")
	}
}

func TestBatchUpdateTransferRejectsRunTokenMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       41,
		JobID:    17,
		Src:      "foo/bar/e.mp4",
		RunToken: "current-run-token",
		Status:   "RUNNING",
		WorkerID: "worker-live",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/update", mustJSONBody(t, []map[string]interface{}{
		{
			"id":            task.ID,
			"job_id":        task.JobID,
			"status":        "COMPLETED",
			"run_token":     "stale-run-token",
			"error_message": "",
		},
	}))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	BatchUpdateTransfer(c)
	if rec.Code != http.StatusConflict {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	raw, err := client.Get(ctx, fmt.Sprintf("tx:task:%d:%d", task.JobID, task.ID)).Result()
	if err != nil {
		t.Fatalf("load task after conflict: %v", err)
	}
	var stored models.TransferTask
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatalf("decode stored task: %v", err)
	}
	if stored.Status != "RUNNING" {
		t.Fatalf("status changed unexpectedly: got %s", stored.Status)
	}
	if stored.RunToken != "current-run-token" {
		t.Fatalf("run token changed unexpectedly: got %s", stored.RunToken)
	}
}

func TestBatchUpdateTransferSchedulesAutoRetryForFailedUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	t.Setenv("TRANSFER_AUTO_RETRY_FAILED_ENABLED", "true")
	t.Setenv("TRANSFER_AUTO_RETRY_FAILED_COOLDOWN_SECONDS", "15")

	ctx := context.Background()
	task := models.TransferTask{
		ID:           42,
		JobID:        18,
		Src:          "foo/bar/f.mp4",
		RunToken:     "run-failed-token",
		Status:       "FAILED",
		WorkerID:     "worker-live",
		ErrorMessage: "old error",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	pipe := client.Pipeline()
	setTransferResumeCandidate(pipe, ctx, task.JobID, task.ID)
	pipe.SAdd(ctx, transferPoolInFlightKey(TransferPoolResume), transferPoolInFlightMember(task.JobID, task.ID, task.RunToken))
	pipe.ZAdd(ctx, transferRunningLastSeenKey(), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken),
	})
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed transfer runtime markers: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/update", mustJSONBody(t, []map[string]interface{}{
		{
			"id":            task.ID,
			"job_id":        task.JobID,
			"status":        "FAILED",
			"run_token":     task.RunToken,
			"error_message": `transfer service status 502: {"error":{"code":"NetworkConnectionLost"}}`,
		},
	}))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	BatchUpdateTransfer(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	scheduled, err := isTransferAutoRetryScheduled(ctx, task.JobID, task.ID)
	if err != nil {
		t.Fatalf("check auto retry scheduled: %v", err)
	}
	if !scheduled {
		t.Fatal("expected failed transfer update to schedule auto retry")
	}
	if client.ZScore(ctx, transferAutoRetryDueKey(), transferAutoRetryMember(task.JobID, task.ID)).Err() != nil {
		t.Fatal("expected failed transfer update to add due queue member")
	}
	if client.ZScore(ctx, transferResumeCandidateIndexKey(), transferResumeCandidateMember(task.JobID, task.ID)).Err() != redis.Nil {
		t.Fatal("expected failed transfer update to clear resume candidate")
	}
	if client.SIsMember(ctx, transferPoolInFlightKey(TransferPoolResume), transferPoolInFlightMember(task.JobID, task.ID, task.RunToken)).Val() {
		t.Fatal("expected failed transfer update to clear resume inflight member")
	}
	if client.ZScore(ctx, transferRunningLastSeenKey(), transferRunningLastSeenMember(task.JobID, task.ID, task.RunToken)).Err() != redis.Nil {
		t.Fatal("expected failed transfer update to clear running last seen member")
	}
}

func TestResetTransferTaskForRetryClearsPreviousAttemptFields(t *testing.T) {
	now := time.Date(2026, time.July, 18, 23, 55, 0, 0, time.UTC)
	task := models.TransferTask{
		ID:           2540,
		JobID:        84,
		Src:          "video/sample.mp4",
		Status:       "FAILED",
		ErrorMessage: "transfer service status 502",
		RunToken:     "old-run-token",
		WorkerID:     "worker-old",
		StartedAt:    now.Add(-5 * time.Minute),
		CompletedAt:  now.Add(-2 * time.Minute),
		UpdatedAt:    now.Add(-1 * time.Minute),
	}

	resetTransferTaskForRetry(&task, now)

	if task.Status != "PENDING" {
		t.Fatalf("expected status PENDING, got %s", task.Status)
	}
	if task.ErrorMessage != "" {
		t.Fatalf("expected error message cleared, got %q", task.ErrorMessage)
	}
	if task.RunToken != "" {
		t.Fatalf("expected run token cleared, got %q", task.RunToken)
	}
	if task.WorkerID != "" {
		t.Fatalf("expected worker id cleared, got %q", task.WorkerID)
	}
	if !task.StartedAt.IsZero() {
		t.Fatalf("expected started_at cleared, got %v", task.StartedAt)
	}
	if !task.CompletedAt.IsZero() {
		t.Fatalf("expected completed_at cleared, got %v", task.CompletedAt)
	}
	if !task.UpdatedAt.Equal(now) {
		t.Fatalf("expected updated_at=%v, got %v", now, task.UpdatedAt)
	}
}

func TestRecordTransferTaskCompensationStoresRunTokenKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       31,
		JobID:    12,
		Src:      "foo/bar/c.mp4",
		RunToken: "run-comp-1",
		Status:   "RUNNING",
		WorkerID: "worker-comp-1",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/compensations", mustJSONBody(t, map[string]interface{}{
		"job_id":     task.JobID,
		"task_id":    task.ID,
		"run_token":  task.RunToken,
		"src":        task.Src,
		"worker_id":  task.WorkerID,
		"size":       123,
		"dst_bucket": "bucket-a",
		"dst_key":    "dst/a.mp4",
		"reason":     "final_status_update_failed",
	}))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	RecordTransferTaskCompensation(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	detailKey := transferCompletionCompensationKey(task.JobID, task.ID, task.RunToken)
	raw, err := client.Get(ctx, detailKey).Result()
	if err != nil {
		t.Fatalf("expected compensation detail key: %v", err)
	}
	var detail transferTaskCompensationDetail
	if err := json.Unmarshal([]byte(raw), &detail); err != nil {
		t.Fatalf("decode compensation detail: %v", err)
	}
	if detail.RunToken != task.RunToken {
		t.Fatalf("run token mismatch: got %q want %q", detail.RunToken, task.RunToken)
	}
	member := transferCompletionPendingMember(task.JobID, task.ID, task.RunToken)
	if _, err := client.ZScore(ctx, transferCompletionPendingKey(), member).Result(); err != nil {
		t.Fatalf("expected pending member: %v", err)
	}
}

func TestRecordTransferTaskCompensationRejectsRunTokenMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	oldRDB := database.RDB
	database.RDB = client
	defer func() {
		_ = client.Close()
		database.RDB = oldRDB
	}()

	ctx := context.Background()
	task := models.TransferTask{
		ID:       32,
		JobID:    12,
		Src:      "foo/bar/d.mp4",
		RunToken: "run-comp-2",
		Status:   "RUNNING",
		WorkerID: "worker-comp-2",
	}
	seedTransferTaskForHandlerTest(t, ctx, client, task)

	req := httptest.NewRequest(http.MethodPost, "/api/transfer-tasks/compensations", mustJSONBody(t, map[string]interface{}{
		"job_id":     task.JobID,
		"task_id":    task.ID,
		"run_token":  "wrong-token",
		"dst_bucket": "bucket-a",
		"dst_key":    "dst/a.mp4",
		"reason":     "final_status_update_failed",
	}))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	RecordTransferTaskCompensation(c)
	if rec.Code != http.StatusConflict {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
}

type testError string

func (e testError) Error() string { return string(e) }

func seedTransferTaskForHandlerTest(t *testing.T, ctx context.Context, client *redis.Client, task models.TransferTask) {
	t.Helper()
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	key := fmt.Sprintf("tx:task:%d:%d", task.JobID, task.ID)
	if err := client.Set(ctx, key, data, 0).Err(); err != nil {
		t.Fatalf("seed task: %v", err)
	}
}

func mustJSONBody(t *testing.T, v interface{}) *bytes.Buffer {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return bytes.NewBuffer(data)
}
