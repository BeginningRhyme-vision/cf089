package handlers

import (
	"testing"

	"unbound-future-backend/models"
)

func TestIsTransferPermanentFailure(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want bool
	}{
		{name: "source not found marker", msg: "SourceNotFound: source head returned 404", want: true},
		{name: "legacy source fetch marker", msg: "source fetch returned 404", want: true},
		{name: "zero size disabled marker", msg: "ZeroSizeDisabled: zero-byte transfer disabled by config", want: true},
		{name: "generic transient error", msg: "transfer service status 502: upstream timeout", want: false},
		{name: "empty message", msg: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransferPermanentFailure(tt.msg); got != tt.want {
				t.Fatalf("isTransferPermanentFailure(%q) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

func TestShouldRetryTransferTaskSkipsPermanentFailures(t *testing.T) {
	tests := []struct {
		name string
		task models.TransferTask
		want bool
	}{
		{
			name: "retryable failed task",
			task: models.TransferTask{Status: "FAILED", ErrorMessage: "temporary upstream timeout"},
			want: true,
		},
		{
			name: "source not found is skipped",
			task: models.TransferTask{Status: "FAILED", ErrorMessage: "SourceNotFound: source head returned 404"},
			want: false,
		},
		{
			name: "zero size disabled is skipped",
			task: models.TransferTask{Status: "FAILED", ErrorMessage: "ZeroSizeDisabled: zero-byte transfer disabled by config"},
			want: false,
		},
		{
			name: "completed task is skipped",
			task: models.TransferTask{Status: "COMPLETED", ErrorMessage: "temporary upstream timeout"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRetryTransferTask(tt.task); got != tt.want {
				t.Fatalf("shouldRetryTransferTask(%+v) = %v, want %v", tt.task, got, tt.want)
			}
		})
	}
}
