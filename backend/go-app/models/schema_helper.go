package models

import (
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

var TaskArrowSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "job_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "url", Type: arrow.BinaryTypes.String},
		{Name: "audio_url", Type: arrow.BinaryTypes.String},
		{Name: "video_url", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "title", Type: arrow.BinaryTypes.String},
		{Name: "video_id", Type: arrow.BinaryTypes.String},
		{Name: "error_message", Type: arrow.BinaryTypes.String},
		{Name: "worker_id", Type: arrow.BinaryTypes.String},
		{Name: "is_download_fail", Type: arrow.FixedWidthTypes.Boolean},
		// Using Timestamp(Microsecond) for times
		{Name: "started_at", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "completed_at", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "updated_at", Type: arrow.FixedWidthTypes.Timestamp_us},
	},
	nil,
)

func ToArrowRecord(tasks []YoutubeTask) (arrow.Record, error) {
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, TaskArrowSchema)
	defer builder.Release()

	// Append data
	bID := builder.Field(0).(*array.Int64Builder)
	bJobID := builder.Field(1).(*array.Int64Builder)
	bURL := builder.Field(2).(*array.StringBuilder)
	bAudioURL := builder.Field(3).(*array.StringBuilder)
	bVideoURL := builder.Field(4).(*array.StringBuilder)
	bStatus := builder.Field(5).(*array.StringBuilder)
	bTitle := builder.Field(6).(*array.StringBuilder)
	bVideoID := builder.Field(7).(*array.StringBuilder)
	bError := builder.Field(8).(*array.StringBuilder)
	bWorker := builder.Field(9).(*array.StringBuilder)
	bIsDownloadFail := builder.Field(10).(*array.BooleanBuilder)
	bStarted := builder.Field(11).(*array.TimestampBuilder)
	bCompleted := builder.Field(12).(*array.TimestampBuilder)
	bCreated := builder.Field(13).(*array.TimestampBuilder)
	bUpdated := builder.Field(14).(*array.TimestampBuilder)

	for _, t := range tasks {
		bID.Append(t.ID)
		bJobID.Append(t.JobID)
		bURL.Append(t.URL)
		bAudioURL.Append(t.AudioURL)
		bVideoURL.Append(t.VideoURL)
		bStatus.Append(t.Status)
		bTitle.Append(t.Title)
		bVideoID.Append(t.VideoID)
		bError.Append(t.ErrorMessage)
		bWorker.Append(t.WorkerID)
		bIsDownloadFail.Append(t.IsDownloadFail)

		appendTime(bStarted, t.StartedAt)
		appendTime(bCompleted, t.CompletedAt)
		appendTime(bCreated, t.CreatedAt)
		appendTime(bUpdated, t.UpdatedAt)
	}

	rec := builder.NewRecord()
	return rec, nil
}

func appendTime(b *array.TimestampBuilder, t time.Time) {
	if t.IsZero() {
		b.AppendNull()
	} else {
		// Arrow Timestamp is int64 (microseconds since epoch)
		// Assuming UTC
		b.Append(arrow.Timestamp(t.UnixMicro()))
	}
}
