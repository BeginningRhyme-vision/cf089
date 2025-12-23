package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/lancedb/lancedb-go/pkg/lancedb"
	"unbound-future-backend/config"
)

func TestLanceDBLifecycle(t *testing.T) {
	// 1. Load Config (we'll override the storage for testing)
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Override with local temp dir for testing to ensure isolation and no network dependency
	tempDir := t.TempDir()
	cfg.LanceDB.URI = tempDir
	// Clear S3 config to avoid InitLanceDB setting AWS env vars that might confuse the local fs backend
	cfg.LanceDB.Endpoint = ""
	cfg.LanceDB.Region = ""
	cfg.LanceDB.AccessKey = ""
	cfg.LanceDB.SecretKey = ""

	t.Logf("Testing with local LanceDB Config: URI=%s", cfg.LanceDB.URI)

	// 2. Initialize LanceDB
	err = InitLanceDB(cfg)
	if err != nil {
		t.Fatalf("Failed to init LanceDB: %v", err)
	}

	if LanceDB == nil {
		t.Fatal("LanceDB instance is nil after successful init")
	}

	ctx := context.Background()
	tableName := "test_youtube_tasks"

	// cleanup potential leftover from previous failed runs
	// Ignore error if table doesn't exist
	_ = LanceDB.DropTable(ctx, tableName)

	// 3. Create Table
	t.Run("CreateTable", func(t *testing.T) {
		pool := memory.NewGoAllocator()
		
		// Schema
		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "job_id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "url", Type: arrow.BinaryTypes.String},
				{Name: "status", Type: arrow.BinaryTypes.String},
				{Name: "title", Type: arrow.BinaryTypes.String},
				{Name: "video_id", Type: arrow.BinaryTypes.String},
				{Name: "error_message", Type: arrow.BinaryTypes.String},
				{Name: "worker_id", Type: arrow.BinaryTypes.String},
				{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_ms}, 
				{Name: "updated_at", Type: arrow.FixedWidthTypes.Timestamp_ms},
				// Vector: FixedSizeList of Float32
				{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
			},
			nil,
		)
		
		// Builders
		bID := array.NewInt64Builder(pool)
		defer bID.Release()
		bJobID := array.NewInt64Builder(pool)
		defer bJobID.Release()
		bURL := array.NewStringBuilder(pool)
		defer bURL.Release()
		bStatus := array.NewStringBuilder(pool)
		defer bStatus.Release()
		bTitle := array.NewStringBuilder(pool)
		defer bTitle.Release()
		bVideoID := array.NewStringBuilder(pool)
		defer bVideoID.Release()
		bError := array.NewStringBuilder(pool)
		defer bError.Release()
		bWorker := array.NewStringBuilder(pool)
		defer bWorker.Release()
		bCreated := array.NewTimestampBuilder(pool, arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType))
		defer bCreated.Release()
		bUpdated := array.NewTimestampBuilder(pool, arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType))
		defer bUpdated.Release()
		
		bVector := array.NewFixedSizeListBuilder(pool, 128, arrow.PrimitiveTypes.Float32)
		defer bVector.Release()
		bVecValues := bVector.ValueBuilder().(*array.Float32Builder)

		// Append 1 row
		bID.Append(1)
		bJobID.Append(1001)
		bURL.Append("http://example.com/video")
		bStatus.Append("PENDING")
		bTitle.Append("Test Video")
		bVideoID.Append("vid123")
		bError.Append("")
		bWorker.Append("worker-1")
		now := arrow.Timestamp(time.Now().UnixMilli())
		bCreated.Append(now)
		bUpdated.Append(now)
		
		bVector.Append(true) // Valid list
		for i := 0; i < 128; i++ {
			bVecValues.Append(float32(i) * 0.1)
		}

		// Create Record
		cols := []arrow.Array{
			bID.NewArray(),
			bJobID.NewArray(),
			bURL.NewArray(),
			bStatus.NewArray(),
			bTitle.NewArray(),
			bVideoID.NewArray(),
			bError.NewArray(),
			bWorker.NewArray(),
			bCreated.NewArray(),
			bUpdated.NewArray(),
			bVector.NewArray(),
		}
		defer func() {
			for _, c := range cols {
				c.Release()
			}
		}()

		record := array.NewRecord(schema, cols, 1)
		defer record.Release()
        
        // Convert Arrow Schema to LanceDB ISchema
        lschema, err := lancedb.NewSchema(schema)
        if err != nil {
            t.Fatalf("Failed to create lancedb schema: %v", err)
        }

		tbl, err := LanceDB.CreateTable(ctx, tableName, lschema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
        
        // Add data
        err = tbl.AddRecords(ctx, []arrow.Record{record}, nil)
        if err != nil {
             t.Fatalf("Failed to add records: %v", err)
        }
        
		t.Log("Table created and data added successfully")
	})

	// 4. Verify Table Exists
	t.Run("ListTables", func(t *testing.T) {
		tables, err := LanceDB.TableNames(ctx)
		if err != nil {
			t.Fatalf("Failed to list tables: %v", err)
		}
		found := false
		for _, name := range tables {
			if name == tableName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Created table %s not found in %v", tableName, tables)
		}
	})

	// 5. Query Data
	t.Run("QueryData", func(t *testing.T) {
		tbl, err := LanceDB.OpenTable(ctx, tableName)
		if err != nil {
			t.Fatalf("Failed to open table: %v", err)
		}

		// Verify count
		count, err := tbl.Count(ctx)
		if err != nil {
			t.Fatalf("Failed to count rows: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}

		// Verify data using SelectWithColumns (simple scan)
		// Select all columns
		rows, err := tbl.SelectWithColumns(ctx, nil) // nil or empty slice usually implies all columns in some implementations, or we specify one.
		if err != nil {
             // Try specifying columns if nil fails
			rows, err = tbl.SelectWithColumns(ctx, []string{"id", "url", "status"})
            if err != nil {
                 t.Fatalf("Failed to select rows: %v", err)
            }
		}

		if len(rows) != 1 {
			t.Errorf("Expected 1 row from select, got %d", len(rows))
		} else {
            // Check values
            row := rows[0]
            if id, ok := row["id"].(int64); ok {
                if id != 1 {
                     t.Errorf("Expected ID 1, got %d", id)
                }
            }
            // Note: Map keys and value types depend on implementation unmarshalling (arrow to map).
            t.Logf("Row data: %v", row)
        }
	})
	
	// 6. Vector Search (Optional check)
	t.Run("VectorSearch", func(t *testing.T) {
		tbl, err := LanceDB.OpenTable(ctx, tableName)
		if err != nil {
			t.Fatalf("Failed to open table: %v", err)
		}
		
		// Create a query vector
		qVec := make([]float32, 128)
		for i := 0; i < 128; i++ {
			qVec[i] = float32(i) * 0.1
		}
		
		// Search
        // Using VectorSearch method on Table interface
		results, err := tbl.VectorSearch(ctx, "vector", qVec, 1)
		if err != nil {
			t.Fatalf("Failed to execute vector search: %v", err)
		}
		
		if len(results) == 0 {
			t.Error("Vector search returned no results")
		} else {
             t.Logf("Vector search returned %d results", len(results))
        }
	})

    // 7. Update Data
    t.Run("UpdateData", func(t *testing.T) {
        tbl, err := LanceDB.OpenTable(ctx, tableName)
        if err != nil {
            t.Fatalf("Failed to open table: %v", err)
        }

        // Update status to COMPLETED where id = 1
        updates := map[string]interface{}{
            "status": "COMPLETED",
        }
        err = tbl.Update(ctx, "id = 1", updates)
        if err != nil {
            t.Fatalf("Failed to update data: %v", err)
        }

        // Verify update
        rows, err := tbl.SelectWithFilter(ctx, "id = 1")
        if err != nil {
            t.Fatalf("Failed to select updated data: %v", err)
        }
        if len(rows) != 1 {
            t.Fatalf("Expected 1 row, got %d", len(rows))
        }
        
        status := rows[0]["status"]
        // Status might be string or []byte depending on implementation, usually string for string type
        if s, ok := status.(string); ok {
            if s != "COMPLETED" {
                t.Errorf("Expected status COMPLETED, got %s", s)
            }
        } else {
            t.Logf("Status type is %T: %v", status, status)
             // If not string, check string representation
             if fmt.Sprintf("%v", status) != "COMPLETED" {
                  t.Errorf("Expected status COMPLETED, got %v", status)
             }
        }
    })

    // 8. Delete Data
    t.Run("DeleteData", func(t *testing.T) {
        tbl, err := LanceDB.OpenTable(ctx, tableName)
        if err != nil {
            t.Fatalf("Failed to open table: %v", err)
        }

        // Delete where id = 1
        err = tbl.Delete(ctx, "id = 1")
        if err != nil {
            t.Fatalf("Failed to delete data: %v", err)
        }

        // Verify deletion
        count, err := tbl.Count(ctx)
        if err != nil {
            t.Fatalf("Failed to count after delete: %v", err)
        }
        if count != 0 {
            t.Errorf("Expected 0 rows after delete, got %d", count)
        }
    })

	// 9. Drop Table
	t.Run("DropTable", func(t *testing.T) {
		err := LanceDB.DropTable(ctx, tableName)
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}
		
		// Verify it's gone
		tables, _ := LanceDB.TableNames(ctx)
		for _, name := range tables {
			if name == tableName {
				t.Errorf("Table %s still exists after drop", tableName)
			}
		}
	})
}

