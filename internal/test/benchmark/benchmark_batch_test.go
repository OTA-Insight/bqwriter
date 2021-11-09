package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter"
)

// NewTmpDataReader create a io.Reader based on the Json-serialized data.
func NewTmpDataReader(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{} {
	data := NewTmpData(insertID, name, uuid, timestamp, truth, parameters)
	b, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return bytes.NewBuffer(b)
}

func BenchmarkBatchStreamerDefault(b *testing.B) {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			BatchClient: new(bqwriter.BatchClientConfig),
		},
	)
	if err != nil {
		b.Fatalf("failed to create default Batch streamer: %v", err)
	}
	defer streamer.Close()
	BenchmarkStreamer(b, "batch", "default (single item per batch)", streamer, NewTmpDataReader)
}

func benchmarkBatchStreamerForParameters(b *testing.B, workerCount int, workerQueueSize int, maxBatchDelay time.Duration) {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			BatchClient:     new(bqwriter.BatchClientConfig),
		},
	)
	if err != nil {
		b.Fatalf("failed to create custom Batch streamer: %v", err)
	}
	defer streamer.Close()
	BenchmarkStreamer(
		b, "batch",
		fmt.Sprintf(
			"single item per batch, workeCount=%d;workerQueue=%d;maxBatchDelay=%v",
			workerCount, workerQueueSize, maxBatchDelay,
		),
		streamer, NewTmpDataReader,
	)
}

func BenchmarkBatchStreamerNoBatchSingleWorkerNoQueue(b *testing.B) {
	benchmarkBatchStreamerForParameters(b, 1, 1, 1)
}

func BenchmarkBatchStreamerNoBatchSingleWorkerWithQueue(b *testing.B) {
	benchmarkBatchStreamerForParameters(b, 1, 8, 1)
}

func BenchmarkBatchStreamerNoBatchMultiWorkerNoQueue(b *testing.B) {
	benchmarkBatchStreamerForParameters(b, 4, 1, 1)
}

func BenchmarkBatchStreamerNoBatchMultiWorkerQueue(b *testing.B) {
	benchmarkBatchStreamerForParameters(b, 4, 8, 1)
}
