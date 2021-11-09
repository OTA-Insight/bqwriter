package benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter"
)

func BenchmarkInsertAllStreamerDefault(b *testing.B) {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		nil,
	)
	if err != nil {
		b.Fatalf("failed to create default InsertAll streamer: %v", err)
	}
	defer streamer.Close()
	benchmarkStreamer(b, "insertAll", "default", streamer, NewTmpData)
}

func benchmarkInsertAllStreamerForParameters(b *testing.B, workerCount int, workerQueueSize int, maxBatchDelay time.Duration, batchSize int) {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			InsertAllClient: &bqwriter.InsertAllClientConfig{
				BatchSize: batchSize,
			},
		},
	)
	if err != nil {
		b.Fatalf("failed to create custom InsertAll streamer: %v", err)
	}
	defer streamer.Close()
	benchmarkStreamer(
		b, "insertAll",
		fmt.Sprintf(
			"workeCount=%d;workerQueue=%d;maxBatchDelay=%v;batchSize=%d",
			workerCount, workerQueueSize, maxBatchDelay, batchSize,
		),
		streamer, NewTmpData,
	)
}

func BenchmarkInsertAllStreamerNoBatchSingleWorkerNoQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 1, 1, 1, 1)
}

func BenchmarkInsertAllStreamerNoBatchSingleWorkerWithQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 1, 8, 1, 1)
}

func BenchmarkInsertAllStreamerNoBatchMultiWorkerNoQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 4, 1, 1, 1)
}

func BenchmarkInsertAllStreamerNoBatchMultiWorkerQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 4, 8, 1, 1)
}

func BenchmarkInsertAllStreamerBatch50SingleWorkerNoQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 1, 1, time.Second*5, 50)
}

func BenchmarkInsertAllStreamerBatch50SingleWorkerWithQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 1, 8, time.Second*5, 50)
}

func BenchmarkInsertAllStreamerBatch50MultiWorkerNoQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 4, 1, time.Second*5, 50)
}

func BenchmarkInsertAllStreamerBatch50MultiWorkerQueue(b *testing.B) {
	benchmarkInsertAllStreamerForParameters(b, 4, 8, time.Second*5, 50)
}
