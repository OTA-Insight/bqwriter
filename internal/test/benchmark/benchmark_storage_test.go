package benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/OTA-Insight/bqwriter"
)

// NOTE: https://github.com/googleapis/google-cloud-go/issues/5097
// TLDR:
// - use adapt.NormalizeDescriptor for nested type descriptors
// - known types aren't supported, types need to be converted, e.g. int64 (micro epoch) for timestamp
// - use proto2, proto3 isn't supported yet (due to the changes, e.g. no required etc)

func BenchmarkStorageStreamerDefault(b *testing.B) {
	protoDescriptor, err := adapt.NormalizeDescriptor((&TemporaryDataProto2{}).ProtoReflect().Descriptor())
	if err != nil {
		b.Fatalf("failed to create normalized descriptor: %v", err)
	}
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			StorageClient: &bqwriter.StorageClientConfig{
				ProtobufDescriptor: protoDescriptor,
			},
		},
	)
	if err != nil {
		b.Fatalf("failed to create default Storage streamer: %v", err)
	}
	defer streamer.Close()
	benchmarkStreamer(b, "storage", "default", streamer, NewProtoTmpData)
}

func BenchmarkStorageStreamerDefaultJson(b *testing.B) {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			StorageClient: &bqwriter.StorageClientConfig{
				BigQuerySchema: &tmpDataBigQuerySchema,
			},
		},
	)
	if err != nil {
		b.Fatalf("failed to create BQ schema-based Storage streamer: %v", err)
	}
	defer streamer.Close()
	benchmarkStreamer(b, "storage", "default (json)", streamer, NewTmpData)
}

func benchmarkStorageStreamerForParameters(b *testing.B, workerCount int, workerQueueSize int, maxBatchDelay time.Duration) {
	protoDescriptor, err := adapt.NormalizeDescriptor((&TemporaryDataProto2{}).ProtoReflect().Descriptor())
	if err != nil {
		b.Fatalf("failed to create normalized descriptor: %v", err)
	}
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			StorageClient: &bqwriter.StorageClientConfig{
				ProtobufDescriptor: protoDescriptor,
			},
		},
	)
	if err != nil {
		b.Fatalf("failed to create custom Storage streamer: %v", err)
	}
	defer streamer.Close()
	benchmarkStreamer(
		b, "storage",
		fmt.Sprintf(
			"workeCount=%d;workerQueue=%d;maxBatchDelay=%v",
			workerCount, workerQueueSize, maxBatchDelay,
		),
		streamer, NewProtoTmpData,
	)
}

func BenchmarkStorageStreamerNoBatchSingleWorkerNoQueue(b *testing.B) {
	benchmarkStorageStreamerForParameters(b, 1, 1, 1)
}

func BenchmarkStorageStreamerNoBatchSingleWorkerWithQueue(b *testing.B) {
	benchmarkStorageStreamerForParameters(b, 1, 8, 1)
}

func BenchmarkStorageStreamerNoBatchMultiWorkerNoQueue(b *testing.B) {
	benchmarkStorageStreamerForParameters(b, 4, 1, 1)
}

func BenchmarkStorageStreamerNoBatchMultiWorkerQueue(b *testing.B) {
	benchmarkStorageStreamerForParameters(b, 4, 8, 1)
}

func benchmarkJsonStorageStreamerForParameters(b *testing.B, workerCount int, workerQueueSize int, maxBatchDelay time.Duration) {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		benchmarkBigQueryProjectID,
		benchmarkBigQueryDatasetID,
		benchmarkBigQueryTableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			StorageClient: &bqwriter.StorageClientConfig{
				BigQuerySchema: &tmpDataBigQuerySchema,
			},
		},
	)
	if err != nil {
		b.Fatalf("failed to create custom BQ-schema based Storage streamer: %v", err)
	}
	defer streamer.Close()
	benchmarkStreamer(
		b, "storage-json",
		fmt.Sprintf(
			"workeCount=%d;workerQueue=%d;maxBatchDelay=%v",
			workerCount, workerQueueSize, maxBatchDelay,
		),
		streamer, NewTmpData,
	)
}

func BenchmarkJsonStorageStreamerNoBatchSingleWorkerNoQueue(b *testing.B) {
	benchmarkJsonStorageStreamerForParameters(b, 1, 1, 1)
}

func BenchmarkJsonStorageStreamerNoBatchSingleWorkerWithQueue(b *testing.B) {
	benchmarkJsonStorageStreamerForParameters(b, 1, 8, 1)
}

func BenchmarkJsonStorageStreamerNoBatchMultiWorkerNoQueue(b *testing.B) {
	benchmarkJsonStorageStreamerForParameters(b, 4, 1, 1)
}

func BenchmarkJsonStorageStreamerNoBatchMultiWorkerQueue(b *testing.B) {
	benchmarkJsonStorageStreamerForParameters(b, 4, 8, 1)
}
