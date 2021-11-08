package benchmark

import (
	"context"
	"testing"

	"github.com/OTA-Insight/bqwriter"
	"google.golang.org/protobuf/reflect/protodesc"
)

func BenchmarkStorageStreamerDefault(b *testing.B) {
	protoDescriptor := protodesc.ToDescriptorProto((&TemporaryDataProto3{}).ProtoReflect().Descriptor())
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
	BenchmarkStreamer(b, "storage", "default", streamer, NewProtoTmpData)
}

// func benchmarkInsertAllStreamerForParameters(b *testing.B, workerCount int, workerQueueSize int, maxBatchDelay time.Duration, batchSize int) {
// 	streamer, err := bqwriter.NewStreamer(
// 		context.Background(),
// 		benchmarkBigQueryProjectID,
// 		benchmarkBigQueryDatasetID,
// 		benchmarkBigQueryTableID,
// 		&bqwriter.StreamerConfig{
// 			WorkerCount:     workerCount,
// 			WorkerQueueSize: workerQueueSize,
// 			MaxBatchDelay:   maxBatchDelay,
// 			InsertAllClient: &bqwriter.InsertAllClientConfig{
// 				BatchSize: batchSize,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		b.Fatalf("failed to create custom InsertAll streamer: %v", err)
// 	}
// 	defer streamer.Close()
// 	BenchmarkStreamer(
// 		b, "insertAll",
// 		fmt.Sprintf(
// 			"workeCount=%d;workerQueue=%d;maxBatchDelay=%v;batchSize=%d",
// 			workerCount, workerQueueSize, maxBatchDelay, batchSize,
// 		),
// 		streamer, NewTmpData,
// 	)
// }

// func BenchmarkInsertAllStreamerNoBatchSingleWorkerNoQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 1, 1, 1, 1)
// }

// func BenchmarkInsertAllStreamerNoBatchSingleWorkerWithQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 1, 8, 1, 1)
// }

// func BenchmarkInsertAllStreamerNoBatchMultiWorkerNoQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 4, 1, 1, 1)
// }

// func BenchmarkInsertAllStreamerNoBatchMultiWorkerQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 4, 8, 1, 1)
// }

// func BenchmarkInsertAllStreamerBatch50SingleWorkerNoQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 1, 1, time.Second*5, 50)
// }

// func BenchmarkInsertAllStreamerBatch50SingleWorkerWithQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 1, 8, time.Second*5, 50)
// }

// func BenchmarkInsertAllStreamerBatch50MultiWorkerNoQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 4, 1, time.Second*5, 50)
// }

// func BenchmarkInsertAllStreamerBatch50MultiWorkerQueue(b *testing.B) {
// 	benchmarkInsertAllStreamerForParameters(b, 4, 8, time.Second*5, 50)
// }
