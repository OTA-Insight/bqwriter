package benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter"
	"google.golang.org/protobuf/reflect/protodesc"
)

// NOTE: created an issue about nested type errors:
// https://github.com/googleapis/google-cloud-go/issues/5097

func BenchmarkStorageStreamerDefault(b *testing.B) {
	// TODO: how can we have these nested types automatically included?
	protoDescriptor := protodesc.ToDescriptorProto((&TemporaryDataProto3{}).ProtoReflect().Descriptor())
	// protoDescriptor.NestedType = append(
	// 	protoDescriptor.NestedType,
	// 	protodesc.ToDescriptorProto((&timestamppb.Timestamp{}).ProtoReflect().Descriptor()),
	// 	protodesc.ToDescriptorProto((&TemporaryDataParameterProto3{}).ProtoReflect().Descriptor()),
	// )
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

func benchmarkStorageStreamerForParameters(b *testing.B, workerCount int, workerQueueSize int, maxBatchDelay time.Duration) {
	// TODO: how can we have these nested types automatically included?
	protoDescriptor := protodesc.ToDescriptorProto((&TemporaryDataProto3{}).ProtoReflect().Descriptor())
	// protoDescriptor.NestedType = append(
	// 	protoDescriptor.NestedType,
	// 	protodesc.ToDescriptorProto((&timestamppb.Timestamp{}).ProtoReflect().Descriptor()),
	// 	protodesc.ToDescriptorProto((&TemporaryDataParameterProto3{}).ProtoReflect().Descriptor()),
	// )
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
	BenchmarkStreamer(
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
