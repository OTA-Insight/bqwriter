// Copyright 2021 OTA Insight Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/OTA-Insight/bqwriter"
)

// NOTE: https://github.com/googleapis/google-cloud-go/issues/5097
// TLDR:
// - use adapt.NormalizeDescriptor for nested type descriptors
// - known types aren't supported, types need to be converted, e.g. int64 (micro epoch) for timestamp
// - use proto2, proto3 isn't supported yet (due to the changes, e.g. no required etc)

func testStorageStreamerDefault(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	protoDescriptor, err := adapt.NormalizeDescriptor((&TemporaryDataProto2{}).ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("failed to create normalized descriptor: %w", err)
	}
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			StorageClient: &bqwriter.StorageClientConfig{
				ProtobufDescriptor: protoDescriptor,
			},
			Logger: logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create default Storage streamer: %w", err)
	}
	return testStreamer(ctx, iterations, "storage", "default", streamer, NewProtoTmpData, logger)
}

func testStorageStreamerDefaultJson(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			StorageClient: &bqwriter.StorageClientConfig{
				BigQuerySchema: &tmpDataBigQuerySchema,
			},
			Logger: logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create BQ schema-based Storage streamer: %w", err)
	}
	return testStreamer(ctx, iterations, "storage", "default-json", streamer, NewStorageTmpData, logger)
}

func testStorageStreamerForParameters(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string, workerCount int, workerQueueSize int, maxBatchDelay time.Duration) error {
	protoDescriptor, err := adapt.NormalizeDescriptor((&TemporaryDataProto2{}).ProtoReflect().Descriptor())
	if err != nil {
		return fmt.Errorf("failed to create normalized descriptor: %w", err)
	}
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			StorageClient: &bqwriter.StorageClientConfig{
				ProtobufDescriptor: protoDescriptor,
			},
			Logger: logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create custom Storage streamer: %w", err)
	}

	return testStreamer(
		ctx, iterations,
		"storage",
		fmt.Sprintf(
			"workeCount=%d;workerQueue=%d;maxBatchDelay=%v",
			workerCount, workerQueueSize, maxBatchDelay,
		),
		streamer, NewProtoTmpData, logger,
	)
}

func testStorageStreamerNoBatchSingleWorkerNoQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 1, 1, 1)
}

func testStorageStreamerNoBatchSingleWorkerWithQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 1, 8, 1)
}

func testStorageStreamerNoBatchMultiWorkerNoQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 4, 1, 1)
}

func testStorageStreamerNoBatchMultiWorkerQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 4, 8, 1)
}

func testJsonStorageStreamerForParameters(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string, workerCount int, workerQueueSize int, maxBatchDelay time.Duration) error {
	streamer, err := bqwriter.NewStreamer(
		context.Background(),
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			StorageClient: &bqwriter.StorageClientConfig{
				BigQuerySchema: &tmpDataBigQuerySchema,
			},
			Logger: logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create custom BQ-schema based Storage streamer: %w", err)
	}
	return testStreamer(
		ctx, iterations,
		"storage-json",
		fmt.Sprintf(
			"workeCount=%d;workerQueue=%d;maxBatchDelay=%v",
			workerCount, workerQueueSize, maxBatchDelay,
		),
		streamer, NewStorageTmpData, logger,
	)
}

func testJsonStorageStreamerNoBatchSingleWorkerNoQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testJsonStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 1, 1, 1)
}

func testJsonStorageStreamerNoBatchSingleWorkerWithQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testJsonStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 1, 8, 1)
}

func testJsonStorageStreamerNoBatchMultiWorkerNoQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testJsonStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 4, 1, 1)
}

func testJsonStorageStreamerNoBatchMultiWorkerQueue(ctx context.Context, iterations int, logger Logger, projectID, datasetID, tableID string) error {
	return testJsonStorageStreamerForParameters(ctx, iterations, logger, projectID, datasetID, tableID, 4, 8, 1)
}
