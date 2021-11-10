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
	"sync"
	"time"

	"github.com/OTA-Insight/bqwriter"
	"github.com/OTA-Insight/bqwriter/log"
)

func testInsertAllStreamerDefault(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	streamer, err := bqwriter.NewStreamer(
		ctx,
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			Logger: logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create default InsertAll streamer: %w", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		testStreamer(ctx, iterations, "insertAll", "default", streamer, NewTmpData, logger)
	}()
	return nil
}

func testInsertAllStreamerForParameters(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string, workerCount int, workerQueueSize int, maxBatchDelay time.Duration, batchSize int) error {
	streamer, err := bqwriter.NewStreamer(
		ctx,
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			WorkerCount:     workerCount,
			WorkerQueueSize: workerQueueSize,
			MaxBatchDelay:   maxBatchDelay,
			InsertAllClient: &bqwriter.InsertAllClientConfig{
				BatchSize: batchSize,
			},
			Logger: logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create custom InsertAll streamer: %w", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		testStreamer(
			ctx, iterations, "insertAll",
			fmt.Sprintf(
				"workeCount=%d;workerQueue=%d;maxBatchDelay=%v;batchSize=%d",
				workerCount, workerQueueSize, maxBatchDelay, batchSize,
			),
			streamer, NewTmpData, logger)
	}()
	return nil
}

func testInsertAllStreamerNoBatchSingleWorkerNoQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 1, 1, 1, 1)
}

func testInsertAllStreamerNoBatchSingleWorkerWithQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 1, 8, 1, 1)
}

func testkInsertAllStreamerNoBatchMultiWorkerNoQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 4, 1, 1, 1)
}

func testInsertAllStreamerNoBatchMultiWorkerQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 4, 8, 1, 1)
}

func testInsertAllStreamerBatch50SingleWorkerNoQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 1, 1, time.Second*5, 50)
}

func testInsertAllStreamerBatch50SingleWorkerWithQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 1, 8, time.Second*5, 50)
}

func testInsertAllStreamerBatch50MultiWorkerNoQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 4, 1, time.Second*5, 50)
}

func testInsertAllStreamerBatch50MultiWorkerQueue(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	return testInsertAllStreamerForParameters(ctx, iterations, wg, logger, projectID, datasetID, tableID, 4, 8, time.Second*5, 50)
}
