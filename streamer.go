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

// Package bqwriter provides a compact Streamer API in order to write
// data concurrently to BigQuery using the insertAll or Storage API.
package bqwriter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/OTA-Insight/bqwriter/internal"
	"github.com/OTA-Insight/bqwriter/internal/bigquery"
	"github.com/OTA-Insight/bqwriter/internal/bigquery/batch"
	"github.com/OTA-Insight/bqwriter/internal/bigquery/insertall"
	"github.com/OTA-Insight/bqwriter/internal/bigquery/storage"
	"github.com/OTA-Insight/bqwriter/internal/bigquery/storage/encoding"
	"github.com/OTA-Insight/bqwriter/log"
)

var mutuallyExclusiveConfigs = errors.New("you can not define both a storage Client and a batch client at once")

// Streamer is a simple BQ stream-writer, allowing you
// write data to a BQ table concurrently.
type Streamer struct {
	logger log.Logger

	workerWg       sync.WaitGroup
	workerCh       chan streamerJob
	workerCtx      context.Context
	workerCancelFn func()
}

// streamerJob is all info required in order to write a row of data to BQ, the job of this streamer.
type streamerJob struct {
	Data interface{}
}

// NewStreamer creates a new Streamer Client. StreamerConfig is optional,
// all other parameters are required.
//
// An error is returned in case the Streamer Client couldn't be created for some unexpected reason,
// most likely something going wrong within the layer of actually interacting with GCloud.
func NewStreamer(ctx context.Context, projectID, dataSetID, tableID string, cfg *StreamerConfig) (*Streamer, error) {
	return newStreamerWithClientBuilder(
		ctx,
		func(ctx context.Context, projectID, dataSetID, tableID string, logger log.Logger, insertAllCfg *InsertAllClientConfig, storageCfg *StorageClientConfig, batchCfg *BatchClientConfig) (bigquery.Client, error) {
			if storageCfg != nil && batchCfg != nil {
				return nil, mutuallyExclusiveConfigs
			}

			if storageCfg != nil {
				if storageCfg.ProtobufDescriptor != nil {
					client, err := storage.NewClient(
						projectID, dataSetID, tableID,
						encoding.NewProtobufEncoder(), storageCfg.ProtobufDescriptor,
						storageCfg.MaxRetries, storageCfg.InitialRetryDelay,
						storageCfg.MaxRetryDeadlineOffset, storageCfg.RetryDelayMultiplier,
						logger,
					)
					if err != nil {
						return nil, fmt.Errorf("BigQuery: NewStreamer: New Protobuf Message Storage client: %w", err)
					}
					return client, nil
				}

				encoder, err := encoding.NewSchemaEncoder(*storageCfg.BigQuerySchema)
				if err != nil {
					return nil, fmt.Errorf("BigQuery: NewStreamer: New BigQuery-Schema encoding Storage client: create schema encoder: %w", err)
				}
				client, err := storage.NewClient(
					projectID, dataSetID, tableID,
					encoder, nil,
					storageCfg.MaxRetries, storageCfg.InitialRetryDelay,
					storageCfg.MaxRetryDeadlineOffset, storageCfg.RetryDelayMultiplier,
					logger,
				)
				if err != nil {
					return nil, fmt.Errorf("BigQuery: NewStreamer: New BigQuery-Schema encoding Storage client: %w", err)
				}
				return client, nil
			}

			if batchCfg != nil {
				client, err := batch.NewClient(
					projectID, dataSetID, tableID,
					!batchCfg.FailForUnknownValues, batchCfg.AutoDetect,
					batchCfg.SourceFormat, batchCfg.WriteDisposition,
					batchCfg.BigQuerySchema, batchCfg.CSVOptions,
				)

				if err != nil {
					return nil, fmt.Errorf("BigQuery: NewStreamer: New BigQuery-Schema Batch client: %w", err)
				}
				return client, nil
			}

			client, err := insertall.NewClient(
				projectID, dataSetID, tableID,
				!insertAllCfg.FailOnInvalidRows,
				!insertAllCfg.FailForUnknownValues,
				insertAllCfg.BatchSize, insertAllCfg.MaxRetryDeadlineOffset,
				logger,
			)
			if err != nil {
				return nil, fmt.Errorf("BigQuery: NewStreamer: New InsertAll client: %w", err)
			}
			return client, nil
		},
		projectID, dataSetID, tableID,
		cfg,
	)
}

type clientBuilderFunc func(ctx context.Context, projectID, dataSetID, tableID string, logger log.Logger, insertAllCfg *InsertAllClientConfig, storageCfg *StorageClientConfig, batchCfg *BatchClientConfig) (bigquery.Client, error)

func newStreamerWithClientBuilder(ctx context.Context, clientBuilder clientBuilderFunc, projectID, dataSetID, tableID string, cfg *StreamerConfig) (*Streamer, error) {
	if projectID == "" {
		return nil, fmt.Errorf("streamer client creation: validate projectID: %w: missing", internal.InvalidParamErr)
	}
	if dataSetID == "" {
		return nil, fmt.Errorf("streamer client creation: validate dataSetID: %w: missing", internal.InvalidParamErr)
	}
	if tableID == "" {
		return nil, fmt.Errorf("streamer client creation: validate tableID: %w: missing", internal.InvalidParamErr)
	}

	// sanitize cfg
	cfg, err := sanitizeStreamerConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("streamer client creation: sanitize streamer config: %w", err)
	}

	// create streamer
	workerCtx, workerCtxCancelFn := context.WithCancel(ctx)
	s := &Streamer{
		logger: cfg.Logger,

		workerCh:       make(chan streamerJob, cfg.WorkerCount*cfg.WorkerQueueSize),
		workerCtx:      workerCtx,
		workerCancelFn: workerCtxCancelFn,
	}
	// create & spawn all worker threads
	for i := 0; i < cfg.WorkerCount; i++ {
		cfg.Logger.Debugf("starting streamer worker thread #%d", i+1)
		s.workerWg.Add(1)
		// each worker thread has its own client
		client, err := clientBuilder(
			workerCtx,
			projectID, dataSetID, tableID,
			cfg.Logger,
			cfg.InsertAllClient, cfg.StorageClient, cfg.BatchClient,
		)
		if err != nil {
			workerCtxCancelFn()
			return nil, fmt.Errorf("create streamer client: create client for worker thread: %w", err)
		}
		go func() {
			defer s.workerWg.Done()
			defer func() {
				err := client.Close()
				if err != nil {
					cfg.Logger.Errorf("streamer: failed to close worker's BQ client: %v", err)
				}
			}()
			s.doWork(client, cfg.MaxBatchDelay)
		}()
	}
	return s, nil
}

// Write a row of data to a BQ table within the streamer's project.
// The row will be written as soon as all previous rows has been written
// and a worker goroutine becomes available to write it.
//
// Jobs that failed to write but which are retryable can be retried on the
// same goroutine in an exponential back-off approach, should the streamer be
// configured to do so.
func (s *Streamer) Write(data interface{}) error {
	if data == nil {
		return fmt.Errorf("streamer client write: validate data: %w: nil data", internal.InvalidParamErr)
	}
	job := streamerJob{
		Data: data,
	}
	if err := s.workerCtx.Err(); errors.Is(err, context.Canceled) {
		return fmt.Errorf("write data into BQ streamer: streamer worker context: %w", err)
	}
	select {
	case s.workerCh <- job:
		s.logger.Debug("inserted write job into bq streamer")
	case <-s.workerCtx.Done():
		return fmt.Errorf("write data into BQ streamer: worker is busy: streamer worker context: %w", context.Canceled)
	}
	return nil
}

// doWork defines the main loop of a Streamer's worker goroutine.
func (s *Streamer) doWork(client bigquery.Client, maxBatchDelay time.Duration) {
	defer func() {
		err := client.Flush()
		if err != nil {
			s.logger.Errorf("streamer worker thread is closing: context is done: flush worker client: failure: %v", err)
		} else {
			s.logger.Debug("streamer worker thread is closing: context is done: flushed worker client successfully")
		}
	}()

	batchDelayTicker := time.NewTicker(maxBatchDelay)

	for {
		select {
		case <-s.workerCtx.Done():
			s.logger.Debug("streamer worker thread is closing: context is done: exit worker thread")
			return

		case <-batchDelayTicker.C:
			err := client.Flush()
			if err != nil {
				s.logger.Errorf("worker thread max batch delay interval: flush worker client: failure: %v", err)
			} else {
				s.logger.Debug("worker thread max batch delay interval: flushed worker client successfully")
			}

		case job := <-s.workerCh:
			flushed, err := client.Put(job.Data)
			if err != nil {
				s.logger.Errorf("worker thread data job received: put data to client: failure: %v", err)
			} else if flushed {
				batchDelayTicker.Reset(maxBatchDelay)
				s.logger.Debug("worker thread data job received: put data to client: flushed all batched rows")
			}
		}
	}
}

// Close closes the streamer and all its worker goroutines.
func (s *Streamer) Close() {
	s.logger.Debug("closing streamer")
	s.workerCancelFn()
	s.workerWg.Wait()
	<-s.workerCtx.Done()
}
