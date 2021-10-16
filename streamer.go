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
)

// Streamer is a simple BQ stream-writer, allowing you
// write data to a BQ table concurrently.
type Streamer struct {
	logger Logger

	workerWg       sync.WaitGroup
	workerCh       chan streamerJob
	workerCtx      context.Context
	workerCancelFn func()
}

// streamerJob is all info required in order to write a row of data to BQ, the job of this streamer.
type streamerJob struct {
	Data interface{}
}

type clientBuilderFunc func(context.Context) (bqClient, error)

func newStreamerWithClientBuilder(ctx context.Context, clientBuilder clientBuilderFunc, workerCount int, workerQueueSize int, maxBatchDelay time.Duration, logger Logger) (*Streamer, error) {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}
	if maxBatchDelay == 0 {
		maxBatchDelay = DefaultMaxBatchDelay
	}
	if workerQueueSize <= 0 {
		workerQueueSize = DefaultWorkerQueueSize
	}
	if logger == nil {
		logger = stdLogger{}
	}
	workerCtx, workerCtxCancelFn := context.WithCancel(ctx)
	s := &Streamer{
		logger: logger,

		workerCh:       make(chan streamerJob, workerCount*workerQueueSize),
		workerCtx:      workerCtx,
		workerCancelFn: workerCtxCancelFn,
	}
	for i := 0; i < workerCount; i++ {
		logger.Debugf("starting streamer worker thread #%d", i+1)
		s.workerWg.Add(1)
		client, err := clientBuilder(workerCtx)
		if err != nil {
			workerCtxCancelFn()
			return nil, fmt.Errorf("create streamer client: create client for worker thread: %w", err)
		}
		go func() {
			defer s.workerWg.Done()
			defer func() {
				err := client.Close()
				if err != nil {
					logger.Errorf("streamer: failed to close worker's BQ client: %v", err)
				}
			}()
			s.doWork(client, maxBatchDelay)
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
		return errors.New("Streamer::Write: data is nil: should be defined")
	}
	job := streamerJob{
		Data: data,
	}
	if errors.Is(s.workerCtx.Err(), context.Canceled) {
		return errors.New("write data into BQ streamer: streamer is already closed")
	}
	select {
	case s.workerCh <- job:
		s.logger.Debug("inserted write job into bq streamer")
	case <-s.workerCtx.Done():
		return errors.New("write data into BQ streamer: worker is busy: streamer is already closed")
	}
	return nil
}

// doWork defines the main loop of a Streamer's worker goroutine.
func (s *Streamer) doWork(client bqClient, maxBatchDelay time.Duration) {
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
