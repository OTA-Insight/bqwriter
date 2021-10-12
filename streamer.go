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
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	backoff "github.com/cenkalti/backoff/v4"
	"google.golang.org/api/googleapi"
)

const (
	// DefaultWorkerCount is used as the default for the WorkerCount property of
	// StreamerConfig, used in case the property is 0 (e.g. when undefined).
	DefaultWorkerCount = 5

	// DefaultMaxRetries is used as the default for the MaxRetries property of
	// StreamerConfig, used in case the property is 0 (e.g. when undefined).
	DefaultMaxRetries = 3

	// DefaultWorkerBatchSize defines amount of rows of data a worker
	// will collect prior to writing it to BQ. Used in case the property is 0 (e.g. when undefined).
	DefaultWorkerBatchSize = 250

	/// DefaultMaxBatchDelay defines the max amount of time a worker batches rows, prior to writing the batched rows,
	// even when not yet full. Used in case the property is 0 (e.g. when undefined).
	DefaultMaxBatchDelay = 5 * time.Second

	// DefaultWorkerJobQueueSize defines the default size of the job queue per worker used
	// in order to allow the Streamer's users to write rows even if all workers are currently
	// too busy to accept new incoming rows. Used in case the property is 0 (e.g. when undefined).
	DefaultWorkerQueueSize = 16

	// MaxTotalElapsedRetryTime is the max amount of the the streamer will
	// allow the exponantional retry-back off logic to retry,
	// as to ensure a goroutine isn't blocked for too long on a faulty write.
	//
	// NOTE: in a future version of this module we might want to turn this into the default value,
	// rather than a hardcoded constant. If we choose to do so we'll have to however figure out the correct value
	// for the initialRetryTime and multiplier, in function of this max total retry duration and the max retry times.
	MaxTotalElapsedRetryTime = time.Second * 60
)

// Streamer is a simple BQ stream-writer, allowing you
// write data to a BQ table concurrently.
type Streamer struct {
	client bqClient

	logger Logger

	workerWg       sync.WaitGroup
	workerCh       chan streamerJob
	workerCtx      context.Context
	workerCancelFn func()

	maxRetries uint64
}

// StreamearConfig defines the optional configurations
// used to create a new (BQ) streamer.
type StreamerConfig struct {
	// WorkerCount defines the amount of workers to be used,
	// each on their own goroutine and with an opt-out channel buffer per routine.
	//
	// Defaults to DefaultWorkerCount.
	WorkerCount int

	// SkipInvalidRows (BQ) causes rows containing invalid data to be silently
	// ignored. The default value is false, which causes the entire request to
	// fail if there is an attempt to insert an invalid row.
	//
	// Defaults to false.
	SkipInvalidRows bool
	// IgnoreUnknownValues (BQ) causes values not matching the schema to be ignored.
	// The default value is false, which causes records containing such values
	// to be treated as invalid records.
	//
	// Defaults to false.
	IgnoreUnknownValues bool

	// MaxRetries is the max amount of times that the retry logic will retry a retryable
	// BQ write error, prior to giving up. Note that non-retryable errors will immediately stop
	// and that there is also an upper limit of MaxTotalElpasedRetryTime to execute in worst case these max retries.
	//
	// Defaults to DefaultMaxRetries.
	MaxRetries uint64

	// BatchSize defines the amount of rows (data) by a worker, prior to a worker
	// actually writing it to BQ. Should a worker have rows left in its local cache when closing,
	// it will flush/write these rows prior to closing.
	//
	// Defaults to DefaultWorkerBatchSize.
	BatchSize int

	// MaxBatchDelay defines the max amount of time a worker batches rows, prior to writing the batched rows,
	// even when not yet full.
	//
	// Defaults to DefaultMaxBatchDelay.
	MaxBatchDelay time.Duration

	// WorkerQueueSize defines the size of the job queue per worker used
	// in order to allow the Streamer's users to write rows even if all workers are currently
	// too busy to accept new incoming rows. Used in case the property is 0 (e.g. when undefined).
	//
	// Defaults to MaxTotalElapsedRetryTime.
	WorkerQueueSize int

	// Logger allows you to attach a logger to be used by the streamer,
	// instead of the default built-in STDERR logging implementation.
	Logger Logger
}

// streamerJob is all info required in order to write a row of data to BQ, the job of this streamer.
type streamerJob struct {
	Data interface{}
}

// bqClient is the interface we expect a BQ client to implement.
// The only reason for this abstraction is so we can easily unit test this class,
// without actual BQ interaction.
type bqClient interface {
	// Put a row of data, with the possibility to opt-out of any scheme validation.
	//
	// No context is passed here, instead background context is always used.
	// Reason being is as we always want to be able to write to BQ,
	// even if the actual parent context is already closed.
	Put(data interface{}) error

	// Close the BQ Client
	Close() error
}

// stdBQClient implements the standard/official BQ (cloud) Client,
// and is the one used in production.
type stdBQClient struct {
	client *bigquery.Client

	dataSetID string
	tableID   string

	skipInvalidRows     bool
	ignoreUnknownValues bool
}

// newStdBQClient creates a new stdBQClient.
func newStdBQClient(projectID, dataSetID, tableID string, skipInvalidRows, ignoreUnknownValues bool) (*stdBQClient, error) {
	if projectID == "" {
		return nil, errors.New("NewStreamer: projectID is empty: should be defined")
	}
	if dataSetID == "" {
		return nil, errors.New("NewStreamer: dataSetID is empty: should be defined")
	}
	if tableID == "" {
		return nil, errors.New("NewStreamer: tableID is empty: should be defined")
	}
	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("create BQ Writer Streamer: %w", err)
	}
	return &stdBQClient{
		client:              client,
		dataSetID:           dataSetID,
		tableID:             tableID,
		skipInvalidRows:     skipInvalidRows,
		ignoreUnknownValues: ignoreUnknownValues,
	}, nil
}

// Put implements bqClient::Put
func (bqc *stdBQClient) Put(data interface{}) error {
	inserter := bqc.client.Dataset(bqc.dataSetID).Table(bqc.tableID).Inserter()
	inserter.SkipInvalidRows = bqc.skipInvalidRows
	inserter.IgnoreUnknownValues = bqc.ignoreUnknownValues
	return inserter.Put(context.Background(), data)
}

// Close implements bqClient::Close
func (bqc *stdBQClient) Close() error {
	return bqc.client.Close()
}

// Logger is the interface used by this module in order to support logging.
// By default the error messages are printed to the STDERR and debug messages are ignored, but you can
// inject any logger you wish into a Streamer.
//
// NOTE that it is assumed by this module for a Logger implementation to be safe for concurrent use.
type Logger interface {
	// Debug logs the arguments as a single debug message.
	Debug(args ...interface{})
	// Debugf logs a formatted debug message, injecting the arguments into the template string.
	Debugf(template string, args ...interface{})
	// Error logs the arguments as a single error message.
	Error(args ...interface{})
	// Errorf logs a formatted error message, injecting the arguments into the template string.
	Errorf(template string, args ...interface{})
}

// stdLogger is the builtin implementation of the Logger interface,
// and logs error messages to the STDERR, but ignores debug messages.
type stdLogger struct{}

// Debug implements Logger::Debug
func (stdl stdLogger) Debug(args ...interface{}) {} // debug messages are ignored by default

// Debugf implements Logger::Debugf
func (stdl stdLogger) Debugf(template string, args ...interface{}) {} // debug messages are ignored by default

// Error implements Logger::Error
func (stdl stdLogger) Error(args ...interface{}) {
	fmt.Fprint(os.Stderr, args...)
}

// Errorf implements Logger::Errorf
func (stdl stdLogger) Errorf(template string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, template, args...)
}

// NewStreamer creates a new streamer client.
// When no error is returned, the returning Streamer should always be closed,
// as to close the in the background created (worker) goroutines.
func NewStreamer(ctx context.Context, projectID, dataSetID, tableID string, cfg StreamerConfig) (*Streamer, error) {
	client, err := newStdBQClient(projectID, dataSetID, tableID, cfg.SkipInvalidRows, cfg.IgnoreUnknownValues)
	if err != nil {
		return nil, err
	}
	return newStreamerWithClient(ctx, client, cfg.WorkerCount, cfg.MaxRetries, cfg.BatchSize, cfg.MaxBatchDelay, cfg.WorkerQueueSize, cfg.Logger)
}

func newStreamerWithClient(ctx context.Context, client bqClient, workerCount int, maxRetries uint64, batchSize int, maxBatchDelay time.Duration, workerQueueSize int, logger Logger) (*Streamer, error) {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}
	if maxRetries == 0 {
		maxRetries = DefaultMaxRetries
	}
	if batchSize <= 0 {
		batchSize = DefaultWorkerBatchSize
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
	ctx, cancelFn := context.WithCancel(ctx)
	s := &Streamer{
		client: client,

		logger: logger,

		workerCh:       make(chan streamerJob, workerCount*workerQueueSize),
		workerCtx:      ctx,
		workerCancelFn: cancelFn,

		maxRetries: maxRetries,
	}
	for i := 0; i < workerCount; i++ {
		logger.Debugf("starting streamer worker thread #%d", i+1)
		s.workerWg.Add(1)
		go func() {
			defer s.workerWg.Done()
			s.doWork(batchSize, maxBatchDelay)
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
	select {
	case <-s.workerCtx.Done():
		return errors.New("write data into BQ streamer: streamer is already closed")
	case s.workerCh <- job:
		s.logger.Debug("inserted write job into bq streamer")
	}
	return nil
}

// doWork defines the main loop of a Streamer's worker goroutine.
func (s *Streamer) doWork(batchSize int, maxBatchDelay time.Duration) {
	rows := make([]interface{}, 0, batchSize)

	batchDelayTicker := time.NewTicker(maxBatchDelay)

	for {
		select {
		case <-s.workerCtx.Done():
			if len(rows) == 0 {
				s.logger.Debug("streamer worker thread closing: context is done")
				return
			}
			s.logger.Debug("streamer worker thread is closing: context is done: flushing batched rows first")

		case <-batchDelayTicker.C:
			if len(rows) == 0 {
				s.logger.Debug("streamer worker thread delay tick: no rows")
				continue
			}

		case job := <-s.workerCh:
			rows = append(rows, job.Data)
			if len(rows) < batchSize {
				continue // nothing to do, need to collect more
			}
		}

		s.writeRows(&rows)
		batchDelayTicker.Reset(maxBatchDelay)
	}
}

func (s *Streamer) writeRows(rowsRef *[]interface{}) {
	defer func() {
		*rowsRef = (*rowsRef)[:0]
	}()
	rows := *rowsRef

	operation := func() error {
		err := s.client.Put(rows)
		if err == nil {
			return nil
		}
		// ensure we only retry, so called retry-able errors
		if apiErr, ok := err.(*googleapi.Error); ok && (apiErr.Code == 500 || apiErr.Code == 503) {
			return err
		}
		// an error that won't be retried
		return backoff.Permanent(err)
	}
	ebo := backoff.NewExponentialBackOff()
	ebo.MaxElapsedTime = MaxTotalElapsedRetryTime
	// NOTE: we do not attach the context here, as once again,
	// our context might already be closed in case we are in a flushing state,
	// so this isn't desired. Oh no amigo.
	bo := backoff.WithMaxRetries(ebo, s.maxRetries)
	err := backoff.Retry(operation, bo)
	if err != nil {
		s.logger.Errorf("failed to write %d row(s) to BQ: %v", len(rows), err)
	} else {
		s.logger.Debugf("successfully wrote %d row(s) to BQ", len(rows))
	}
}

// Close closes the streamer and all its worker goroutines.
func (s *Streamer) Close() {
	s.logger.Debug("closing streamer")
	s.workerCancelFn()
	s.workerWg.Wait()
	err := s.client.Close()
	if err != nil {
		s.logger.Errorf("close streamer: close BQ client: %v", err)
	}
}
