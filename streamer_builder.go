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

package bqwriter

import (
	"context"
	"errors"
	"time"
)

// StreamerBuilder is used to build a Streamer (client).
// The builder pattern is used as to ensure the streamer is configured
// with sane defaults and with compatible configurations at all steps of the configuration.
//
// You can use the same builder to build many Steamer clients.
// This is however not intended or needed, given the Streamer can have multiple
// background workers active at the same time.
//
// Only required configuration is the project, data set and table identifiers,
// to which the data is to be written. All else is optional.
type StreamerBuilder struct {
	projectID string
	dataSetID string
	tableID   string

	workerCount     int
	workerQueueSize int

	retryConfig *WriteRetryConfig

	batchSize     int
	maxBatchDelay time.Duration

	clientInsertAllConfig bqClientInsertAllConfig

	logger Logger
}

// TODO: add StorageStreamerBuilder,
// which is created from a StreamerBuilder,
// in orde to be able to configure more advanced
// configurations for a Storage API, not required for a insertAll api :)

// bqClientInsertAllConfig is the config used by the StreamerBuilder
// in order to configure the InsertAll client used by default.
type bqClientInsertAllConfig struct {
	skipInvalidRows     bool
	ignoreUnknownValues bool
}

// NewStreamerBuilder creates a new StreamBuilder that is to be used to build a Streamer (client).
// The builder pattern is used as to ensure the streamer is configured
// with sane defaults and with compatible configurations at all steps of the configuration.
//
// The project, data set and table identifiers are all required. An error will be returned
// for the first one that is undefined (empty string).
func NewStreamerBuilder(projectID, dataSetID, tableID string) (*StreamerBuilder, error) {
	if projectID == "" {
		return nil, errors.New("NewStreamerBuilder: projectID is empty: should be defined")
	}
	if dataSetID == "" {
		return nil, errors.New("NewStreamerBuilder: dataSetID is empty: should be defined")
	}
	if tableID == "" {
		return nil, errors.New("NewStreamerBuilder: tableID is empty: should be defined")
	}

	return &StreamerBuilder{
		projectID: projectID,
		dataSetID: dataSetID,
		tableID:   tableID,

		workerCount:     DefaultWorkerCount,
		workerQueueSize: DefaultWorkerQueueSize,

		retryConfig: new(WriteRetryConfig),

		batchSize:     DefaultBatchSize,
		maxBatchDelay: DefaultMaxBatchDelay,

		logger: stdLogger{},
	}, nil
}

// WorkerCount defines the amount of workers to be used,
// each on their own goroutine and with an opt-out channel buffer per routine.
//
// Defaults to DefaultWorkerCount if passed in argument <= 0.
func (builder *StreamerBuilder) WorkerCount(n int) *StreamerBuilder {
	if n <= 0 {
		builder.workerCount = DefaultWorkerCount
	} else {
		builder.workerCount = n
	}
	return builder
}

// WorkerQueueSize defines the size of the job queue per worker used
// in order to allow the Streamer's users to write rows even if all workers are currently
// too busy to accept new incoming rows.
//
// Defaults to MaxTotalElapsedRetryTime if passed in argument <= 0.
func (builder *StreamerBuilder) WorkerQueueSize(n int) *StreamerBuilder {
	if n <= 0 {
		builder.workerQueueSize = DefaultWorkerQueueSize
	} else {
		builder.workerQueueSize = n
	}
	return builder
}

// WriteRetryConfig configures the config to be used for the Retry back off logic
// of the to be built Streamer (client). Passing in a nil value will effectively
// disable the retry logic.
//
// NOTE: this retry config is only used for a Storage API based Streamer Client,
// and has no affect on the InsertAll based Streamer client. The latter does use more
// or less the same code in the background. The difference being that for the InsertAll client
// these Retry configurations cannot be configured and are instead hardcoded to the Default
// ones as also found in this package.
func (builder *StreamerBuilder) WriteRetryConfig(cfg *WriteRetryConfig) *StreamerBuilder {
	builder.retryConfig = cfg
	return builder
}

// BatchSize defines the amount of rows (data) by a worker, prior to a worker
// actually writing it to BQ. Should a worker have rows left in its local cache when closing,
// it will flush/write these rows prior to closing.
//
// Defaults to DefaultBatchSize if n <= 0.
func (builder *StreamerBuilder) BatchSize(n int) *StreamerBuilder {
	if n <= 0 {
		builder.batchSize = DefaultBatchSize
	} else {
		builder.batchSize = n
	}
	return builder
}

// MaxBatchDelay defines the max amount of time a worker batches rows,
// prior to writing the batched rows, even when not yet full.
//
// Defaults to DefaultMaxBatchDelay if d == 0.
func (builder *StreamerBuilder) MaxBatchDelay(d time.Duration) *StreamerBuilder {
	if d == 0 {
		builder.maxBatchDelay = DefaultMaxBatchDelay
	} else {
		builder.maxBatchDelay = d
	}
	return builder
}

// ClientInsertAll configures the internal InsertAll BigQuery client.
// Note that this config is overwritten when ClientStorage is used/called.
//
// Internal API used: https://pkg.go.dev/cloud.google.com/go/bigquery
//
// Defaults the skipInvalidRows and ignoreUnknownValues values to False if this function is never called.
func (builder *StreamerBuilder) ClientInsertAll(skipInvalidRows, ignoreUnknownValues bool) *StreamerBuilder {
	builder.clientInsertAllConfig.skipInvalidRows = skipInvalidRows
	builder.clientInsertAllConfig.ignoreUnknownValues = ignoreUnknownValues
	return builder
}

// ClientStorage configures the internal Storage BigQuery client.
// Note that this config overwrites the default ClientInsertAll config, even when configured explicitly.
//
// Internal API used: https://pkg.go.dev/cloud.google.com/go/bigquery/storage/apiv1
//
// Defaults the ... TODO
func (builder *StreamerBuilder) ClientStorage() *StreamerBuilder {
	panic("TODO")
}

// Logger allows you to attach a logger to be used by the streamer,
// instead of the default built-in STDERR logging implementation.
//
// Defaults to the same built-in STDERR logging if logger == nil.
func (builder *StreamerBuilder) Logger(logger Logger) *StreamerBuilder {
	if logger == nil {
		builder.logger = stdLogger{}
	} else {
		builder.logger = logger
	}
	return builder
}

// BuildStreamer builds a new Streamer Client using the configuration stored internally within the builder
// and which was optionally configured by the user of this StreamerBuilder instance.
//
// An error is returned in case the Streamer Client couldn't be created for some unexpected reason,
// most likely something going wrong within the layer of actually interacting with GCloud.
func (builder *StreamerBuilder) BuildStreamer(ctx context.Context) (*Streamer, error) {
	// TODO: support also storage client
	builderFunc := func(context.Context) (bqClient, error) {
		return newStdBQInsertAllThickClient(
			builder.projectID, builder.dataSetID, builder.tableID,
			builder.clientInsertAllConfig.skipInvalidRows, builder.clientInsertAllConfig.ignoreUnknownValues,
			builder.batchSize,
			builder.logger,
		)
	}
	return newStreamerWithClientBuilder(
		ctx, builderFunc,
		builder.workerCount, builder.workerQueueSize,
		builder.maxBatchDelay,
		builder.logger,
	)
}
