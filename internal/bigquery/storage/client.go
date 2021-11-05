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

package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OTA-Insight/bqwriter/internal/bigquery/storage/managedwriter"
	"github.com/OTA-Insight/bqwriter/log"
)

// Client implements the standard/official BQ (cloud) Client,
// using the regular insertAll API with retry logic added on top of that. By default
// the workers will also batch its received rows rather than writing them one by one,
// this can be disabled by setting the batchSize value to the value of 1.
type Client struct {
	client *managedwriter.Client
	stream *managedwriter.ManagedStream

	ctx context.Context

	logger log.Logger
}

func NewClient(projectID, dataSetID, tableID string, maxRetries int, initialRetryDelay time.Duration, maxRetryDeadlineOffset time.Duration, retryDelayMultiplier float64, logger log.Logger) (*Client, error) {
	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	ctx := context.Background()

	writer, err := managedwriter.NewClient(
		ctx, projectID,
		maxRetries, initialRetryDelay, maxRetryDeadlineOffset, retryDelayMultiplier,
	)
	if err != nil {
		return nil, fmt.Errorf("BQ Storage Client creation: create managed writer: %w", err)
	}
	stream, err := writer.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(fmt.Sprintf(
			"projects/%s/datasets/%s/tables/%s",
			projectID, dataSetID, tableID,
		)),
		managedwriter.WithDataOrigin("OTA-Insight/bqwriter"),
	)
	if err != nil {
		if err := writer.Close(); err != nil {
			logger.Errorf("failed to close BQ Storage client that failed to create stream: %v", err)
		}
		return nil, fmt.Errorf("BQ Storage Client creation: create managed writer: %w", err)
	}
	return &Client{
		client: writer,
		stream: stream,
		ctx:    ctx,
		logger: logger,
	}, nil
}

// Put implements bigquery.Client::Put
func (bqc *Client) Put(data interface{}) (bool, error) {
	binaryData, err := bqc.encodeData(data)
	if err != nil {
		return false, fmt.Errorf("BQ Storage Client: Put Data: encode data: %w", err)
	}

	// TODO: do something with result
	// NOTE: we do not define an offset here,
	// as it would only be useful in case we want to do
	// diagnostics with them
	_, err = bqc.stream.AppendRows(bqc.ctx, binaryData, -1)
	if err != nil {
		return false, fmt.Errorf("BQ Storage Client: Stream: AppendRows: %w", err)
	}

	return false, nil
}

// Flush implements bigquery.Client::Flush
func (bqc *Client) Flush() error {
	// NOTE: flushing is only equired once we support non-default streams
	return nil
}

// Close implements b,fxigquery.Client::Close
func (bqc *Client) Close() error {
	if err := bqc.stream.Close(); err != nil {
		bqc.logger.Errorf("close BQ storage client: close stream: %w", err)
	}
	if err := bqc.client.Close(); err != nil {
		return fmt.Errorf("close BQ storage client: close internal storage writer client: %w", err)
	}
	return nil
}

func (bqc *Client) encodeData(data interface{}) ([][]byte, error) {
	// nolint: goerr113
	return nil, errors.New("TODO")
}
