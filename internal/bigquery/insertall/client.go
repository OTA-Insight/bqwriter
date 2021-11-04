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

package insertall

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/OTA-Insight/bqwriter/constant"
	"github.com/OTA-Insight/bqwriter/internal"
	"github.com/OTA-Insight/bqwriter/log"
)

// TODO: move to internal/bigquery/insertall

// Client implements the standard/official BQ (cloud) Client,
// using the regular insertAll API with retry logic added on top of that. By default
// the workers will also batch its received rows rather than writing them one by one,
// this can be disabled by setting the batchSize value to the value of 1.
type Client struct {
	client bqClient

	logger log.Logger

	rows      []interface{}
	batchSize int

	maxRetryDeadlineOffset time.Duration
}

// bqClient defines the API we expect from the BQ InsertAll client,
// allowing it to be stubbed for testing purposes as well.
type bqClient interface {
	// Put uploads one or more rows to the BigQuery service.
	Put(ctx context.Context, data interface{}) error
	// Close closes any resources held by the client.
	// Close should be called when the client is no longer needed.
	// It need not be called at program exit.
	Close() error
}

// stdBQClient impements bqClient using the official Golang Gcloud BigQuery API client.
type stdBQClient struct {
	client *bigquery.Client

	dataSetID string
	tableID   string

	skipInvalidRows     bool
	ignoreUnknownValues bool
}

// Put implements bqClient::Put
func (bqc *stdBQClient) Put(ctx context.Context, data interface{}) error {
	inserter := bqc.client.Dataset(bqc.dataSetID).Table(bqc.tableID).Inserter()
	inserter.SkipInvalidRows = bqc.skipInvalidRows
	inserter.IgnoreUnknownValues = bqc.ignoreUnknownValues
	if err := inserter.Put(ctx, data); err != nil {
		return fmt.Errorf("put data into BQ using google-API inertAll: %w", err)
	}
	return nil
}

// Close implements bqClient::Close
func (bqc *stdBQClient) Close() error {
	if err := bqc.client.Close(); err != nil {
		return fmt.Errorf("close BQ google-API insertAll client: %w", err)
	}
	return nil
}

// newStdBQClient creates a new Client,
// a production-ready implementation of bqClient.
func newStdBQClient(projectID, dataSetID, tableID string, skipInvalidRows, ignoreUnknownValues bool) (*stdBQClient, error) {
	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("create BQ Insert All Client: %w", err)
	}
	return &stdBQClient{
		client: client,

		dataSetID: dataSetID,
		tableID:   tableID,

		skipInvalidRows:     skipInvalidRows,
		ignoreUnknownValues: ignoreUnknownValues,
	}, nil
}

// NewClient creates a new Client.
func NewClient(projectID, dataSetID, tableID string, skipInvalidRows, ignoreUnknownValues bool, batchSize int, maxRetryDeadlineOffset time.Duration, logger log.Logger) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("thick client creation: validate projectID: %w: missing", internal.InvalidParamErr)
	}
	if dataSetID == "" {
		return nil, fmt.Errorf("thick client creation: validate dataSetID: %w: missing", internal.InvalidParamErr)
	}
	if tableID == "" {
		return nil, fmt.Errorf("thick client creation: validate tableID: %w: missing", internal.InvalidParamErr)
	}
	client, err := newStdBQClient(projectID, dataSetID, tableID, skipInvalidRows, ignoreUnknownValues)
	if err != nil {
		return nil, err
	}
	return newClient(client, batchSize, maxRetryDeadlineOffset, logger)
}

func newClient(client bqClient, batchSize int, maxRetryDeadlineOffset time.Duration, logger log.Logger) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("thick client creation: validate client: %w: missing", internal.InvalidParamErr)
	}
	if logger == nil {
		return nil, fmt.Errorf("thick client creation: logger client: %w: missing", internal.InvalidParamErr)
	}
	if batchSize <= 0 {
		batchSize = constant.DefaultBatchSize
	}
	if maxRetryDeadlineOffset == 0 {
		maxRetryDeadlineOffset = constant.DefaultMaxRetryDeadlineOffset
	}
	return &Client{
		client: client,

		logger: logger,

		rows:      make([]interface{}, 0, batchSize),
		batchSize: batchSize,

		maxRetryDeadlineOffset: maxRetryDeadlineOffset,
	}, nil
}

// Put implements bqClient::Put
func (bqc *Client) Put(data interface{}) (bool, error) {
	bqc.rows = append(bqc.rows, data)
	if len(bqc.rows) < bqc.batchSize {
		return false, nil // batch not yet full, nothing more to do
	}
	// batch max size has been reached, write all data to BQ,
	// optionally retrying for retry-able failures as well
	return true, bqc.Flush()
}

// Flush implements bqClient::Flush
func (bqc *Client) Flush() (err error) {
	if len(bqc.rows) == 0 {
		return nil // nothing to do :)
	}
	// ensure at the end we clear out our written rows,
	// we could return the unwritten rows with the errors,
	// and that is what an early prototype of this library did,
	// however that would require an additional public method or channel
	// added to the Streamer API in order to be able to communicate these kind
	// of errors back to the client, given that these kind of errors
	// haven on worker goroutines.
	defer func() {
		if err != nil {
			bqc.logger.Errorf("BQ InsertAll Client: Flush: dropping %d row(s) due to error: %v", len(bqc.rows), err)
		}
		// empty rows, written or not,
		// such that we can start inserting new rows
		bqc.rows = bqc.rows[:0]
	}()
	// retry logic is to be implemented by the actual BQ (inserAll) client,
	// it certainly is the case for the actual one used
	//
	// background ctx is used, as we always want to flush unwritten rows, even if parent ctx is done
	// we do wrap it with a deadline context to ensure we get a correct deadline
	ctx, cancelFunc := context.WithTimeout(context.Background(), bqc.maxRetryDeadlineOffset)
	defer cancelFunc()
	if err := bqc.client.Put(ctx, bqc.rows); err != nil {
		return fmt.Errorf("thick insertAll BQ client: put batched rows (count=%d): %w", len(bqc.rows), err)
	}
	return nil
}

// Close implements bqClient::Close
func (bqc *Client) Close() error {
	// no need to flush first,
	// as this is an internal client used by Streamer only,
	// which does flush prior to closing it :)
	if err := bqc.client.Close(); err != nil {
		return fmt.Errorf("close thick insertAll BQ client: %w", err)
	}
	return nil
}
