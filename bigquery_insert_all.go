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
	"fmt"

	"cloud.google.com/go/bigquery"
)

// bqInsertAllThickClient implements the standard/official BQ (cloud) Client,
// using the regular insertAll API with retry logic added on top of that. By default
// the workers will also batch its received rows rather than writing them one by one,
// this can be disabled by setting the batchSize value to the value of 1.
type bqInsertAllThickClient struct {
	client bqInsertAllClient

	logger Logger

	rows      []interface{}
	batchSize int
}

// bqInsertAllClient defines the API we expect from the BQ InsertAll client,
// allowing it to be stubbed for testing purposes as well.
type bqInsertAllClient interface {
	// Put uploads one or more rows to the BigQuery service.
	Put(ctx context.Context, data interface{}) error
	// Close closes any resources held by the client.
	// Close should be called when the client is no longer needed.
	// It need not be called at program exit.
	Close() error
}

// stdBQInsertAllClient impements bqInsertAllClient using the official Golang Gcloud BigQuery API client.
type stdBQInsertAllClient struct {
	client *bigquery.Client

	dataSetID string
	tableID   string

	skipInvalidRows     bool
	ignoreUnknownValues bool
}

// Put implements bqInsertAllClient::Put
func (bqc *stdBQInsertAllClient) Put(ctx context.Context, data interface{}) error {
	inserter := bqc.client.Dataset(bqc.dataSetID).Table(bqc.tableID).Inserter()
	inserter.SkipInvalidRows = bqc.skipInvalidRows
	inserter.IgnoreUnknownValues = bqc.ignoreUnknownValues
	return inserter.Put(ctx, data)
}

// Close implements bqInsertAllClient::Close
func (bqc *stdBQInsertAllClient) Close() error {
	return bqc.client.Close()
}

// newStdBQInsertAllClient creates a new stdBQInsertAllClient,
// a production-ready implementation of bqInsertAllClient.
func newStdBQInsertAllClient(projectID, dataSetID, tableID string, skipInvalidRows, ignoreUnknownValues bool) (*stdBQInsertAllClient, error) {
	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("create BQ Insert All Client: %w", err)
	}
	return &stdBQInsertAllClient{
		client: client,

		dataSetID: dataSetID,
		tableID:   tableID,

		skipInvalidRows:     skipInvalidRows,
		ignoreUnknownValues: ignoreUnknownValues,
	}, nil
}

// newStdBQInsertAllThickClient creates a new bqInsertAllThickClient.
func newStdBQInsertAllThickClient(projectID, dataSetID, tableID string, skipInvalidRows, ignoreUnknownValues bool, batchSize int, logger Logger) (*bqInsertAllThickClient, error) {
	if projectID == "" {
		return nil, errors.New("NewStreamer: projectID is empty: should be defined")
	}
	if dataSetID == "" {
		return nil, errors.New("NewStreamer: dataSetID is empty: should be defined")
	}
	if tableID == "" {
		return nil, errors.New("NewStreamer: tableID is empty: should be defined")
	}
	client, err := newStdBQInsertAllClient(projectID, dataSetID, tableID, skipInvalidRows, ignoreUnknownValues)
	if err != nil {
		return nil, err
	}
	return newBQInsertAllThickClient(client, batchSize, logger)
}

func newBQInsertAllThickClient(client bqInsertAllClient, batchSize int, logger Logger) (*bqInsertAllThickClient, error) {
	if client == nil {
		return nil, errors.New("BQ Insert All Client expected")
	}
	if logger == nil {
		return nil, errors.New("BQ InsertAll Client: Logger expected")
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &bqInsertAllThickClient{
		client: client,

		logger: logger,

		rows:      make([]interface{}, 0, batchSize),
		batchSize: batchSize,
	}, nil
}

// Put implements bqClient::Put
func (bqc *bqInsertAllThickClient) Put(data interface{}) (bool, error) {
	bqc.rows = append(bqc.rows, data)
	if len(bqc.rows) < bqc.batchSize {
		return false, nil // batch not yet full, nothing more to do
	}
	// batch max size has been reached, write all data to BQ,
	// optionally retrying for retry-able failures as well
	return true, bqc.Flush()
}

// Flush implements bqClient::Flush
func (bqc *bqInsertAllThickClient) Flush() (err error) {
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
	return bqc.putRows(context.Background()) // background ctx is used, as we always want to flush unwritten rows, even if parent ctx is done
}

func (bqc *bqInsertAllThickClient) putRows(ctx context.Context) error {
	return bqc.client.Put(ctx, bqc.rows)
}

// Close implements bqClient::Close
func (bqc *bqInsertAllThickClient) Close() error {
	// no need to flush first,
	// as this is an internal client used by Streamer only,
	// which does flush prior to closing it :)
	return bqc.client.Close()
}
