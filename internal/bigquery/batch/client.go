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

package batch

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/OTA-Insight/bqwriter/log"

	"cloud.google.com/go/bigquery"
)

var (
	errCouldNotConvertReader = errors.New("BQ batch client: could not convert data into io.Reader")
)

// Client implements the standard/official BQ (cloud) Client,
// using the regular insertAll API with retry logic added on top of that. By default
// the workers will also batch its received rows rather than writing them one by one,
// this can be disabled by setting the batchSize value to the value of 1.
type Client struct {
	client *bigquery.Client

	dataSetID string
	tableID   string

	schema              *bigquery.Schema
	sourceFormat        bigquery.DataFormat
	ignoreUnknownValues bool
	writeDisposition    bigquery.TableWriteDisposition

	logger log.Logger
}

// NewClient creates a new Client.
func NewClient(projectID, dataSetID, tableID string, ignoreUnknownValues bool, sourceFormat bigquery.DataFormat, writeDisposition bigquery.TableWriteDisposition, schema *bigquery.Schema, logger log.Logger) (*Client, error) {
	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("BQ batch client: creation failed %w", err)
	}

	return newClient(
		client, dataSetID, tableID,
		ignoreUnknownValues,
		sourceFormat, writeDisposition,
		schema, logger,
	)
}
func newClient(client *bigquery.Client, dataSetID, tableID string, ignoreUnknownValues bool, sourceFormat bigquery.DataFormat, writeDisposition bigquery.TableWriteDisposition, schema *bigquery.Schema, logger log.Logger) (*Client, error) {
	return &Client{
		client: client,

		dataSetID: dataSetID,
		tableID:   tableID,

		schema:              schema,
		sourceFormat:        sourceFormat,
		ignoreUnknownValues: ignoreUnknownValues,
		writeDisposition:    writeDisposition,

		logger: logger,
	}, nil
}

// Put implements bqClient::Put
func (bqc *Client) Put(data interface{}) (bool, error) {
	reader, ok := data.(io.Reader)
	if !ok {
		return false, errCouldNotConvertReader
	}

	ctx := context.Background()
	source := bigquery.NewReaderSource(reader)
	source.SourceFormat = bqc.sourceFormat
	source.IgnoreUnknownValues = bqc.ignoreUnknownValues

	if bqc.schema == nil {
		source.AutoDetect = true
	} else {
		source.Schema = *bqc.schema
	}

	table := bqc.client.Dataset(bqc.dataSetID).Table(bqc.tableID)
	loader := table.LoaderFrom(source)
	loader.WriteDisposition = bqc.writeDisposition
	job, err := loader.Run(ctx)
	if err != nil {
		return false, fmt.Errorf("BQ batch client: failed to run loader %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return false, fmt.Errorf("BQ batch client: job failed while waiting %w", err)
	}

	if err := status.Err(); err != nil {
		for _, statErr := range status.Errors {
			bqc.logger.Errorf("BQ batch client: status error: %w", statErr)
		}
		return false, fmt.Errorf("BQ batch client: job returned an error status %w", err)
	}

	// We flush every time when we write data.
	return true, nil
}

// Flush implements bigquery.Client::Flush
func (bqc *Client) Flush() error {
	// NOTE: The data is always flushed instantly upon putting the data.
	return nil
}

// Close implements bqClient::Close
func (bqc *Client) Close() error {
	// no need to flush first,
	// as this is an internal client used by Streamer only,
	// which does flush prior to closing it :)
	if err := bqc.client.Close(); err != nil {
		return fmt.Errorf("BQ batch client: failed while closing %w", err)
	}
	return nil
}
