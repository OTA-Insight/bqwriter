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

	"cloud.google.com/go/bigquery"
)

var (
	couldNotConvertReaderErr = errors.New("BQ batch client: could not convert data into io.Reader")
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
	autoDetect          bool
	ignoreUnknownValues bool
	csvOptions          *bigquery.CSVOptions
	writeDisposition    bigquery.TableWriteDisposition

	errors []*bigquery.Error
}

// NewClient creates a new Client.
func NewClient(projectID, dataSetID, tableID string, ignoreUnknownValues, autoDetect bool, sourceFormat bigquery.DataFormat, writeDisposition bigquery.TableWriteDisposition, schema *bigquery.Schema, csvOptions *bigquery.CSVOptions) (*Client, error) {
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
		ignoreUnknownValues, autoDetect,
		sourceFormat, writeDisposition,
		schema, csvOptions,
	)
}

func newClient(client *bigquery.Client, dataSetID, tableID string, ignoreUnknownValues, autoDetect bool, sourceFormat bigquery.DataFormat, writeDisposition bigquery.TableWriteDisposition, schema *bigquery.Schema, csvOptions *bigquery.CSVOptions) (*Client, error) {
	return &Client{
		client: client,

		dataSetID: dataSetID,
		tableID:   tableID,

		schema:              schema,
		sourceFormat:        sourceFormat,
		autoDetect:          autoDetect,
		ignoreUnknownValues: ignoreUnknownValues,
		csvOptions:          csvOptions,
		writeDisposition:    writeDisposition,
	}, nil
}

// Put implements bqClient::Put
func (bqc *Client) Put(data interface{}) (bool, error) {
	reader, ok := data.(io.Reader)
	if !ok {
		return false, couldNotConvertReaderErr
	}

	ctx := context.Background()
	source := bigquery.NewReaderSource(reader)
	source.AutoDetect = bqc.autoDetect
	source.SourceFormat = bqc.sourceFormat
	source.IgnoreUnknownValues = bqc.ignoreUnknownValues
	if bqc.csvOptions != nil {
		source.CSVOptions = *bqc.csvOptions
	}
	if bqc.schema != nil {
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
		bqc.errors = status.Errors
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

func (bqc Client) Errors() []*bigquery.Error {
	return bqc.errors
}
