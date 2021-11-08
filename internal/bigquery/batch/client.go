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

	"github.com/OTA-Insight/bqwriter/internal"

	"cloud.google.com/go/bigquery"
)

var (
	autoDetectSchemaNotSupported = errors.New("usage of autodetect being true and schema being passed is not supported")
	csvOptionsNotAllowed         = errors.New("csvOptions is only supported if the sourceFormat is CSV")
	couldNotConvertReader        = errors.New("could not convert data into io.Reader")
	schemaRequired               = errors.New("schema is required if autoDetect is false")
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
func NewClient(projectID, dataSetID, tableID string, ignoreUnknownValues, autoDetect bool, sourceFormat bigquery.DataFormat, writeDisposition *bigquery.TableWriteDisposition, schema *bigquery.Schema, csvOptions *bigquery.CSVOptions) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("bq insertAll client creation: validate projectID: %w: missing", internal.InvalidParamErr)
	}
	if dataSetID == "" {
		return nil, fmt.Errorf("bq insertAll client creation: validate dataSetID: %w: missing", internal.InvalidParamErr)
	}
	if tableID == "" {
		return nil, fmt.Errorf("bq insertAll client creation: validate tableID: %w: missing", internal.InvalidParamErr)
	}

	// If autoDetect is passed while the format is not JSON or CSV, error as this is only supported for csv.
	if autoDetect && (sourceFormat != bigquery.JSON && sourceFormat != bigquery.CSV) {
		return nil, fmt.Errorf("autoDetect is not supported for the sourceFormat %s", sourceFormat)
	}

	// If autoDetect is passed and both the schema is passed error as these are mutually exclusive.
	if autoDetect && schema != nil {
		return nil, autoDetectSchemaNotSupported
	}

	if !autoDetect && schema == nil {
		return nil, schemaRequired
	}

	// csvOptions is only allowed if the sourceFormat is CSV.
	if csvOptions != nil && sourceFormat != bigquery.CSV {
		return nil, csvOptionsNotAllowed
	}

	tableWriteDisposition := bigquery.WriteAppend
	if writeDisposition != nil {
		tableWriteDisposition = *writeDisposition
	}

	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("create BQ batch Client: %w", err)
	}

	return &Client{
		client: client,

		dataSetID: dataSetID,
		tableID:   tableID,

		schema:              schema,
		sourceFormat:        sourceFormat,
		autoDetect:          autoDetect,
		ignoreUnknownValues: ignoreUnknownValues,
		csvOptions:          csvOptions,
		writeDisposition:    tableWriteDisposition,
	}, nil
}

// Put implements bqClient::Put
func (bqc *Client) Put(data interface{}) (bool, error) {
	reader, ok := data.(io.Reader)
	if !ok {
		return false, couldNotConvertReader
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
		return false, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return false, err
	}

	if err := status.Err(); err != nil {
		bqc.errors = status.Errors
		return false, err
	}

	// we flush every time we write data.
	return true, nil
}

// Flush implements bigquery.Client::Flush
func (bqc *Client) Flush() error {
	// NOTE: flushing is only equired once we support non-default streams
	return nil
}

// Close implements bqClient::Close
func (bqc *Client) Close() error {
	// no need to flush first,
	// as this is an internal client used by Streamer only,
	// which does flush prior to closing it :)
	if err := bqc.client.Close(); err != nil {
		return fmt.Errorf("close thick batch BQ client: %w", err)
	}
	return nil
}

func (bqc Client) Errors() []*bigquery.Error {
	return bqc.errors
}
