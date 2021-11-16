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

	"cloud.google.com/go/bigquery"
)

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
		return fmt.Errorf("put data into BQ using google-API insertAll: %w", err)
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
