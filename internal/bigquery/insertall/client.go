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

	"github.com/OTA-Insight/bqwriter/constant"
	"github.com/OTA-Insight/bqwriter/internal"
	internalBQ "github.com/OTA-Insight/bqwriter/internal/bigquery"
	"github.com/OTA-Insight/bqwriter/log"

	"google.golang.org/api/option"
)

// Client implements the standard/official BQ (cloud) Client,
// using the regular insertAll API with retry logic added on top of that. By default
// the workers will also batch its received rows rather than writing them one by one,
// this can be disabled by setting the batchSize value to the value of 1.
type Client struct {
	client bqClient

	logger log.Logger

	rows      []interface{}
	batchSize int
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

// NewClient creates a new Client.
func NewClient(projectID, dataSetID, tableID string, skipInvalidRows, ignoreUnknownValues bool, batchSize int, logger log.Logger, opts ...option.ClientOption) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("bq insertAll client creation: validate projectID: %w: missing", internal.ErrInvalidParam)
	}
	if dataSetID == "" {
		return nil, fmt.Errorf("bq insertAll client creation: validate dataSetID: %w: missing", internal.ErrInvalidParam)
	}
	if tableID == "" {
		return nil, fmt.Errorf("bq insertAll client creation: validate tableID: %w: missing", internal.ErrInvalidParam)
	}
	client, err := newStdBQClient(projectID, dataSetID, tableID, skipInvalidRows, ignoreUnknownValues, opts...)
	if err != nil {
		return nil, err
	}
	return newClient(client, batchSize, logger)
}

func newClient(client bqClient, batchSize int, logger log.Logger) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("bq insertAll client creation: validate client: %w: missing", internal.ErrInvalidParam)
	}
	if logger == nil {
		return nil, fmt.Errorf("bq insertAll client creation: logger client: %w: missing", internal.ErrInvalidParam)
	}
	if batchSize <= 0 {
		batchSize = constant.DefaultBatchSize
	}
	return &Client{
		client: client,

		logger: logger,

		rows:      make([]interface{}, 0, batchSize),
		batchSize: batchSize,
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
	// background ctx is used, as we always want to flush unwritten rows, even if parent ctx is done,
	// we do retry on top of the default BigQuery logic
	// (as defined in https://github.com/googleapis/google-cloud-go/blob/b288668e0b6a66df92ca9115f759bc0670e6e822/bigquery/bigquery.go#L172-L239)
	// for the only reason that they choose to not retry 5xx internal errors as these indicate tmp issues with BigQuery that could
	// be considered as a bug in BigQuery layer instead. In the issue https://github.com/googleapis/google-cloud-go/issues/3792
	// someomeone reported the same issue from which it is clear that this won't be supported by google apis out of the box,
	// as such we support it here
	retryer := internalBQ.NewRetryer(
		context.Background(),
		-1,
		constant.DefaultInitialRetryDelay,
		constant.DefaultMaxRetryDeadlineOffset,
		constant.DefaultRetryDelayMultiplier,
		internalBQ.HttpInternalErrorFilter,
	)
	err = retryer.RetryOp(func(ctx context.Context) error {
		//nolint: wrapcheck
		return bqc.client.Put(ctx, bqc.rows)
	})
	if err != nil {
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
