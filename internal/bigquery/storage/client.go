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
	"io"
	"sync"

	"github.com/OTA-Insight/bqwriter/internal"
	"github.com/OTA-Insight/bqwriter/internal/bigquery/storage/encoding"
	"github.com/OTA-Insight/bqwriter/log"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TODO (if desired by someone):
// - support ComittedStream: it is similar to the DefaultStream,
//   but also allows offset tracking as a dedicated stream is created for it;
// - support PendingStream: here no data is made visible until made explicitly,
//   which we could allow by for example finalizing a stream after x delay or y rows,
//   and creating a new stream after that;

// Client implements the standard/official BQ (cloud) Client,
// using the regular insertAll API with retry logic added on top of that. By default
// the workers will also batch its received rows rather than writing them one by one,
// this can be disabled by setting the batchSize value to the value of 1.
type Client struct {
	client *managedwriter.Client
	stream *managedwriter.ManagedStream

	encoder encoding.Encoder

	ctx context.Context

	wg             sync.WaitGroup
	appendResultCh chan *managedwriter.AppendResult

	logger log.Logger
}

// NewClient creates a new BQ Storage Client.
// See the documentation of Client for more information how to use it.
func NewClient(projectID, dataSetID, tableID string, encoder encoding.Encoder, dp *descriptorpb.DescriptorProto, logger log.Logger) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("bq storage client creation: validate projectID: %w: missing", internal.ErrInvalidParam)
	}
	if dataSetID == "" {
		return nil, fmt.Errorf("bq storage client creation: validate dataSetID: %w: missing", internal.ErrInvalidParam)
	}
	if tableID == "" {
		return nil, fmt.Errorf("bq storage client creation: validate tableID: %w: missing", internal.ErrInvalidParam)
	}
	if encoder == nil {
		return nil, fmt.Errorf("bq storage client creation: validate encoder: %w: missing", internal.ErrInvalidParam)
	}
	if dp == nil {
		return nil, fmt.Errorf("bq storage client creation: validate dp (DescriptorProto): %w: missing", internal.ErrInvalidParam)
	}

	// NOTE: we are using the background Context,
	// as to ensure that we can always write to the client,
	// even when the actual parent context is already done.
	// This is a requirement given the streamer will batch its rows.
	ctx := context.Background()

	writer, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("BQ Storage Client creation: create managed writer: %w", err)
	}
	writerOpts := []managedwriter.WriterOption{
		managedwriter.WithDestinationTable(fmt.Sprintf(
			"projects/%s/datasets/%s/tables/%s",
			projectID, dataSetID, tableID,
		)),
		managedwriter.WithDataOrigin("OTA-Insight/bqwriter"),
		managedwriter.WithType(managedwriter.DefaultStream),
	}
	if dp != nil {
		writerOpts = append(writerOpts, managedwriter.WithSchemaDescriptor(dp))
	}
	stream, err := writer.NewManagedStream(ctx, writerOpts...)
	if err != nil {
		if err := writer.Close(); err != nil {
			logger.Errorf("failed to close BQ Storage client that failed to create stream: %v", err)
		}
		return nil, fmt.Errorf("BQ Storage Client creation: create managed writer: %w", err)
	}

	// create storage client
	client := &Client{
		client:         writer,
		stream:         stream,
		encoder:        encoder,
		ctx:            ctx,
		appendResultCh: make(chan *managedwriter.AppendResult, 1),
		logger:         logger,
	}

	// spawn a worker goroutine,
	// in order to check the append results in the background
	client.wg.Add(1)
	go func() {
		defer client.wg.Done()
		client.checkAppendResultsAsync()
	}()

	// return the client, successfully created
	return client, nil
}

// Put implements bigquery.Client::Put
func (bqc *Client) Put(data interface{}) (bool, error) {
	binaryData, err := bqc.encoder.EncodeRows(data)
	if err != nil {
		return false, fmt.Errorf("BQ Storage Client: Put Data: encode data: %w", err)
	}

	// NOTE: we do not define an offset here,
	// as it would only be useful in case we want to do
	// diagnostics with them. Once we would support CommittedStream than
	// we do want to use the offset for tracking purposes.
	// NOTE 2: default streams can not have an option set,
	// in the past we used "NoStreamOffset" for this, but the API has evolved to options,
	// and thuse we can rely on the default for our purposes.
	result, err := bqc.stream.AppendRows(bqc.ctx, binaryData)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("BQ Storage Client: Stream: AppendRows: %w", err)
	}
	bqc.appendResultCh <- result
	// we flush every time we write data,
	// as the default stream commits immediately
	return true, nil
}

func (bqc *Client) checkAppendResultsAsync() {
	var results []*managedwriter.AppendResult
	defer func() {
		for _, result := range results {
			select {
			case <-result.Ready():
				_, err := result.GetResult(context.Background())
				if err != nil {
					if isCanceledGRPCError(err) {
						bqc.logger.Debugf("exit checkAppendResultsAsync: ready append resulted in error: %v", err)
					} else {
						bqc.logger.Errorf("exit checkAppendResultsAsync: ready append resulted in error: %v", err)
					}
				}
			default:
				bqc.logger.Debug("append result not yet ready: checkAppendResultsAsync exited anyway")
			}
		}
	}()
	for {
		select {
		case <-bqc.ctx.Done():
			return

		case result, ok := <-bqc.appendResultCh:
			if !ok {
				return
			}
			results = append(results, result)
			// go through all results, and filter out the ready ones
			n := 0
			for _, result := range results {
				select {
				case <-result.Ready():
					_, err := result.GetResult(context.Background())
					if err != nil {
						if isCanceledGRPCError(err) {
							bqc.logger.Debugf("ready append resulted in error: %v", err)
						} else {
							bqc.logger.Errorf("ready append resulted in error: %v", err)
						}
					}
				default:
					// keep result until next time
					results[n] = result
					n++
				}
			}
			results = results[:n]
		}
	}
}

func isCanceledGRPCError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	code := st.Code()
	return code == codes.Canceled || code == codes.Unavailable
}

// Flush implements bigquery.Client::Flush
func (bqc *Client) Flush() error {
	// NOTE: flushing is only equired once we support non-default streams
	return nil
}

// Close implements b,fxigquery.Client::Close
func (bqc *Client) Close() error {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			bqc.logger.Errorf("close BQ storage client: close internal append result ch: %v", panicErr)
		}
	}()
	if err := bqc.stream.Close(); err != nil && !errors.Is(err, io.EOF) {
		bqc.logger.Errorf("close BQ storage client: close stream: %v", err)
	}
	if err := bqc.client.Close(); err != nil {
		return fmt.Errorf("close BQ storage client: close internal storage writer client: %w", err)
	}
	close(bqc.appendResultCh)
	bqc.wg.Wait()
	return nil
}
