// Copyright 2021 OTA Insight Ltd
// Copyright 2021 Google LLC
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
	"runtime"
	"strings"
	"time"

	"github.com/OTA-Insight/bqwriter/internal"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// enables testing
type streamClientFunc func(context.Context, ...gax.CallOption) (storagepb.BigQueryWrite_AppendRowsClient, error)

// Client is a managed BigQuery Storage write client scoped to a single project.
type Client struct {
	rawClient *storage.BigQueryWriteClient

	projectID string

	maxRetries             int
	initialRetryDelay      time.Duration
	maxRetryDeadlineOffset time.Duration
	retryDelayMultiplier   float64
}

// NewClient instantiates a new client.
func NewClient(ctx context.Context, projectID string, maxRetries int, initialRetryDelay time.Duration, maxRetryDeadlineOffset time.Duration, retryDelayMultiplier float64, opts ...option.ClientOption) (c *Client, err error) {
	if projectID == "" {
		return nil, fmt.Errorf("BQ: Storage: NewClient: projectID not defined: %w", internal.InvalidParamErr)
	}
	numConns := runtime.GOMAXPROCS(0)
	if numConns > 4 {
		numConns = 4
	}
	o := []option.ClientOption{
		option.WithGRPCConnectionPool(numConns),
	}
	o = append(o, opts...)

	rawClient, err := storage.NewBigQueryWriteClient(ctx, o...)
	if err != nil {
		return nil, fmt.Errorf("BQStorage: NewClient: %w", err)
	}

	return &Client{
		rawClient: rawClient,

		projectID: projectID,

		maxRetries:             maxRetries,
		initialRetryDelay:      initialRetryDelay,
		maxRetryDeadlineOffset: maxRetryDeadlineOffset,
		retryDelayMultiplier:   retryDelayMultiplier,
	}, nil
}

// Close releases resources held by the client.
func (c *Client) Close() error {
	// TODO: consider if we should propagate a cancellation from client to all associated managed streams.
	if c.rawClient == nil {
		// nolint: goerr113
		return errors.New("already closed")
	}
	c.rawClient.Close()
	c.rawClient = nil
	return nil
}

// NewManagedStream establishes a new managed stream for appending data into a table.
//
// Context here is retained for use by the underlying streaming connections the managed stream may create.
func (c *Client) NewManagedStream(ctx context.Context, opts ...WriterOption) (*ManagedStream, error) {
	return c.buildManagedStream(ctx, c.rawClient.AppendRows, false, opts...)
}

func (c *Client) buildManagedStream(ctx context.Context, streamFunc streamClientFunc, skipSetup bool, opts ...WriterOption) (*ManagedStream, error) {
	ctx, cancel := context.WithCancel(ctx)

	settings := defaultStreamSettings()
	settings.MaxRetries = c.maxRetries
	settings.InitialRetryDelay = c.initialRetryDelay
	settings.MaxRetryDeadlineOffset = c.maxRetryDeadlineOffset
	settings.RetryDelayMultiplier = c.retryDelayMultiplier

	ms := &ManagedStream{
		streamSettings: settings,
		c:              c.rawClient,
		ctx:            ctx,
		cancel:         cancel,
		open: func(streamID string) (storagepb.BigQueryWrite_AppendRowsClient, error) {
			arc, err := streamFunc(
				// Bidi Streaming doesn't append stream ID as request metadata, so we must inject it manually.
				metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", fmt.Sprintf("write_stream=%s", streamID)),
				gax.WithGRPCOptions(grpc.MaxCallRecvMsgSize(10*1024*1024)))
			if err != nil {
				return nil, err
			}
			return arc, nil
		},
	}

	// apply writer options
	for _, opt := range opts {
		opt(ms)
	}

	// skipSetup exists for testing scenarios.
	if !skipSetup {
		if err := c.validateOptions(ctx, ms); err != nil {
			return nil, err
		}

		if ms.streamSettings.streamID == "" {
			// not instantiated with a stream, construct one.
			streamName := fmt.Sprintf("%s/_default", ms.destinationTable)
			if ms.streamSettings.streamType != DefaultStream {
				// For everything but a default stream, we create a new stream on behalf of the user.
				req := &storagepb.CreateWriteStreamRequest{
					Parent: ms.destinationTable,
					WriteStream: &storagepb.WriteStream{
						Type: StreamTypeToEnum(ms.streamSettings.streamType),
					}}
				resp, err := ms.c.CreateWriteStream(ctx, req)
				if err != nil {
					return nil, fmt.Errorf("couldn't create write stream: %w", err)
				}
				streamName = resp.GetName()
			}
			ms.streamSettings.streamID = streamName
		}
	}
	if ms.streamSettings != nil {
		// if ms.ctx != nil {
		// 	ms.ctx = keyContextWithTags(ms.ctx, ms.streamSettings.streamID, ms.streamSettings.dataOrigin)
		// }
		ms.fc = newFlowController(ms.streamSettings.MaxInflightRequests, ms.streamSettings.MaxInflightBytes)
	} else {
		ms.fc = newFlowController(0, 0)
	}
	return ms, nil
}

// validateOptions is used to validate that we received a sane/compatible set of WriterOptions
// for constructing a new managed stream.
func (c *Client) validateOptions(ctx context.Context, ms *ManagedStream) error {
	if ms == nil {
		return fmt.Errorf("no managed stream definition: %w", internal.InvalidParamErr)
	}
	if ms.streamSettings.streamID != "" {
		// User supplied a stream, we need to verify it exists.
		info, err := c.getWriteStream(ctx, ms.streamSettings.streamID)
		if err != nil {
			return fmt.Errorf("BQ Storage: validate options: a streamname was specified, but lookup of stream failed: %w", err)
		}
		// update type and destination based on stream metadata
		ms.streamSettings.streamType = StreamType(info.Type.String())
		ms.destinationTable = TableParentFromStreamName(ms.streamSettings.streamID)
	}
	if ms.destinationTable == "" {
		return fmt.Errorf("no destination table specified: %w", internal.InvalidParamErr)
	}
	// auto-select DEFAULT here
	if ms.StreamType() == "" {
		ms.streamSettings.streamType = DefaultStream
	}
	return nil
}

// BatchCommitWriteStreams atomically commits a group of PENDING streams that belong to the same
// parent table.
//
// Streams must be finalized before commit and cannot be committed multiple
// times. Once a stream is committed, data in the stream becomes available
// for read operations.
func (c *Client) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	resp, err := c.rawClient.BatchCommitWriteStreams(ctx, req, opts...)
	if err != nil {
		return nil, fmt.Errorf("BQ Storage Client: BatchCommitWriteStreams: %w", err)
	}
	return resp, nil
}

// CreateWriteStream creates a write stream to the given table.
// Additionally, every table has a special stream named ‘_default’
// to which data can be written. This stream doesn’t need to be created using
// CreateWriteStream. It is a stream that can be used simultaneously by any
// number of clients. Data written to this stream is considered committed as
// soon as an acknowledgement is received.
func (c *Client) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	stream, err := c.rawClient.CreateWriteStream(ctx, req, opts...)
	if err != nil {
		return nil, fmt.Errorf("BQ Storage Client: CreateWriteStream: %w", err)
	}
	return stream, nil
}

// getWriteStream returns information about a given write stream.
//
// It's primarily used for setup validation, and not exposed directly to end users.
func (c *Client) getWriteStream(ctx context.Context, streamName string) (*storagepb.WriteStream, error) {
	req := &storagepb.GetWriteStreamRequest{
		Name: streamName,
	}
	// nolint: wrapcheck
	return c.rawClient.GetWriteStream(ctx, req)
}

// TableParentFromStreamName is a utility function for extracting the parent table
// prefix from a stream name.  When an invalid stream ID is passed, this simply returns
// the original stream name.
func TableParentFromStreamName(streamName string) string {
	// Stream IDs have the following prefix:
	// projects/{project}/datasets/{dataset}/tables/{table}/blah
	parts := strings.SplitN(streamName, "/", 7)
	if len(parts) < 7 {
		// invalid; just pass back the input
		return streamName
	}
	return strings.Join(parts[:6], "/")
}
