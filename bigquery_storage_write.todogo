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

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
)

// bqStorageThickClient implements the standard/official BQ (cloud) Client,
// using the new GRPC-backed Storage Stream Write API with retry logic added on top of that.
// Rows will be written seemingly immediately to the server without any explicit batching.
// Each worker will have its own stream and all write to the default channel..
type bqStorageThickClient struct {
	ctx context.Context

	client       *bqStorage.BigQueryWriteClient
	appendClient storagepb.BigQueryWrite_AppendRowsClient

	batchCh chan interface{}
	respCh  chan interface{}

	logger Logger
}

// TODO: implement sender and receiver with with the buffer defined on the batch size

// Put implements bqClient::Put
func (bqc *bqStorageThickClient) Put(data interface{}) (bool, error) {
	select {
	case <-bqc.ctx.Done():
		return false, fmt.Errorf("put data (row) in BQ stream using the storage Write API: %w", context.Canceled)

	case bqc.batchCh <- data:
		// we never have to flush or flush, so no need to do this
		// TODO: confirm
		return false, nil
	}
}

func (bqc *bqStorageThickClient) processBatch() {
	defer func() {
		err := bqc.appendClient.CloseSend()
		if err != nil {
			panic("LOG")
		}
	}()

	// TODO: define better harmony with ctx and all cases how one or the other can be done/closed first
	for data := range bqc.batchCh {
		err := bqc.appendClient.Send(nil)
		if err != nil {
			panic("LOG")
		}
	}
}

func (bqc *bqStorageThickClient) processResponses() {
	// TODO: define better harmony with ctx and all cases how one or the other can be done/closed first
	for data := range bqc.respCh {
		resp, err := bqc.appendClient.Recv(nil)
		panic("TODO")
	}
}

/*
func main() {
	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()
	stream, err := c.AppendRows(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	go func() {
		reqs := []*storagepb.AppendRowsRequest{
			// TODO: Create requests.
		}
		for _, req := range reqs {
			if err := stream.Send(req); err != nil {
				// TODO: Handle error.
			}
		}
		stream.CloseSend()
	}()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}
*/

// Flush implements bqClient::Flush
func (bqc *bqStorageThickClient) Flush() error {
	// we use the default stream,
	// as such there is no need to flush
	// TODO: support also a non-default stream: if so we do need to call FlushRows here
	return nil
}

// Close implements bqClient::Close
func (bqc *bqStorageThickClient) Close() error {
	if bqc.client == nil {
		return errors.New("BQ Storage Client already closed")
	}
	close(bqc.batchCh)
	err := bqc.client.Close()
	bqc.client = nil
	if err != nil {
		return fmt.Errorf("close BigQuery Storage Write client: %w", err)
	}
	return nil
}
