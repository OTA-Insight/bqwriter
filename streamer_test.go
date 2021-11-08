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
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter/internal"
	"github.com/OTA-Insight/bqwriter/internal/bigquery"
	"github.com/OTA-Insight/bqwriter/internal/test"
	"github.com/OTA-Insight/bqwriter/log"
)

func TestNewStreamerInputErrors(t *testing.T) {
	testCases := []struct {
		ProjectID string
		DataSetID string
		TableID   string
	}{
		{"", "", ""},
		{"a", "", ""},
		{"", "a", ""},
		{"", "", "a"},
		{"a", "b", ""},
		{"a", "", "b"},
		{"", "a", "b"},
		{"", "a", "b"},
	}
	for _, testCase := range testCases {
		builder, err := NewStreamer(
			context.Background(),
			testCase.ProjectID, testCase.DataSetID, testCase.TableID,
			nil,
		)
		test.AssertError(t, err)
		test.AssertIsError(t, err, internal.InvalidParamErr)
		test.AssertNil(t, builder)
	}
}

// stubBQClient is an in-memory stub client for the bigquery.Client interface,
// allowing us to see what data is written into it
type stubBQClient struct {
	rows         []interface{}
	flushCount   int
	flushNextPut bool
	nextErrors   []error
	putSignal    chan<- struct{}
}

// Put implements bigquery.Client::Put
func (sbqc *stubBQClient) Put(data interface{}) (bool, error) {
	defer func() {
		if sbqc.putSignal != nil {
			sbqc.putSignal <- struct{}{}
		}
	}()
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return false, err
	}
	if rows, ok := data.([]interface{}); ok {
		sbqc.rows = append(sbqc.rows, rows...)
	} else {
		sbqc.rows = append(sbqc.rows, data)
	}
	if sbqc.flushNextPut {
		sbqc.flushNextPut = false
		return true, sbqc.Flush()
	}
	return false, nil
}

func (sbqc *stubBQClient) Flush() error {
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	sbqc.flushCount += 1
	return nil
}

// Close implements bigquery.Client::Close
func (sbqc *stubBQClient) Close() error {
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	return nil
}

func (sbqc *stubBQClient) AddNextError(err error) {
	sbqc.nextErrors = append(sbqc.nextErrors, err)
}

func (sbqc *stubBQClient) FlushNextPut() {
	sbqc.flushNextPut = true
}

func (sbqc *stubBQClient) SubscribeToPutSignal(ch chan<- struct{}) {
	sbqc.putSignal = ch
}

func (sbqc *stubBQClient) AssertFlushCount(t *testing.T, expected int) {
	test.AssertEqual(t, sbqc.flushCount, expected)
}

func (sbqc *stubBQClient) AssertStringSlice(t *testing.T, expected []string) {
	got := make([]string, 0, len(sbqc.rows))
	for _, row := range sbqc.rows {
		s, ok := row.(string)
		if !ok {
			t.Errorf("unexpected value (non-string): %v", row)
		} else {
			got = append(got, s)
		}
	}
	sort.Strings(got)
	sort.Strings(expected)
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("unexpected rows of data in client, expected %v, got %v", expected, got)
	}
}

func (sbqc *stubBQClient) AssertAnyStringSlice(t *testing.T, expectedSlice ...[]string) {
	got := make([]string, 0, len(sbqc.rows))
	for _, row := range sbqc.rows {
		s, ok := row.(string)
		if !ok {
			t.Errorf("unexpected value (non-string): %v", row)
		} else {
			got = append(got, s)
		}
	}
	sort.Strings(got)
	for _, expected := range expectedSlice {
		sort.Strings(expected)
		if reflect.DeepEqual(expected, got) {
			return
		}
	}
	t.Errorf("unexpected rows of data in client, expected any of %v, got %v", expectedSlice, got)
}

type testStreamerConfig struct {
	WorkerCount     int
	MaxBatchDelay   time.Duration
	WorkerQueueSize int
}

func newTestStreamer(ctx context.Context, t *testing.T, cfg testStreamerConfig) (*stubBQClient, *Streamer) {
	client := new(stubBQClient)
	// always use same client for our purposes
	clientBuilder := func(ctx context.Context, projectID, dataSetID, tableID string, logger log.Logger, insertAllCfg *InsertAllClientConfig, storageCfg *StorageClientConfig, batchCfg *BatchClientConfig) (bigquery.Client, error) {
		return client, nil
	}
	streamer, err := newStreamerWithClientBuilder(
		ctx, clientBuilder,
		"a", "b", "c",
		&StreamerConfig{
			WorkerCount:     cfg.WorkerCount,
			WorkerQueueSize: cfg.WorkerQueueSize,
			MaxBatchDelay:   cfg.MaxBatchDelay,
		},
	)
	test.AssertNoErrorFatal(t, err)
	return client, streamer
}

func TestStreamerFlowStandard(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{})
	defer streamer.Close()
	putSignalCh := make(chan struct{}, 1)
	client.SubscribeToPutSignal(putSignalCh)

	test.AssertNoError(t, streamer.Write("hello"))
	test.AssertNoError(t, streamer.Write("world"))
	<-putSignalCh
	<-putSignalCh

	// our stub BQ client doesn't batch, so it immediately has the data
	client.AssertStringSlice(t, []string{"hello", "world"})
}

func TestStreamerWriteErrorAlreadyClosed(t *testing.T) {
	_, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{})
	streamer.Close()
	test.AssertError(t, streamer.Write("hello"))
}

func TestStreamerWriteErrorNilData(t *testing.T) {
	_, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{})
	defer streamer.Close()
	err := streamer.Write(nil)
	test.AssertError(t, err)
	test.AssertIsError(t, err, internal.InvalidParamErr)
}

func TestStreamerCloseError(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{})
	client.AddNextError(fmt.Errorf("some client close error: %w", test.StaticErr))
	streamer.Close()
	// this is logged to stderr, so should be okay for user
}

func TestStreamerFlushCount(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		WorkerCount:   1,
		MaxBatchDelay: 200 * time.Millisecond,
	})
	time.Sleep(250 * time.Millisecond)
	client.AssertFlushCount(t, 1)
	streamer.Close()
	client.AssertFlushCount(t, 2)
}
