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
	"net/http"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter/constant"
	"github.com/OTA-Insight/bqwriter/internal"
	"github.com/OTA-Insight/bqwriter/internal/test"
	"github.com/OTA-Insight/bqwriter/log"
	"google.golang.org/api/googleapi"
)

// stubClient is an in-memory stub client for the bqInsertAllClient interface,
// allowing us to see what data is written into it
type stubClient struct {
	rows            []interface{}
	nextErrors      []error
	sleepPriorToPut time.Duration
}

// Put implements bqClient::Put
func (sbqc *stubClient) Put(ctx context.Context, data interface{}) error {
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	if sbqc.sleepPriorToPut > 0 {
		time.Sleep(sbqc.sleepPriorToPut)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("put data into BQ using stub insertAll: %w", err)
	}
	if rows, ok := data.([]interface{}); ok {
		sbqc.rows = append(sbqc.rows, rows...)
	} else {
		sbqc.rows = append(sbqc.rows, data)
	}
	return nil
}

// Close implements bqClient::Close
func (sbqc *stubClient) Close() error {
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	return nil
}

func (sbqc *stubClient) AddNextError(err error) {
	sbqc.nextErrors = append(sbqc.nextErrors, err)
}

func (sbqc *stubClient) SetSleepPriorToPut(d time.Duration) {
	sbqc.sleepPriorToPut = d
}

func (sbqc *stubClient) AssertStringSlice(t *testing.T, expected []string) {
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

type TestClientConfig struct {
	BatchSize int
}

func newTestClient(t *testing.T, cfg *TestClientConfig) (*stubClient, *Client) {
	client := new(stubClient)
	if cfg == nil {
		cfg = new(TestClientConfig)
	}
	retryClient, err := newClient(client, cfg.BatchSize, test.Logger{})
	test.AssertNoErrorFatal(t, err)
	return client, retryClient
}

func TestBQInsertAllThickClientDefaultBatchSize(t *testing.T) {
	_, client := newTestClient(t, nil)
	test.AssertEqual(t, constant.DefaultBatchSize, client.batchSize)
	client.Close()
}

func TestBQInsertAllThickClientFlushNop(t *testing.T) {
	_, client := newTestClient(t, nil)
	defer client.Close()
	err := client.Flush()
	test.AssertNoError(t, err)
}

func TestBQInsertAllThickClientBatchExhaustBatch(t *testing.T) {
	stubClient, client := newTestClient(t, &TestClientConfig{
		BatchSize: 2,
	})
	defer stubClient.Close()

	flushed, err := client.Put("hello")
	test.AssertNoError(t, err)
	test.AssertFalse(t, flushed)
	stubClient.AssertStringSlice(t, []string{})

	flushed, err = client.Put("world")
	test.AssertNoError(t, err)
	test.AssertTrue(t, flushed)
	stubClient.AssertStringSlice(t, []string{"hello", "world"})

	flushed, err = client.Put("!")
	test.AssertNoError(t, err)
	test.AssertFalse(t, flushed)
	stubClient.AssertStringSlice(t, []string{"hello", "world"})

	err = client.Flush()
	test.AssertNoError(t, err)
	stubClient.AssertStringSlice(t, []string{"hello", "world", "!"})
}

func TestBQInsertAllRetryInternalErrors(t *testing.T) {
	stubClient, client := newTestClient(t, &TestClientConfig{
		BatchSize: 1,
	})
	defer stubClient.Close()

	stubClient.AddNextError(&googleapi.Error{
		Code: http.StatusInternalServerError,
	})

	flushed, err := client.Put("hello")
	test.AssertNoError(t, err)
	test.AssertTrue(t, flushed)
	stubClient.AssertStringSlice(t, []string{"hello"})
}

func TestBQInsertAllCloseError(t *testing.T) {
	stubClient, client := newTestClient(t, nil)
	defer stubClient.Close()
	stubClient.AddNextError(fmt.Errorf("close error: %w", test.ErrStatic))
	err := client.Close()
	test.AssertIsError(t, err, test.ErrStatic)
}

func TestNewBQInsertAllThickClientWithNilClient(t *testing.T) {
	client, err := newClient(nil, 0, test.Logger{})
	test.AssertError(t, err)
	test.AssertNil(t, client)
}

func TestNewBQInsertAllThickClientWithNilLogger(t *testing.T) {
	client, err := newClient(new(stubClient), 0, nil)
	test.AssertError(t, err)
	test.AssertNil(t, client)
}

func TestNewStdBQInsertAllThickClientInputErrors(t *testing.T) {
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
		client, err := NewClient(
			testCase.ProjectID, testCase.DataSetID, testCase.TableID,
			false, false, 0,
			test.Logger{},
		)
		test.AssertError(t, err)
		test.AssertIsError(t, err, internal.ErrInvalidParam)
		test.AssertNil(t, client)
	}
}

func TestNewBQInsertAllThickClientErrors(t *testing.T) {
	testCases := []struct {
		Client bqClient
		Logger log.Logger
	}{
		{nil, nil},
		{new(stubClient), nil},
		{nil, test.Logger{}},
	}
	for _, testCase := range testCases {
		client, err := newClient(
			testCase.Client, 0, testCase.Logger,
		)
		test.AssertError(t, err)
		test.AssertIsError(t, err, internal.ErrInvalidParam)
		test.AssertNil(t, client)
	}
}
