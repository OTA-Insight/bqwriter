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
	"reflect"
	"sort"
	"testing"
)

// subBQInsertAllClient is an in-memory stub client for the bqInsertAllClient interface,
// allowing us to see what data is written into it
type subBQInsertAllClient struct {
	rows       []interface{}
	nextErrors []error
}

// Put implements bqClient::Put
func (sbqc *subBQInsertAllClient) Put(_ context.Context, data interface{}) error {
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	if rows, ok := data.([]interface{}); ok {
		sbqc.rows = append(sbqc.rows, rows...)
	} else {
		sbqc.rows = append(sbqc.rows, data)
	}
	return nil
}

// Close implements bqClient::Close
func (sbqc *subBQInsertAllClient) Close() error {
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	return nil
}

func (sbqc *subBQInsertAllClient) AddNextError(err error) {
	sbqc.nextErrors = append(sbqc.nextErrors, err)
}

func (sbqc *subBQInsertAllClient) AssertStringSlice(t *testing.T, expected []string) {
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

type TestBQInsertAllThickClientConfig struct {
	BatchSize int
}

func newTestBQInsertAllThickClient(t *testing.T, cfg *TestBQInsertAllThickClientConfig) (*subBQInsertAllClient, *bqInsertAllThickClient) {
	client := new(subBQInsertAllClient)
	if cfg == nil {
		cfg = new(TestBQInsertAllThickClientConfig)
	}
	retryClient, err := newBQInsertAllThickClient(client, cfg.BatchSize, stdLogger{})
	assertNoErrorFatal(t, err)
	return client, retryClient
}

func TestBQInsertAllThickClientDefaultBatchSize(t *testing.T) {
	_, client := newTestBQInsertAllThickClient(t, nil)
	assertEqual(t, DefaultBatchSize, client.batchSize)
	client.Close()
}

func TestBQInsertAllThickClientFlushNop(t *testing.T) {
	_, client := newTestBQInsertAllThickClient(t, nil)
	defer client.Close()
	err := client.Flush()
	assertNoError(t, err)
}

func TestBQInsertAllThickClientBatchExhaustBatch(t *testing.T) {
	stubClient, client := newTestBQInsertAllThickClient(t, &TestBQInsertAllThickClientConfig{
		BatchSize: 2,
	})
	defer stubClient.Close()

	flushed, err := client.Put("hello")
	assertNoError(t, err)
	assertEqual(t, false, flushed)
	stubClient.AssertStringSlice(t, []string{})

	flushed, err = client.Put("world")
	assertNoError(t, err)
	assertEqual(t, true, flushed)
	stubClient.AssertStringSlice(t, []string{"hello", "world"})

	flushed, err = client.Put("!")
	assertNoError(t, err)
	assertEqual(t, false, flushed)
	stubClient.AssertStringSlice(t, []string{"hello", "world"})

	err = client.Flush()
	assertNoError(t, err)
	stubClient.AssertStringSlice(t, []string{"hello", "world", "!"})
}

func TestNewBQInsertAllThickClientWithNilClient(t *testing.T) {
	client, err := newBQInsertAllThickClient(nil, 0, stdLogger{})
	assertError(t, err)
	assertNil(t, client)
}

func TestNewBQInsertAllThickClientWithNilLogger(t *testing.T) {
	client, err := newBQInsertAllThickClient(new(subBQInsertAllClient), 0, nil)
	assertError(t, err)
	assertNil(t, client)
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
		client, err := newStdBQInsertAllThickClient(
			testCase.ProjectID, testCase.DataSetID, testCase.TableID,
			false, false, 0,
			stdLogger{},
		)
		assertError(t, err)
		assertNil(t, client)
	}
}
