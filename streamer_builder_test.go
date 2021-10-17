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
	"testing"
	"time"
)

func TestNewStreamerBuilderInputErrors(t *testing.T) {
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
	for testCaseIndex, testCase := range testCases {
		builder, err := NewStreamerBuilder(
			testCase.ProjectID, testCase.DataSetID, testCase.TableID,
		)
		if err == nil {
			t.Errorf("testCase #%d: expected an error to be returned, non was received", testCaseIndex)
		}
		if builder != nil {
			t.Errorf("testCase #%d: expected builder to be nil, received one", testCaseIndex)
		}
	}
}

func newTestStreamerBuilder(t *testing.T) *StreamerBuilder {
	builder, err := NewStreamerBuilder("a", "b", "c")
	assertNoErrorFatal(t, err)
	return builder
}

func TestStreamerBuilderWorkerCount(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	assertEqual(t, DefaultWorkerCount, builder.workerCount)
	assertEqual(t, DefaultWorkerCount, builder.WorkerCount(-1).workerCount)
	assertEqual(t, 42, builder.WorkerCount(42).workerCount)
	assertEqual(t, DefaultWorkerCount, builder.WorkerCount(0).workerCount)
}

func TestStreamerBuilderWorkerQueueSize(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	assertEqual(t, DefaultWorkerQueueSize, builder.workerQueueSize)
	assertEqual(t, DefaultWorkerQueueSize, builder.WorkerQueueSize(-1).workerQueueSize)
	assertEqual(t, 42, builder.WorkerQueueSize(42).workerQueueSize)
	assertEqual(t, DefaultWorkerQueueSize, builder.WorkerQueueSize(0).workerQueueSize)
}

func TestStreamerBuilderWriteRetryConfig(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	assertEqual(t, new(WriteRetryConfig), builder.retryConfig)
	assertEqual(t,
		&WriteRetryConfig{
			MaxRetries:             42,
			InitialRetryDelay:      time.Second,
			MaxRetryDeadlineOffset: time.Minute,
		},
		builder.WriteRetryConfig(&WriteRetryConfig{
			MaxRetries:             42,
			InitialRetryDelay:      time.Second,
			MaxRetryDeadlineOffset: time.Minute,
		}).retryConfig)
	assertNil(t, builder.WriteRetryConfig(nil).retryConfig)
}

func TestStreamerBuilderBatchSize(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	assertEqual(t, DefaultBatchSize, builder.batchSize)
	assertEqual(t, DefaultBatchSize, builder.BatchSize(-1).batchSize)
	assertEqual(t, 42, builder.BatchSize(42).batchSize)
	assertEqual(t, DefaultBatchSize, builder.BatchSize(0).batchSize)
}

func TestStreamerBuilderMaxBatchDelay(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	assertEqual(t, DefaultMaxBatchDelay, builder.maxBatchDelay)
	assertEqual(t, DefaultMaxBatchDelay, builder.MaxBatchDelay(0).maxBatchDelay)
	assertEqual(t, time.Minute, builder.MaxBatchDelay(time.Minute).maxBatchDelay)
}

func TestStreamerBuilderClientInsertAll(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	assertEqual(t, false, builder.clientInsertAllConfig.skipInvalidRows)
	assertEqual(t, false, builder.clientInsertAllConfig.ignoreUnknownValues)
	builder.ClientInsertAll(false, true)
	assertEqual(t, false, builder.clientInsertAllConfig.skipInvalidRows)
	assertEqual(t, true, builder.clientInsertAllConfig.ignoreUnknownValues)
	builder.ClientInsertAll(true, false)
	assertEqual(t, true, builder.clientInsertAllConfig.skipInvalidRows)
	assertEqual(t, false, builder.clientInsertAllConfig.ignoreUnknownValues)
	builder.ClientInsertAll(true, true)
	assertEqual(t, true, builder.clientInsertAllConfig.skipInvalidRows)
	assertEqual(t, true, builder.clientInsertAllConfig.ignoreUnknownValues)
}

type testLogger struct{}

func (_ testLogger) Debug(_args ...interface{})                    {}
func (_ testLogger) Debugf(_template string, _args ...interface{}) {}
func (_ testLogger) Error(_args ...interface{})                    {}
func (_ testLogger) Errorf(_template string, _args ...interface{}) {}

func TestStreamerBuilderLogger(t *testing.T) {
	builder := newTestStreamerBuilder(t)
	switch builder.logger.(type) {
	case stdLogger:
	default:
		t.Errorf("unexpected logger type: %v (%T)", builder.logger, builder.logger)
	}
	builder.Logger(testLogger{})
	switch builder.logger.(type) {
	case testLogger:
	default:
		t.Errorf("unexpected logger type: %v (%T)", builder.logger, builder.logger)
	}
	builder.Logger(nil)
	switch builder.logger.(type) {
	case stdLogger:
	default:
		t.Errorf("unexpected logger type: %v (%T)", builder.logger, builder.logger)
	}
}
