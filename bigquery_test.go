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

func TestWriteRetryConfigSanitizeMaxRetries(t *testing.T) {
	assertEqual(t, DefaultMaxRetries, ((*WriteRetryConfig)(nil)).getSanitizedMaxRetries())
	testCases := []struct {
		InputMaxRetries    int
		ExpectedMaxRetries int
	}{
		{-42, 0},
		{-1, 0},
		{0, DefaultMaxRetries},
		{1, 1},
		{42, 42},
	}
	for _, testCase := range testCases {
		cfg := &WriteRetryConfig{MaxRetries: testCase.InputMaxRetries}
		assertEqual(t, testCase.ExpectedMaxRetries, cfg.getSanitizedMaxRetries())
	}
}

func TestWriteRetryConfigSanitizedInitialRetryDelay(t *testing.T) {
	assertEqual(t, DefaultInitialRetryDelay, ((*WriteRetryConfig)(nil)).getSanitizedInitialRetryDelay())
	testCases := []struct {
		InputInitialRetryDelay    time.Duration
		ExpectedInitialRetryDelay time.Duration
	}{
		{0, DefaultInitialRetryDelay},
		{time.Second, time.Second},
		{time.Hour, time.Hour},
	}
	for _, testCase := range testCases {
		cfg := &WriteRetryConfig{InitialRetryDelay: testCase.InputInitialRetryDelay}
		assertEqual(t, testCase.ExpectedInitialRetryDelay, cfg.getSanitizedInitialRetryDelay())
	}
}

func TestWriteRetryConfigSanitizedMaxRetryDeadlineOffset(t *testing.T) {
	assertEqual(t, DefaultMaxRetryDeadlineOffset, ((*WriteRetryConfig)(nil)).getSanitizedMaxRetryDeadlineOffset())
	testCases := []struct {
		InputMaxRetryDeadlineOffset    time.Duration
		ExpectedMaxRetryDeadlineOffset time.Duration
	}{
		{0, DefaultMaxRetryDeadlineOffset},
		{time.Second, time.Second},
		{time.Hour, time.Hour},
	}
	for _, testCase := range testCases {
		cfg := &WriteRetryConfig{MaxRetryDeadlineOffset: testCase.InputMaxRetryDeadlineOffset}
		assertEqual(t, testCase.ExpectedMaxRetryDeadlineOffset, cfg.getSanitizedMaxRetryDeadlineOffset())
	}
}

func TestWriteRetryConfigSanitizeRetryDelayMultiplier(t *testing.T) {
	assertEqual(t, float64(DefaultRetryDelayMultiplier), ((*WriteRetryConfig)(nil)).getSanitizedRetryDelayMultiplier())
	testCases := []struct {
		InputRetryDelayMultiplier    float64
		ExpectedRetryDelayMultiplier float64
	}{
		{-42, DefaultRetryDelayMultiplier},
		{-1, DefaultRetryDelayMultiplier},
		{0, DefaultRetryDelayMultiplier},
		{0.5, DefaultRetryDelayMultiplier},
		{1, 1},
		{1.05, 1.05},
		{42, 42},
	}
	for _, testCase := range testCases {
		cfg := &WriteRetryConfig{RetryDelayMultiplier: testCase.InputRetryDelayMultiplier}
		assertEqual(t, testCase.ExpectedRetryDelayMultiplier, cfg.getSanitizedRetryDelayMultiplier())
	}
}
