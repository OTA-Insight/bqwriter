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
	"strings"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBQInsertAllRetryerRetryOpFlowFailure(t *testing.T) {
	retryer := newBQInsertAllRetryer(
		context.Background(),
		DefaultMaxRetries,
		1*time.Millisecond,
		DefaultMaxRetryDeadlineOffset,
		1.1,
		nil, // no error filter
	)
	expectedFinalErr := errors.New("fourth error")
	errors := []error{
		errors.New("first error"),
		errors.New("second error"),
		errors.New("third error"),
		expectedFinalErr,
	}
	op := func(context.Context) error {
		if len(errors) > 0 {
			err := errors[0]
			errors = errors[1:]
			return err
		}
		return nil
	}
	err := retryer.RetryOp(op)
	assertEqual(t, expectedFinalErr, err)
	assertEqual(t, 0, len(errors))
}

func TestBQInsertAllRetryerRetryOpFlowSuccess(t *testing.T) {
	retryer := newBQInsertAllRetryer(
		context.Background(),
		DefaultMaxRetries,
		1*time.Millisecond,
		DefaultMaxRetryDeadlineOffset,
		1.1,
		nil, // no error filter
	)
	errors := []error{
		errors.New("first error"),
		errors.New("second error"),
		errors.New("third error"),
	}
	op := func(context.Context) error {
		if len(errors) > 0 {
			err := errors[0]
			errors = errors[1:]
			return err
		}
		return nil
	}
	err := retryer.RetryOp(op)
	assertNil(t, err)
	assertEqual(t, 0, len(errors))
}

func TestBQInsertAllRetryerNoRetryBecauseOfNilError(t *testing.T) {
	retryer := newBQInsertAllRetryer(
		context.Background(),
		DefaultMaxRetries,
		DefaultInitialRetryDelay,
		DefaultMaxRetryDeadlineOffset,
		DefaultRetryDelayMultiplier,
		nil, // no error filter
	)
	pause, shouldRetry := retryer.Retry(nil)
	assertEqual(t, false, shouldRetry)
	assertEqual(t, time.Duration(0), pause)
}

func TestBQInsertAllRetryerNoRetryBecauseOfCanceledContext(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	retryer := newBQInsertAllRetryer(
		ctx,
		DefaultMaxRetries,
		DefaultInitialRetryDelay,
		DefaultMaxRetryDeadlineOffset,
		DefaultRetryDelayMultiplier,
		nil, // no error filter
	)
	pause, shouldRetry := retryer.Retry(errors.New("a test error"))
	assertEqual(t, false, shouldRetry)
	assertEqual(t, time.Duration(0), pause)
}

func TestBQInsertAllRetryerNoRetryBecauseOfMaxRetries(t *testing.T) {
	retryer := newBQInsertAllRetryer(
		context.Background(),
		1, // retry max 1 time
		DefaultInitialRetryDelay,
		DefaultMaxRetryDeadlineOffset,
		DefaultRetryDelayMultiplier,
		nil, // no error filter
	)

	// first time will work
	pause, shouldRetry := retryer.Retry(errors.New("a test error"))
	assertEqual(t, true, shouldRetry)
	if pause == 0 || pause > DefaultInitialRetryDelay {
		t.Errorf("unexpeted pause duration: %v", pause)
	}

	// second time not, as we reached our limit of max retries
	pause, shouldRetry = retryer.Retry(errors.New("a test error"))
	assertEqual(t, false, shouldRetry)
	assertEqual(t, time.Duration(0), pause)
}

func TestBQInsertAllRetryerNoRetryBecauseOfErrorFilter(t *testing.T) {
	retryer := newBQInsertAllRetryer(
		context.Background(),
		DefaultMaxRetries,
		DefaultInitialRetryDelay,
		DefaultMaxRetryDeadlineOffset,
		DefaultRetryDelayMultiplier,
		func(err error) bool {
			return !strings.Contains(err.Error(), "stop")
		},
	)

	// first time will work, as the filter didn't trigger
	pause, shouldRetry := retryer.Retry(errors.New("retry this test error please"))
	assertEqual(t, true, shouldRetry)
	if pause == 0 || pause > DefaultInitialRetryDelay {
		t.Errorf("unexpeted pause duration: %v", pause)
	}

	// second time not, as we triggered the error filter in the wrong way
	pause, shouldRetry = retryer.Retry(errors.New("another test error, but please do not stop! :("))
	assertEqual(t, false, shouldRetry)
	assertEqual(t, time.Duration(0), pause)
}

func TestBQRestAPIRetryErrorFilterTrue(t *testing.T) {
	testCases := []int{
		500,
		503,
	}
	for _, testCase := range testCases {
		err := &googleapi.Error{Code: testCase}
		assertEqual(t, true, bqRestAPIRetryErrorFilter(err))
	}
}

func TestBQRestAPIRetryErrorFilterFalse(t *testing.T) {
	// nil error is not an accepted error
	assertEqual(t, false, bqRestAPIRetryErrorFilter(nil))
	// custom error is not an accepted error
	assertEqual(t, false, bqRestAPIRetryErrorFilter(errors.New("todo")))
	// correct error, but wrong code
	err := &googleapi.Error{Code: 404}
	assertEqual(t, false, bqRestAPIRetryErrorFilter(err))
}

func TestBQGRPCRetryErrorFilterTrue(t *testing.T) {
	testCases := []codes.Code{
		codes.Unavailable,
		codes.FailedPrecondition,
		codes.ResourceExhausted,
		codes.DataLoss,
	}
	for _, testCase := range testCases {
		err := status.New(testCase, "test error").Err()
		assertEqual(t, true, bqGRPCRetryErrorFilter(err))
	}
}

func TestBQGRPCRetryErrorFilterFalse(t *testing.T) {
	// nil error is not an accepted error
	assertEqual(t, false, bqGRPCRetryErrorFilter(nil))
	// custom error is not an accepted error
	assertEqual(t, false, bqGRPCRetryErrorFilter(errors.New("todo")))
	// correct error, but wrong code
	err := status.New(codes.Aborted, "test error").Err()
	assertEqual(t, false, bqGRPCRetryErrorFilter(err))
}
