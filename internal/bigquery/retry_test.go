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

package bigquery

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter/constant"
	"github.com/OTA-Insight/bqwriter/internal/test"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBQRetryerRetryOpFlowFailure(t *testing.T) {
	retryer := newBQRetryer(
		context.Background(),
		constant.DefaultMaxRetries,
		1*time.Millisecond,
		constant.DefaultMaxRetryDeadlineOffset,
		1.1,
		nil, // no error filter
	)
	expectedFinalErr := fmt.Errorf("fourth error: %w", test.StaticErr)
	errors := []error{
		fmt.Errorf("first error: %w", test.StaticErr),
		fmt.Errorf("second error: %w", test.StaticErr),
		fmt.Errorf("third error: %w", test.StaticErr),
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
	test.AssertEqual(t, expectedFinalErr, err)
	test.AssertEqual(t, 0, len(errors))
}

func TestBQRetryerRetryOpFlowSuccess(t *testing.T) {
	retryer := newBQRetryer(
		context.Background(),
		constant.DefaultMaxRetries,
		1*time.Millisecond,
		constant.DefaultMaxRetryDeadlineOffset,
		1.1,
		nil, // no error filter
	)
	errors := []error{
		fmt.Errorf("first error: %w", test.StaticErr),
		fmt.Errorf("second error: %w", test.StaticErr),
		fmt.Errorf("third error: %w", test.StaticErr),
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
	test.AssertNil(t, err)
	test.AssertEqual(t, 0, len(errors))
}

func TestBQRetryerNoRetryBecauseOfNilError(t *testing.T) {
	retryer := newBQRetryer(
		context.Background(),
		constant.DefaultMaxRetries,
		constant.DefaultInitialRetryDelay,
		constant.DefaultMaxRetryDeadlineOffset,
		constant.DefaultRetryDelayMultiplier,
		nil, // no error filter
	)
	pause, shouldRetry := retryer.Retry(nil)
	test.AssertFalse(t, shouldRetry)
	test.AssertEqual(t, time.Duration(0), pause)
}

func TestBQRetryerNoRetryBecauseOfCanceledContext(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	retryer := newBQRetryer(
		ctx,
		constant.DefaultMaxRetries,
		constant.DefaultInitialRetryDelay,
		constant.DefaultMaxRetryDeadlineOffset,
		constant.DefaultRetryDelayMultiplier,
		nil, // no error filter
	)
	pause, shouldRetry := retryer.Retry(fmt.Errorf("retry: %w", test.StaticErr))
	test.AssertFalse(t, shouldRetry)
	test.AssertEqual(t, time.Duration(0), pause)
}

func TestBQRetryerNoRetryBecauseOfMaxRetries(t *testing.T) {
	retryer := newBQRetryer(
		context.Background(),
		1, // retry max 1 time
		constant.DefaultInitialRetryDelay,
		constant.DefaultMaxRetryDeadlineOffset,
		constant.DefaultRetryDelayMultiplier,
		nil, // no error filter
	)

	// first time will work
	pause, shouldRetry := retryer.Retry(fmt.Errorf("retry: %w", test.StaticErr))
	test.AssertTrue(t, shouldRetry)
	if pause == 0 || pause > constant.DefaultInitialRetryDelay {
		t.Errorf("unexpeted pause duration: %v", pause)
	}

	// second time not, as we reached our limit of max retries
	pause, shouldRetry = retryer.Retry(fmt.Errorf("retry: %w", test.StaticErr))
	test.AssertFalse(t, shouldRetry)
	test.AssertEqual(t, time.Duration(0), pause)
}

func TestBQRetryerNoRetryBecauseOfErrorFilter(t *testing.T) {
	retryer := newBQRetryer(
		context.Background(),
		constant.DefaultMaxRetries,
		constant.DefaultInitialRetryDelay,
		constant.DefaultMaxRetryDeadlineOffset,
		constant.DefaultRetryDelayMultiplier,
		func(err error) bool {
			return !strings.Contains(err.Error(), "stop")
		},
	)

	// first time will work, as the filter didn't trigger
	pause, shouldRetry := retryer.Retry(fmt.Errorf("retry (should continue): %w", test.StaticErr))
	test.AssertTrue(t, shouldRetry)
	if pause == 0 || pause > constant.DefaultInitialRetryDelay {
		t.Errorf("unexpeted pause duration: %v", pause)
	}

	// second time not, as we triggered the error filter in the wrong way
	pause, shouldRetry = retryer.Retry(fmt.Errorf("retry (should stop): %w", test.StaticErr))
	test.AssertFalse(t, shouldRetry)
	test.AssertEqual(t, time.Duration(0), pause)
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
		test.AssertTrue(t, bqGRPCRetryErrorFilter(err))
	}
}

func TestBQGRPCRetryErrorFilterFalse(t *testing.T) {
	// nil error is not an accepted error
	test.AssertFalse(t, bqGRPCRetryErrorFilter(nil))
	// custom error is not an accepted error
	test.AssertFalse(t, bqGRPCRetryErrorFilter(fmt.Errorf("todo: %w", test.StaticErr)))
	// correct error, but wrong code
	err := status.New(codes.Aborted, "test error").Err()
	test.AssertFalse(t, bqGRPCRetryErrorFilter(err))
}
