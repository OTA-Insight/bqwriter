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
	"time"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bqRetryer is a retryer inspired by other community back-off implementations,
// in order to not have another dependency added to this library,
// while still being able to rely on existing retry-related google code of
// dependencies already required by this library for its core functionality
type bqRetryer struct {
	backoff                gax.Backoff
	retries                int
	maxRetries             int
	startTime              time.Time
	maxRetryDeadlineOffset time.Duration
	deadlineCtx            context.Context
	cancelDeadlineCtx      func()
	errorFilter            func(error) bool
}

// compile-time interface compliance
var _ gax.Retryer = (*bqRetryer)(nil)

func newBQRetryer(ctx context.Context, maxRetries int, initialRetryDelay time.Duration, maxRetryDeadlineOffset time.Duration, retryDelayMultiplier float64, errorFilter func(error) bool) *bqRetryer {
	startTime := time.Now()
	deadlineCtx, cancelDeadlineCtx := context.WithDeadline(ctx, startTime.Add(maxRetryDeadlineOffset))
	return &bqRetryer{
		backoff: gax.Backoff{
			Initial:    initialRetryDelay,
			Max:        maxRetryDeadlineOffset,
			Multiplier: retryDelayMultiplier,
		},
		maxRetries:             maxRetries,
		startTime:              startTime,
		maxRetryDeadlineOffset: maxRetryDeadlineOffset,
		deadlineCtx:            deadlineCtx,
		cancelDeadlineCtx:      cancelDeadlineCtx,
		errorFilter:            errorFilter,
	}
}

// RetryOp retries the operation
func (r *bqRetryer) RetryOp(op func(context.Context) error) error {
	defer r.cancelDeadlineCtx()
	for {
		err := op(r.deadlineCtx)
		if err == nil {
			return nil
		}
		pause, ok := r.Retry(err)
		if !ok {
			// retryer wishes not to retry, return early
			return err
		}
		// we'll retry, but first will sleep
		time.Sleep(pause)
	}
}

// Retry implements gax::Retryer::Retry
func (r *bqRetryer) Retry(err error) (pause time.Duration, shouldRetry bool) {
	defer func() {
		if !shouldRetry {
			r.cancelDeadlineCtx()
		}
	}()
	if err == nil {
		// no error returned, no need to retry
		return 0, false
	}
	if errors.Is(r.deadlineCtx.Err(), context.Canceled) {
		// if parent ctx is done or the deadline has been reached,
		// no retry is possible any longer either
		return 0, false
	}
	if r.retries >= r.maxRetries {
		// no longer need to retry,
		// already exhausted our retry attempts
		return 0, false
	}
	if r.errorFilter != nil && !r.errorFilter(err) {
		// we do not wish to retry this kind of error either,
		// as it is not detected as retryable by us
		return 0, false
	}
	// correct the Max time, as to stay as close as possible to our max elapsed retry time
	elapsedTime := time.Now().Sub(r.startTime)
	r.backoff.Max = r.maxRetryDeadlineOffset - elapsedTime
	// retry with the pause time indicated by the gax BackOff algorithm
	r.retries += 1
	return r.backoff.Pause(), true
}

// bqRestAPIRetryErrorFilter used to be defined based on HTTP Codes 500 and 503.
// It turns out however that the actual BQ Insert All client cannot be configured in terms
// of Retryability, and instead these values are hardcoded. Its RetryError filter is also a lot more advanced
// than this
//
// You can find its implementation at: https://github.com/googleapis/google-cloud-go/blob/45fd2594d99ef70c776df26866f0a3b537e7e69e/bigquery/bigquery.go

// bqGRPCRetryErrorFilter returns a Retry error filter to be used
// for retrying GRPC Google API operations (e.g. BQ Storage client)
func bqGRPCRetryErrorFilter(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.FailedPrecondition, codes.ResourceExhausted, codes.DataLoss:
		return true
	default:
		return false
	}
}
