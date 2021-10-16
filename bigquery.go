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

import "time"

// bqClient is the interface we expect a BQ client to implement.
// The only reason for this abstraction is so we can easily unit test this class,
// without actual BQ interaction.
type bqClient interface {
	// Put a row of data, with the possibility to opt-out of any scheme validation.
	//
	// No context is passed here, instead background context is always used.
	// Reason being is as we always want to be able to write to BQ,
	// even if the actual parent context is already closed.
	//
	// A boolean is also returned indicating whether or not the client
	// has flushed as part of its Put process.
	Put(data interface{}) (bool, error)

	// Flush any data already Put but not yet written to BigQuery.
	Flush() error

	// Close the BQ Client
	Close() error
}

// WriteRetryConfig is an optional configuration that can be used
// in order to ensure retry-able write errors are automatically retried
// using a built-in back off algorithm.
type WriteRetryConfig struct {
	// MaxRetries is the max amount of times that the retry logic will retry a retryable
	// BQ write error, prior to giving up. Note that non-retryable errors will immediately stop
	// and that there is also an upper limit of MaxTotalElpasedRetryTime to execute in worst case these max retries.
	//
	// Defaults to DefaultMaxRetries if MaxRetries == 0,
	// or use MaxRetries < 0 if you want to explicitly disable Retrying,
	// but in that case you might as well not pass in a WriteRetryConfig at all.
	MaxRetries int

	// InitialRetryDelay is the initial time the back off algorithm will wait and which will
	// be used as the base value to be multiplied for any possible sequential retries.
	//
	// Defaults to DefaultInitialRetryDelay if InitialRetryDelay == 0.
	InitialRetryDelay time.Duration

	// MaxRetryDeadlineOffset is the max amount of time the back off algorithm is allowed to take
	// for its initial as well as all retry attempts. No retry should be attempted when already over this limit.
	// This Offset is to be seen as a maximum, which can be stepped over but not by too much.
	//
	// Defaults to DefaultMaxRetryDeadlineOffset if MaxRetryDeadlineOffset == 0.
	MaxRetryDeadlineOffset time.Duration

	// RetryDelayMultiplier is the retry delay multipler used by the retry
	// back off algorithm in order to increase the delay in between each sequential write-retry of the
	// same back off sequence.
	//
	// Defaults to DefaultRetryDelayMultiplier if RetryDelayMultiplier < 1, as 2 is also the lowest possible multiplier accepted.
	RetryDelayMultiplier float64
}

func (cfg *WriteRetryConfig) getSanitizedMaxRetries() int {
	if cfg == nil || cfg.MaxRetries == 0 {
		return DefaultMaxRetries
	}
	if cfg.MaxRetries < 0 {
		return 0
	}
	return cfg.MaxRetries
}

func (cfg *WriteRetryConfig) getSanitizedInitialRetryDelay() time.Duration {
	if cfg == nil || cfg.InitialRetryDelay == 0 {
		return DefaultInitialRetryDelay
	}
	return cfg.InitialRetryDelay
}

func (cfg *WriteRetryConfig) getSanitizedMaxRetryDeadlineOffset() time.Duration {
	if cfg == nil || cfg.MaxRetryDeadlineOffset == 0 {
		return DefaultMaxRetryDeadlineOffset
	}
	return cfg.MaxRetryDeadlineOffset
}

func (cfg *WriteRetryConfig) getSanitizedRetryDelayMultiplier() float64 {
	if cfg == nil || cfg.RetryDelayMultiplier < 1 {
		return DefaultRetryDelayMultiplier
	}
	return cfg.RetryDelayMultiplier
}
