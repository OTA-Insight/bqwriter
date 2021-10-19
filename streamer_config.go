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

type (
	// StreamerConfig is used to build a Streamer (client).
	// All configurations found in this structure are optional and have sane defaults
	// defined for them. All required parameters are to be passed in as separate arguments
	// to the NewStreamer constructor method.
	StreamerConfig struct {
		// WorkerCount defines the amount of workers to be used,
		// each on their own goroutine and with an opt-out channel buffer per routine.
		// Use a negative value in order to just want a single worker (same as defining it as 1 explicitly).
		//
		// Defaults to DefaultWorkerCount if not defined explicitly.
		WorkerCount int

		// WorkerQueueSize defines the size of the job queue per worker used
		// in order to allow the Streamer's users to write rows even if all workers are currently
		// too busy to accept new incoming rows.
		//
		// Use a negative value in order to provide no buffer for the workers at all,
		// not rcommended but a possibility for you to choose non the less.
		//
		// Defaults to MaxTotalElapsedRetryTime if not defined explicitly
		WorkerQueueSize int

		// MaxBatchDelay defines the max amount of time a worker batches rows,
		// prior to writing the batched rows, even when not yet full.
		//
		// Defaults to DefaultMaxBatchDelay if d == 0.
		MaxBatchDelay time.Duration

		// Logger allows you to attach a logger to be used by the streamer,
		// instead of the default built-in STDERR logging implementation,
		// with the latter being used as the default in case this logger isn't defined explicitly.
		Logger Logger

		// InsertAllClient allows you to overwrite any or all of the defaults used to configure an
		// InsertAll client API driven Streamer Client. Note that this optional configuration is ignored
		// all together in case StorageClient is defined as a non-nil value.
		InsertAllClient *InsertAllClientConfig

		// StorageClient allows you to create a Storage API driven Streamer Client.
		// You can do so using `new(StorageClientConfig)` in order to create a StorageClient
		// with all possible configurations configured using their defaults as defined by this Go package.
		StorageClient *StorageClientConfig
	}

	// InsertAllClientConfig is used to configure an InsertAll client API driven Streamer Client.
	// All properties have sane defaults as defined and used by this Go package.
	InsertAllClientConfig struct {
		// FailOnInvalidRows causes rows containing invalid data to fail
		// if there is an attempt to insert an invalid row.
		//
		// Defaults to false, making it ignore any invalid rows, silently ignoring these errors.
		FailOnInvalidRows bool

		// FailForUnknownValues causes records containing such values
		// to be treated as invalid records.
		//
		// Defaults to false, making it ignore any invalid values, silently ignoring these errors,
		// and publishing the rows with the unknown values removed from them.
		FailForUnknownValues bool

		// BatchSize defines the amount of rows (data) by a worker, prior to a worker
		// actually writing it to BQ. Should a worker have rows left in its local cache when closing,
		// it will flush/write these rows prior to closing.
		//
		// Defaults to DefaultBatchSize if n == 0,
		// use a negative value or an explicit value of 1
		// in case you want to write each row directly.
		BatchSize int

		// MaxRetryDeadlineOffset is the max amount of time the back off algorithm is allowed to take
		// for its initial as well as all retry attempts. No retry should be attempted when already over this limit.
		// This Offset is to be seen as a maximum, which can be stepped over but not by too much.
		//
		// Defaults to DefaultMaxRetryDeadlineOffset if MaxRetryDeadlineOffset == 0.
		MaxRetryDeadlineOffset time.Duration
	}

	// StorageClientConfig is used to configure a storage client API driven Streamer Client.
	// All properties have sane defaults as defined and used by this Go package.
	//
	// A non-nil StorageClientConfig instance has to be passed in to the StorageClient property of
	// a StreamerConfig in order to indicate the Streamer should be build using the Storage API Client under the hood.
	StorageClientConfig struct {
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
)

// sanitizeStreamerConfig is used to fill in some or all properties
// with sane default values for the StreamerConfig.
// Defined as a function to keep its logic contained and well tested.
func sanitizeStreamerConfig(cfg *StreamerConfig) (sanCfg *StreamerConfig) {
	// we want to create a new config, as to not mutate an input param (the cfg),
	// this comes at the cost of allocating extra memory, but as this is only expected
	// to be used at setup time it should be ok, the memory gods will forgive us I'm sure
	sanCfg = new(StreamerConfig)

	// if no cfg was given, we can simply make it check our new cfg,
	// as that will allow us to reuse the same logic to default all parameters rather than just some
	if cfg == nil {
		cfg = sanCfg
	}

	// We default to some worker threads as to ensure
	// that we do have a worker writing rows even if another one is hangling longer than usual.
	if cfg.WorkerCount < 0 {
		sanCfg.WorkerCount = 1
	} else if cfg.WorkerCount == 0 {
		sanCfg.WorkerCount = DefaultWorkerCount
	} else {
		sanCfg.WorkerCount = cfg.WorkerCount
	}

	// sanitize the insertAll client, something that can be created even
	// if the StorageClient is used instead.
	sanCfg.InsertAllClient = sanitizeInsertAllClientConfig(cfg.InsertAllClient)

	// We default to some half of the batch size used,
	// in order to have some buffer per worker thread.
	if cfg.WorkerQueueSize < 0 {
		sanCfg.WorkerQueueSize = 0
	} else if cfg.WorkerQueueSize == 0 {
		if cfg.StorageClient == nil {
			sanCfg.WorkerQueueSize = (sanCfg.InsertAllClient.BatchSize + 1) / 2
		} else {
			// storage API is a true streamer,
			// and thus uses a hardcoded queue (channel buffer) size
			// per worker instead.
			sanCfg.WorkerQueueSize = DefaultWorkerQueueSize
		}
	} else {
		sanCfg.WorkerQueueSize = cfg.WorkerQueueSize
	}

	// an insanely low value of `1` can be used to check constantly
	// if rows can be written. And while this is possible, it is not recommended.
	if cfg.MaxBatchDelay == 0 {
		sanCfg.MaxBatchDelay = DefaultMaxBatchDelay
	} else {
		sanCfg.MaxBatchDelay = cfg.MaxBatchDelay
	}

	// use default logger if none was defined
	if cfg.Logger == nil {
		sanCfg.Logger = stdLogger{}
	} else {
		sanCfg.Logger = cfg.Logger
	}

	// only sanitize the Storage (client) Config if it is actually defined
	// otherwise nil will be returned
	sanCfg.StorageClient = sanitizeStorageClientConfig(cfg.StorageClient)

	// return the sanitized named output config
	return
}

// sanitizeInsertAllClientConfig is used to fill in some or all properties
// with sane default values for the InsertAllClientConfig.
// Defined as a function to keep its logic contained and well tested.
func sanitizeInsertAllClientConfig(cfg *InsertAllClientConfig) (sanCfg *InsertAllClientConfig) {
	// we want to create a new config, as to not mutate an input param (the cfg),
	// this comes at the cost of allocating extra memory, but as this is only expected
	// to be used at setup time it should be ok, the memory gods will forgive us I'm sure
	sanCfg = new(InsertAllClientConfig)

	// if no cfg was given, we can simply make it check our new cfg,
	// as that will allow us to reuse the same logic to default all parameters rather than just some
	if cfg == nil {
		cfg = sanCfg
	}

	// simply assign the bool properties,
	// no need for any validation there
	sanCfg.FailOnInvalidRows = cfg.FailOnInvalidRows
	sanCfg.FailForUnknownValues = cfg.FailForUnknownValues

	// default the batch size to a sane default,
	// with the user setting it to a value of 1 if no batching is desired.
	if cfg.BatchSize < 0 {
		sanCfg.BatchSize = 1
	} else if cfg.BatchSize == 0 {
		sanCfg.BatchSize = DefaultBatchSize
	} else {
		sanCfg.BatchSize = cfg.BatchSize
	}

	// MaxRetryDeadlineOffset is the total time the write action is allowed to take
	// and cannot be disabled. It is either the by this Go package defined default,
	// or else its value is respected as-is, with once again no upper limit.
	if cfg.MaxRetryDeadlineOffset == 0 {
		sanCfg.MaxRetryDeadlineOffset = DefaultMaxRetryDeadlineOffset
	} else {
		sanCfg.MaxRetryDeadlineOffset = cfg.MaxRetryDeadlineOffset
	}

	// return the sanitized named output config
	return
}

// sanitizeStorageClientConfig is used to fill in some or all properties
// with sane default values for the StorageClientConfig.
// Defined as a function to keep its logic contained and well tested.
func sanitizeStorageClientConfig(cfg *StorageClientConfig) (sanCfg *StorageClientConfig) {
	if cfg == nil {
		// Nothing to do, no config is created if it already exists,
		// given this is used within the API to create an InsertAll API client driven
		// Streamer rather than a Storage API client driven streamer.
		return
	}

	// we want to create a new config, as to not mutate an input param (the cfg),
	// this comes at the cost of allocating extra memory, but as this is only expected
	// to be used at setup time it should be ok, the memory gods will forgive us I'm sure
	sanCfg = new(StorageClientConfig)

	// MaxRetries can be:
	// - negative to disable the the entire Retry back-off algorithm,
	//   and make sure the client doesn't retry retry-able errors.
	// - a zero value will make it use the by this Go package defined default for Max Retries
	// - any other value is respected as-is, so also no upper limit.
	if cfg.MaxRetries < 0 {
		sanCfg.MaxRetries = 0
	} else if cfg.MaxRetries == 0 {
		sanCfg.MaxRetries = DefaultMaxRetries
	} else {
		sanCfg.MaxRetries = cfg.MaxRetries
	}

	// InitialRetryDelay is the first delay used for the retry-back off algorithm,
	// and cannot be disabled. It is either the by this Go package defined default,
	// or else its value is respected as-is, with once again no upper limit.
	if cfg.InitialRetryDelay == 0 {
		sanCfg.InitialRetryDelay = DefaultInitialRetryDelay
	} else {
		sanCfg.InitialRetryDelay = cfg.InitialRetryDelay
	}

	// MaxRetryDeadlineOffset is the total time the write action is allowed to take
	// and cannot be disabled. It is either the by this Go package defined default,
	// or else its value is respected as-is, with once again no upper limit.
	if cfg.MaxRetryDeadlineOffset == 0 {
		sanCfg.MaxRetryDeadlineOffset = DefaultMaxRetryDeadlineOffset
	} else {
		sanCfg.MaxRetryDeadlineOffset = cfg.MaxRetryDeadlineOffset
	}

	// RetryDelayMultiplier is the multiplier used to exponentially increase the max delay
	// used in between retry attempts. Note that the actual retry duration is each time a random
	// value in the partially exlusive `]0, currentRetryDelay]` range. While this jitter might
	// be counter intuitive, it turns out from empirical evidence in the wild that this works surprisingly well.
	if cfg.RetryDelayMultiplier <= 1 {
		sanCfg.RetryDelayMultiplier = DefaultRetryDelayMultiplier
	} else {
		sanCfg.RetryDelayMultiplier = cfg.RetryDelayMultiplier
	}

	// return the sanitized named output non-nil config
	return
}
