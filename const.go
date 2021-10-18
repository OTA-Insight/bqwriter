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

const (
	// DefaultWorkerCount is used as the default for the WorkerCount property of
	// StreamerConfig, used in case the property is 0 (e.g. when undefined).
	DefaultWorkerCount = 2

	// DefaultMaxRetries is used as the default for the MaxRetries property of
	// the StreamerConfig's WriteRetryConfig, used in case the property is 0 (e.g. when undefined).
	DefaultMaxRetries = 3

	// DefaultInitialRetryDelay is used as the default for the InitialRetryDuration property
	// of the StreamerConfig's WriteRetryConfig, used in case the property is 0 (e.g. when undefined),
	// and only if the WriteRetryConfig is actually defined.
	//
	// Default based on suggestions made in https://cloud.google.com/bigquery/sla.
	DefaultInitialRetryDelay = time.Second * 1

	// DefaultMaxRetryDeadlineOffset is the default max amount of the the streamer will
	// allow the retry back off logic to retry, as to ensure a goroutine isn't blocked for too long on a faulty write.
	// Used in case the property is 0 (e.g. when undefined), and only if the WriteRetryConfig is actually defined.
	//
	// Default based on suggestions made in https://cloud.google.com/bigquery/sla.
	DefaultMaxRetryDeadlineOffset = time.Second * 32

	// DefaultRetryDelayMultiplier is the default retry delay multipler used by the streamer's
	// back off algorithm in order to increase the delay in between each sequential write-retry of the
	// same back off sequence. Used in case the property is < 2, as 2 is also the lowest possible multiplier accepted.
	DefaultRetryDelayMultiplier = 2

	// DefaultBatchSize defines amount of rows of data a worker
	// will collect prior to writing it to BQ. Used in case the property is 0 (e.g. when undefined).
	DefaultBatchSize = 200

	// DefaultMaxBatchDelay defines the max amount of time a worker batches rows, prior to writing the batched rows,
	// even when not yet full. Used in case the property is 0 (e.g. when undefined).
	DefaultMaxBatchDelay = 5 * time.Second

	// DefaultWorkerQueueSize defines the default size of the job queue per worker used
	// in order to allow the Streamer's users to write rows even if all workers are currently
	// too busy to accept new incoming rows. Used in case the property is 0 (e.g. when undefined).
	DefaultWorkerQueueSize = 100
)
