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
