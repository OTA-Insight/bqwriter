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

package encoding

import "errors"

// Encoder is the interface required by the BigQuery storage client
// in order to encode the data into the protobuf expected format.
type Encoder interface {
	// EncodeRows encodes the data, as understood by the encoder,
	// into proto-encoded binary rows. InvalidDataError is to be returned
	// in case the data was of an unexpected type or format. Any other error
	// can be returned for all other possible error cases.
	EncodeRows(data interface{}) (rows [][]byte, err error)
}

var (
	// InvalidDataError is an error that can be returned by an Encoder
	// in case the given input data was invalid within the context of that encoder.
	InvalidDataError = errors.New("invalid data")
)
