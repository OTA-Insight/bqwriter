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

package internal

import "errors"

var (
	// internal error used in case an input parameter was invalid.
	ErrInvalidParam = errors.New("invalid parameter")

	ErrMutuallyExclusiveConfigs     = errors.New("you cannot define both a storage Client and a batch client at once")
	ErrAutoDetectSchemaNotSupported = errors.New("BQ batch client: autoDetect is only supported for JSON and CSV format")
	ErrProtobufOrSChemaRequired     = errors.New("StorageClientConfig invalid: either a Protobuf descriptor or BigQuery schema is required")
)
