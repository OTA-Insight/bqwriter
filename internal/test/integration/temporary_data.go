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

package main

import (
	"time"
)

// NewProtoTmpData create a Protobuf-based temporary data model,
// implented using the dataGenerator syntax.
func NewProtoTmpData(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{} {
	parameterSlice := make([]*TemporaryDataParameterProto2, 0, len(parameters))
	for name, value := range parameters {
		name := name
		value := value
		parameterSlice = append(parameterSlice, &TemporaryDataParameterProto2{
			Name:  &name,
			Value: &value,
		})
	}
	epochMs := timestamp.UnixNano() / 1000
	return &TemporaryDataProto2{
		Name:       &name,
		Uuid:       &uuid,
		Timestamp:  &epochMs,
		Truth:      &truth,
		Parameters: parameterSlice,
	}
}
