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

package benchmark

import (
	"time"
)

// TODO: support parameters & timestamp once
// we know how  to support nested types

// NewProtoTmpData create a Protobuf-based temporary data model,
// implented using the DataGenerator syntax.
func NewProtoTmpData(insertID string, name string, uuid int64, timestamp time.Time, truth bool, _parameters map[string]string) interface{} {
	// parameterSlice := make([]*TemporaryDataParameterProto3, 0, len(parameters))
	// for name, value := range parameters {
	// 	parameterSlice = append(parameterSlice, &TemporaryDataParameterProto3{
	// 		Name:  name,
	// 		Value: []byte(value),
	// 	})
	// }
	return &TemporaryDataProto3{
		Name: name,
		Uuid: uuid,
		// Timestamp: &timestamppb.Timestamp{
		// 	Seconds: timestamp.Unix(),
		// },
		Truth: truth,
		// Parameters: parameterSlice,
	}
}
