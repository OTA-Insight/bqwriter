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

import (
	"fmt"

	"github.com/OTA-Insight/bqwriter/internal"
	"google.golang.org/protobuf/proto"
)

// ProtobufEncoder is the preferred encoder shipped with the bqwriter package.
// It encodes any valid proto Message, the schema itself is to be defined upon creating
// the actual storage client.
//
// In case using a proto message is not an option for you,
// you can use the SchemaEncoder instead, but do not that you
// will be mostlikely pay a performance penalty for doing so.
type ProtobufEncoder struct{}

// interface compile-time compliance check
var (
	_ Encoder = (*ProtobufEncoder)(nil)
)

// NewProtobufEncoder creates a new ProtobufEncoder.
// See the documentation on ProtobufEncoder to know what values
// can be encoded using it.
func NewProtobufEncoder() *ProtobufEncoder {
	return &ProtobufEncoder{}
}

// EncodeRows implements Encoder::EncodeRows
//
// Data passed in as input and to be encoded is expected to be a single row of data only.
func (pbe *ProtobufEncoder) EncodeRows(data interface{}) ([][]byte, error) {
	msg, ok := data.(proto.Message)
	if !ok {
		return nil, fmt.Errorf(
			"ProtoBufEncoder: EncodeRows: data is expected to be a proto.Message"+
				", %T is not supported: %w", data, internal.ErrInvalidParam)
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("ProtobufEncoder: EncodeRows: failed to proto marshal message: %w", err)
	}
	return [][]byte{b}, nil
}
