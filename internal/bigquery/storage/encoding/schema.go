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
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// SchemaEncoder is an encoder that encodes the data into a single row,
// based on a dynamically defined BigQuery schema. If possible you should however
// use the ProtobufEncoder as it is much more efficient. On the contrary,
// this Encoder requires a lot of recflection as well as some possibly some extra trial-and-error.
//
// The following values can be encoded:
// - []byte, expected to be a Json encoded message from which it will proto-encode
//   using a json-driven decoder (see the official protobuf protojson package);
// - JsonMarshaler, which will be Json-encoded to []byte
//   and follow the same path as previous option from here;
// - string, expected to be a Text (human-friendly) encoded message from which it will
//   proto-encode using a text-driven decoder (see the official protobuf prototext package);
// - Stringer, which will be stringified to string
//   and follow the same path as previous option here;
//
// Any value of a type different than the ones listed above will be attempted to be encoded
// using the bigquery.StructSaver in order to be able to via that long road to Json-Encode
// it to a []byte value and use a Json-driven decoder in the end just as when you would
// have passed in a []byte value yourself. It's a long road and even much more so inefficient,
// you have been warned.
type SchemaEncoder struct {
	schema bigquery.Schema

	md              protoreflect.MessageDescriptor
	descriptorProto *descriptorpb.DescriptorProto
}

// interface compile-time compliance check
var (
	_ Encoder = (*SchemaEncoder)(nil)
)

// NewSchemaEncoder creates a new SchemaEncoder. Can fail in case the given
// bigquery.Schema cannot be converted to a Protobuf Descriptor. See the
// SchemaEncoder documentation to learn more about what kind of
// values can be encoded using it.
func NewSchemaEncoder(schema bigquery.Schema) (*SchemaEncoder, error) {
	md, descriptorProto, err := bqSchemaToProtoDesc(schema)
	if err != nil {
		return nil, fmt.Errorf("NewSchemaEncoder: %w", err)
	}
	return &SchemaEncoder{
		schema:          schema,
		md:              md,
		descriptorProto: descriptorProto,
	}, nil
}

// EncodeRows implements Encoder::EncodeRows
//
// Data passed in as input and to be encoded is expected to be a single row of data only.
func (se *SchemaEncoder) EncodeRows(data interface{}) ([][]byte, error) {
	message := dynamicpb.NewMessage(se.md)

	switch typedData := data.(type) {
	// json Proto
	case []byte:
		if err := protojson.Unmarshal(typedData, message); err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to Unmarshal json message as row: %w", err)
		}
	case json.Marshaler:
		bytes, err := typedData.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to Marshal json.Marshaler as bytes: %w", err)
		}
		if err := protojson.Unmarshal(bytes, message); err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to Unmarshal json (marshalled) message as row: %w", err)
		}

	// text Proto
	case string:
		if err := prototext.Unmarshal([]byte(typedData), message); err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to Unmarshal text message as row: %w", err)
		}
	case interface{ String() string }:
		if err := prototext.Unmarshal([]byte(typedData.String()), message); err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to Unmarshal Stringer as text message as row: %w", err)
		}

	// use it using the struct saver as a last resort,
	// very inefficient, so best to give json bytes instead
	default:
		bqMap, _, err := (&bigquery.StructSaver{
			Schema: se.schema,
			Struct: data,
		}).Save()
		if err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: unknown data type (%T) not useable as StructSaver: %w", data, err)
		}
		bytes, err := json.Marshal(bqMap)
		if err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: StructSaved data type (%T) failed to marshal as Json: %w", data, err)
		}
		if err := protojson.Unmarshal(bytes, message); err != nil {
			return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to Unmarshal struct-saved-to-json message as row: %w", err)
		}
	}

	b, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("SchemaEncoder: EncodeRows: failed to marshal proto bytes as row: %w", err)
	}
	return [][]byte{b}, nil
}

func bqSchemaToProtoDesc(schema bigquery.Schema) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("adapt.BQSchemaToStorageTableSchema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return nil, nil, fmt.Errorf("adapt.StorageSchemaToDescriptor: %w", err)
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		//nolint: goerr113
		return nil, nil, errors.New("adapted descriptor is not a message descriptor")
	}
	return messageDescriptor, protodesc.ToDescriptorProto(messageDescriptor), nil
}
