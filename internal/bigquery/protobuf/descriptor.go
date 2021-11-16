package protobuf

import (
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func BigQuerySchemaToProtobufDescripor(schema bigquery.Schema) (*descriptorpb.DescriptorProto, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("adapt.BQSchemaToStorageTableSchema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("adapt.StorageSchemaToDescriptor: %w", err)
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		// nolint: goerr113
		return nil, errors.New("adapted descriptor is not a message descriptor")
	}
	protobufDescriptor, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, fmt.Errorf("adapt.NormalizeDescriptor: %w", err)
	}
	return protobufDescriptor, nil
}
