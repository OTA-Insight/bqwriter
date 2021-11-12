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
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

var tmpDataBigQuerySchema = bigquery.Schema{
	&bigquery.FieldSchema{
		Name: "name",
		Type: bigquery.StringFieldType,
	},
	&bigquery.FieldSchema{
		Name: "uuid",
		Type: bigquery.IntegerFieldType,
	},
	&bigquery.FieldSchema{
		Name: "timestamp",
		Type: bigquery.TimestampFieldType,
	},
	&bigquery.FieldSchema{
		Name: "truth",
		Type: bigquery.BooleanFieldType,
	},
	&bigquery.FieldSchema{
		Name:     "parameters",
		Type:     bigquery.RecordFieldType,
		Repeated: true,
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{
				Name: "name",
				Type: bigquery.StringFieldType,
			},
			&bigquery.FieldSchema{
				Name: "value",
				Type: bigquery.StringFieldType,
			},
		},
	},
}

// NewTmpData create a ValueSaver/JsonMarshal-based temporary data model,
// implented using the dataGenerator syntax.
func NewTmpData(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{} {
	parameterSlice := make([]*tmpDataParameter, 0, len(parameters))
	for name, value := range parameters {
		parameterSlice = append(parameterSlice, &tmpDataParameter{
			Name:  name,
			Value: value,
		})
	}
	return &tmpData{
		InsertID:   insertID,
		Name:       name,
		Uuid:       uuid,
		Timestamp:  timestamp,
		Truth:      truth,
		Parameters: parameterSlice,
	}
}

// NewStorageTmpData create a ValueSaver/JsonMarshal-based temporary data model,
// implented using the dataGenerator syntax.
// Meant for storage writes.
func NewStorageTmpData(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{} {
	data := NewTmpData(insertID, name, uuid, timestamp, truth, parameters)
	t, _ := data.(*tmpData)
	t.TimestampAsEpoch = true
	return data
}

// tmpData is the data structure used by BigQuery and an implementation that is used
// for testing the InsertAll (as ValueSaver) and Batch client (as Json). The Storage
// API client is tested using the Protobuf definition instead.
type tmpData struct {
	InsertID         string
	TimestampAsEpoch bool

	Name       string
	Uuid       int64
	Timestamp  time.Time
	Truth      bool
	Parameters []*tmpDataParameter
}

// tmpDataParameter is the data type for the repeated Parameter
// Type that can be given as parameters for a tmpData record.
type tmpDataParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Save implements bigquery.ValueSaver.Save
func (td *tmpData) Save() (row map[string]bigquery.Value, insertID string, err error) {
	parameters := make([]bigquery.Value, 0, len(td.Parameters))
	for _, param := range td.Parameters {
		parameters = append(parameters, param.asBigqueryValue())
	}
	timestamp := civil.DateTimeOf(td.Timestamp).String()
	return map[string]bigquery.Value{
		"name":       td.Name,
		"uuid":       td.Uuid,
		"timestamp":  timestamp[:len(timestamp)-3],
		"truth":      td.Truth,
		"parameters": parameters,
	}, td.InsertID, nil
}

func (tp *tmpDataParameter) asBigqueryValue() bigquery.Value {
	return map[string]bigquery.Value{
		"name":  tp.Name,
		"value": tp.Value,
	}
}

// Save implements json.JsonMarshaler.MarshalJSON
func (td *tmpData) MarshalJSON() ([]byte, error) {
	var timestamp interface{} = civil.DateTimeOf(td.Timestamp)
	if td.TimestampAsEpoch {
		timestamp = td.Timestamp.UnixNano() / 1000
	}
	// nolint: wrapcheck
	return json.Marshal(map[string]interface{}{
		"name":       td.Name,
		"uuid":       td.Uuid,
		"timestamp":  timestamp,
		"truth":      td.Truth,
		"parameters": td.Parameters,
	})
}
