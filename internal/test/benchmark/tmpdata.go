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
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

var TmpDataBigQuerySchema = bigquery.Schema{
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
		Type: bigquery.DateTimeFieldType,
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
				Type: bigquery.BytesFieldType,
			},
		},
	},
}

// NewTmpData create a ValueSaver/JsonMarshal-based temporary data model,
// implented using the DataGenerator syntax.
func NewTmpData(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{} {
	parameterSlice := make([]*TmpDataParameter, 0, len(parameters))
	for name, value := range parameters {
		parameterSlice = append(parameterSlice, &TmpDataParameter{
			Name:  name,
			Value: []byte(value),
		})
	}
	return &TmpData{
		InsertID:   insertID,
		Name:       name,
		Uuid:       uuid,
		Timestamp:  civil.DateTimeOf(timestamp),
		Truth:      truth,
		Parameters: parameterSlice,
	}
}

// TmpData is the data structure used by BigQuery and an implementation that is used
// for testing the InsertAll (as ValueSaver) and Batch client (as Json). The Storage
// API client is tested using the Protobuf definition instead.
type TmpData struct {
	InsertID string

	Name       string
	Uuid       int64
	Timestamp  civil.DateTime
	Truth      bool
	Parameters []*TmpDataParameter
}

// TmpDataParameter is the data type for the repeated Parameter
// Type that can be given as parameters for a TmpData record.
type TmpDataParameter struct {
	Name  string `json:"name"`
	Value []byte `json:"value"`
}

func (td *TmpData) Save() (row map[string]bigquery.Value, insertID string, err error) {
	parameters := make([]bigquery.Value, 0, len(td.Parameters))
	for _, param := range td.Parameters {
		parameters = append(parameters, param.AsBigqueryValue())
	}
	timestamp := td.Timestamp.String()
	return map[string]bigquery.Value{
		"name":       td.Name,
		"uuid":       td.Uuid,
		"timestamp":  timestamp[:len(timestamp)-3],
		"truth":      td.Truth,
		"parameters": parameters,
	}, td.InsertID, nil
}

func (tp *TmpDataParameter) AsBigqueryValue() bigquery.Value {
	return map[string]bigquery.Value{
		"name":  tp.Name,
		"value": tp.Value,
	}
}

func (td *TmpData) MarshalJSON() ([]byte, error) {
	// nolint: wrapcheck
	return json.Marshal(map[string]interface{}{
		"name":       td.Name,
		"uuid":       td.Uuid,
		"timestamp":  td.Timestamp.In(time.Local).Unix(),
		"truth":      td.Truth,
		"parameters": td.Parameters,
	})
}
