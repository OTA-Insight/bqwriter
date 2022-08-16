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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/OTA-Insight/bqwriter/internal/test"
)

var (
	SimpleMessageSchema bigquery.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "value", Type: bigquery.IntegerFieldType},
	}
	SimpleNameOnlyMessageSchema bigquery.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
	}
)

type simpleMessageStruct struct {
	Name  string
	Value int
}

type simpleMessageStructWithoutValue struct {
	Name string
}

type simpleMessageStructWithoutName struct {
	Value int
}

type simpleMessageStructEmpty struct{}

func newSimpleMessageStruct(name string, value int) interface{} {
	if value <= 0 {
		if name == "" {
			return &simpleMessageStructEmpty{}
		}
		return &simpleMessageStructWithoutValue{Name: name}
	} else if name == "" {
		return &simpleMessageStructWithoutName{Value: value}
	}
	return &simpleMessageStruct{Name: name, Value: value}
}

type simpleMessageJsonMarshaller struct {
	optName  *string
	optValue *int
}

func newSimpleMessageJsonMarshaller(name string, value int) *simpleMessageJsonMarshaller {
	if value <= 0 {
		if name == "" {
			return &simpleMessageJsonMarshaller{}
		}
		return &simpleMessageJsonMarshaller{optName: &name}
	} else if name == "" {
		return &simpleMessageJsonMarshaller{optValue: &value}
	}
	return &simpleMessageJsonMarshaller{optName: &name, optValue: &value}
}

func (sm *simpleMessageJsonMarshaller) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	if sm.optName != nil {
		m["name"] = *sm.optName
	}
	if sm.optValue != nil {
		m["value"] = *sm.optValue
	}
	//nolint: wrapcheck
	return json.Marshal(m)
}

var _ json.Marshaler = (*simpleMessageJsonMarshaller)(nil)

type simpleMessageStringer struct {
	optName  *string
	optValue *int
}

func newSimpleMessageStringer(name string, value int) *simpleMessageStringer {
	if value <= 0 {
		if name == "" {
			return &simpleMessageStringer{}
		}
		return &simpleMessageStringer{optName: &name}
	} else if name == "" {
		return &simpleMessageStringer{optValue: &value}
	}
	return &simpleMessageStringer{optName: &name, optValue: &value}
}

func (sm *simpleMessageStringer) String() string {
	var s []string
	if sm.optName != nil {
		s = append(s, fmt.Sprintf(`name: "%s"`, *sm.optName))
	}
	if sm.optValue != nil {
		s = append(s, fmt.Sprintf(`value: %d`, *sm.optValue))
	}
	return strings.Join(s, "\n")
}

var _ interface{ String() string } = (*simpleMessageStringer)(nil)

func TestSchemaEncoder(t *testing.T) {
	const (
		expectedOutputHexForSurprise        = "0a1a73757270726973652c206e6f2076616c756520666f7220796f75"
		expectedOutputHexForSingleFieldFoo  = "0a03666f6f"
		expectedOutputHexForSingleFieldFive = "0a0466697665"
		expectedOutputForError              = ""
		expectedOutputForEmpty              = "NIL"
	)
	var (
		// proto encoding is not guaranteed to be deterministic,
		// see for example <https://gist.github.com/kchristidis/39c8b310fd9da43d515c4394c3cd9510> as a reference
		expectedOutputHexForOne  = []interface{}{"0a036f6e651001", "10010a036f6e65"}
		expectedOutputHexForFive = []interface{}{"0a04666976651005", "10050a0466697665"}
	)

	testCases := []struct {
		Message        string
		InputData      interface{}
		Schema         bigquery.Schema
		ExpectedOutput interface{}
	}{
		// test json-encoded binary input data
		{
			"json-encoded binary input data: expected to encode",
			[]byte(`{"name": "one", "value": 1}`),
			SimpleMessageSchema,
			expectedOutputHexForOne,
		},
		{
			"json-encoded binary input data: expected to encode",
			[]byte(`{"name": "five", "value": 5}`),
			SimpleMessageSchema,
			expectedOutputHexForFive,
		},
		{
			"json-encoded binary input data: expected to encode",
			[]byte(`{"name": "surprise, no value for you"}`),
			SimpleMessageSchema,
			expectedOutputHexForSurprise,
		},
		{
			"json-encoded binary input data: expected to fail as required name field is missing",
			[]byte(`{"value": 42}`),
			SimpleMessageSchema,
			expectedOutputForError,
		},
		// test text-encoded string input data
		{
			"text-encoded string input data: expected to encode",
			`name: "one"` + "\n" + `value: 1`,
			SimpleMessageSchema,
			expectedOutputHexForOne,
		},
		{
			"text-encoded string input data: expected to encode",
			`name: "five"` + "\n" + `value: 5`,
			SimpleMessageSchema,
			expectedOutputHexForFive,
		},
		{
			"text-encoded string input data: expected to encode",
			`name: "surprise, no value for you"`,
			SimpleMessageSchema,
			expectedOutputHexForSurprise,
		},
		{
			"text-encoded string input data: expected to fail as required field name is missing",
			`value: 42`,
			SimpleMessageSchema,
			expectedOutputForError,
		},
		// test message stringer, to turn an object into a human-friendly text-encoded message, which can be converted to a protobuf...
		{
			"Stringer object as input data: expected to encode",
			newSimpleMessageStringer("one", 1),
			SimpleMessageSchema,
			expectedOutputHexForOne,
		},
		{
			"Stringer object as input data: expected to encode",
			newSimpleMessageStringer("five", 5),
			SimpleMessageSchema,
			expectedOutputHexForFive,
		},
		{
			"Stringer object as input data: expected to encode",
			newSimpleMessageStringer("surprise, no value for you", -1),
			SimpleMessageSchema,
			expectedOutputHexForSurprise,
		},
		{
			"Stringer object as input data: expected to fail as required field name is missing",
			newSimpleMessageStringer(expectedOutputForError, 42),
			SimpleMessageSchema,
			expectedOutputForError, // expect an error as name is required
		},
		// test JsonMarshaller, to turn an object into a json encoded []byte slice, which can be converted to a protobuf...
		{
			"Json Marshaler object as input data: expected to encode",
			newSimpleMessageJsonMarshaller("one", 1),
			SimpleMessageSchema,
			expectedOutputHexForOne,
		},
		{
			"Json Marshaler object as input data: expected to encode",
			newSimpleMessageJsonMarshaller("five", 5),
			SimpleMessageSchema,
			expectedOutputHexForFive,
		},
		{
			"Json Marshaler object as input data: expected to encode",
			newSimpleMessageJsonMarshaller("surprise, no value for you", -1),
			SimpleMessageSchema,
			expectedOutputHexForSurprise,
		},
		{
			"Json Marshaler object as input data: expected to encode",
			newSimpleMessageJsonMarshaller(expectedOutputForError, 42),
			SimpleMessageSchema,
			expectedOutputForError, // expect an error as name is required
		},
		// test struct data, turned into a json object using whale reflection
		{
			"raw struct object as input data: expected to encode",
			newSimpleMessageStruct("one", 1),
			SimpleMessageSchema,
			expectedOutputHexForOne,
		},
		{
			"raw struct object as input data: expected to encode",
			newSimpleMessageStruct("five", 5),
			SimpleMessageSchema,
			expectedOutputHexForFive,
		},
		{
			"raw struct object as input data: expected to encode",
			newSimpleMessageStruct("surprise, no value for you", -1),
			SimpleMessageSchema,
			expectedOutputHexForSurprise,
		},
		{
			"raw struct object as input data: expected to fail as required field name is missing",
			newSimpleMessageStruct(expectedOutputForError, 42),
			SimpleMessageSchema,
			expectedOutputForError,
		},
		// using the above with a schema with overlapping fields will work as well
		{
			"json-encoded binary date as input data for single field message: expected to fail as field value is unknown",
			[]byte(`{"name": "one", "value": 1}`),
			SimpleNameOnlyMessageSchema,
			expectedOutputForError,
		},
		{
			"json-encoded binary date as input data for single field message: expected to encode",
			[]byte(`{"name": "foo"}`),
			SimpleNameOnlyMessageSchema,
			expectedOutputHexForSingleFieldFoo,
		},
		{
			"json-encoded binary date as input data for single field message: expected to encode, as the field isn't required",
			[]byte(`{}`),
			SimpleNameOnlyMessageSchema,
			expectedOutputForEmpty,
		},
		{
			"text encoded string as input data for single field message: expected to encode",
			`name: "foo"`,
			SimpleMessageSchema,
			expectedOutputHexForSingleFieldFoo,
		},
		{
			"raw struct object as input data for single field message: expected to work as extra/unknown value field should not be matched during reflecting",
			newSimpleMessageStruct("five", 5),
			SimpleNameOnlyMessageSchema,
			expectedOutputHexForSingleFieldFive,
		},
		{
			"raw struct object as input data for single field message: expected to work as extra/unknown value field should not be matched during reflecting",
			newSimpleMessageStruct("", 5),
			SimpleNameOnlyMessageSchema,
			expectedOutputForEmpty,
		},
		{
			"json marshaler object as input data for single field message: expected to fail as extra field value is unknown",
			newSimpleMessageJsonMarshaller("five", 5),
			SimpleNameOnlyMessageSchema,
			expectedOutputForError,
		},
		{
			"Json marshaler object as input data for single field message: expected to work",
			newSimpleMessageJsonMarshaller("foo", -1),
			SimpleNameOnlyMessageSchema,
			expectedOutputHexForSingleFieldFoo,
		},
		{
			"raw Json Marshaler object as input data for single field message: expected to fail as extra field value is unknown",
			newSimpleMessageJsonMarshaller("", 5),
			SimpleNameOnlyMessageSchema,
			expectedOutputForError,
		},
		{
			"raw Json Marshaler object as input data for single field message: expected to work as extra/unknown value field should not be matched during reflecting",
			newSimpleMessageJsonMarshaller("", -1),
			SimpleNameOnlyMessageSchema,
			expectedOutputForEmpty,
		},
		{
			"Stringer object as input data for single field message: expected to fail as extra field value is unknown",
			newSimpleMessageStringer("five", 5),
			SimpleNameOnlyMessageSchema,
			expectedOutputForError, // this will not work as the extra field (value) is unknown
		},
		{
			"Stringer object as input data for single field message: expected to work",
			newSimpleMessageStringer("foo", -1),
			SimpleNameOnlyMessageSchema,
			expectedOutputHexForSingleFieldFoo,
		},
		{
			"Stringer object as input data for single field message: expected to fail as extra field value is unknown",
			newSimpleMessageStringer("", 5),
			SimpleNameOnlyMessageSchema,
			expectedOutputForError, // this will not work however as it has an unknown field (value)
		},
		{
			"Stringer object as input data for single field message: expected to work",
			newSimpleMessageStringer("", -1),
			SimpleNameOnlyMessageSchema,
			expectedOutputForEmpty,
		},
	}
	for _, testCase := range testCases {
		displayInputData := testCase.InputData
		if b, ok := displayInputData.([]byte); ok {
			displayInputData = string(b)
		} else if s, ok := displayInputData.(string); ok {
			displayInputData = strings.Replace(s, "\n", "\\n", -1)
		}
		contextArgs := []interface{}{testCase.Message + " [%v => %v]", displayInputData, testCase.ExpectedOutput}
		encoder, err := NewSchemaEncoder(testCase.Schema)
		if !test.AssertNoError(t, err, contextArgs...) || !test.AssertNotNil(t, encoder, contextArgs...) {
			continue
		}
		rows, err := encoder.EncodeRows(testCase.InputData)
		if testCase.ExpectedOutput != expectedOutputForError {
			// test success case
			test.AssertNoError(t, err, contextArgs...)
			test.AssertNotNil(t, rows, contextArgs...)
			if !test.AssertEqual(t, 1, len(rows), contextArgs...) {
				continue
			}
			if testCase.ExpectedOutput == expectedOutputForEmpty {
				test.AssertEqual(t, 0, len(rows[0]), contextArgs...)
			} else {
				rowHexEncoded := hex.EncodeToString(rows[0])
				if expectedMultiOutput, ok := testCase.ExpectedOutput.([]interface{}); ok {
					test.AssertEqualAny(t, rowHexEncoded, expectedMultiOutput, contextArgs...)
				} else {
					test.AssertEqual(t, testCase.ExpectedOutput, rowHexEncoded, contextArgs...)
				}
			}
		} else {
			// test error case
			test.AssertError(t, err, contextArgs...)
			test.AssertNil(t, rows, contextArgs...)
		}
	}
}
