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
	"fmt"
	"testing"

	"github.com/OTA-Insight/bqwriter/internal/bigquery/storage/encoding/testdata"
	"github.com/OTA-Insight/bqwriter/internal/test"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtobufEncoderProto2(t *testing.T) {
	testCases := []struct {
		InputName      string
		InputValue     int64
		ExpectedOutput string
	}{
		{
			"two", 2,
			"0a0374776f1002",
		},
		{
			"six", 6,
			"0a037369781006",
		},
		{
			"", 1,
			"0a001001",
		},
		{
			"three", 0,
			"0a0574687265651000",
		},
		{
			"", 0,
			"0a001000",
		},
	}

	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	for _, testCase := range testCases {
		contextArgs := []interface{}{
			fmt.Sprintf("[{%s + %d} => %s]", testCase.InputName, testCase.InputValue, testCase.ExpectedOutput),
		}
		inputData := &testdata.SimpleMessageProto2{
			Name:  &testCase.InputName,
			Value: &testCase.InputValue,
		}
		rows, err := encoder.EncodeRows(inputData)
		test.AssertNoError(t, err, contextArgs...)
		test.AssertEqual(t, 1, len(rows), contextArgs...)
		outputHexEncoded := hex.EncodeToString(rows[0])
		test.AssertEqual(t, testCase.ExpectedOutput, outputHexEncoded, contextArgs...)
	}
}

func TestProtobufEncoderProto2SingleFieldName(t *testing.T) {
	testCases := []struct {
		InputName      string
		ExpectedOutput string
	}{
		{
			"two",
			"0a0374776f",
		},
		{
			"six",
			"0a03736978",
		},
		{
			"",
			"0a00",
		},
	}

	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	for _, testCase := range testCases {
		contextArgs := []interface{}{
			fmt.Sprintf("[{%s} => %s]", testCase.InputName, testCase.ExpectedOutput),
		}
		inputData := &testdata.SimpleMessageProto2{
			Name: &testCase.InputName,
		}
		rows, err := encoder.EncodeRows(inputData)
		test.AssertNoError(t, err, contextArgs...)
		test.AssertEqual(t, 1, len(rows), contextArgs...)
		outputHexEncoded := hex.EncodeToString(rows[0])
		test.AssertEqual(t, testCase.ExpectedOutput, outputHexEncoded, contextArgs...)
	}
}

func TestProtobufEncoderProto2SingleFieldValue(t *testing.T) {
	var (
		inputValue int64 = 42
	)
	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	inputData := &testdata.SimpleMessageProto2{
		Value: &inputValue,
	}
	rows, err := encoder.EncodeRows(inputData)
	test.AssertError(t, err)
	test.AssertNil(t, rows)
}

func TestProtobufEncoderProto3(t *testing.T) {
	testCases := []struct {
		InputName      string
		InputValue     int64
		ExpectedOutput string
	}{
		{
			"two", 2,
			"0a0374776f12020802",
		},
		{
			"six", 6,
			"0a0373697812020806",
		},
		{
			"", 1,
			"12020801",
		},
		{
			"three", 0,
			"0a0574687265651200",
		},
		{
			"", 0,
			"1200",
		},
	}

	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	for _, testCase := range testCases {
		contextArgs := []interface{}{
			fmt.Sprintf("[{%s + %d} => %s]", testCase.InputName, testCase.InputValue, testCase.ExpectedOutput),
		}
		inputData := &testdata.SimpleMessageProto3{
			Name:  testCase.InputName,
			Value: &wrapperspb.Int64Value{Value: testCase.InputValue},
		}
		rows, err := encoder.EncodeRows(inputData)
		test.AssertNoError(t, err, contextArgs...)
		test.AssertEqual(t, 1, len(rows), contextArgs...)
		outputHexEncoded := hex.EncodeToString(rows[0])
		test.AssertEqual(t, testCase.ExpectedOutput, outputHexEncoded, contextArgs...)
	}
}

func TestProtobufEncoderProto3SingleFieldName(t *testing.T) {
	testCases := []struct {
		InputName      string
		ExpectedOutput string
	}{
		{
			"two",
			"0a0374776f",
		},
		{
			"six",
			"0a03736978",
		},
		{
			"",
			"",
		},
	}

	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	for _, testCase := range testCases {
		contextArgs := []interface{}{
			fmt.Sprintf("[{%s} => %s]", testCase.InputName, testCase.ExpectedOutput),
		}
		inputData := &testdata.SimpleMessageProto3{
			Name: testCase.InputName,
		}
		rows, err := encoder.EncodeRows(inputData)
		test.AssertNoError(t, err, contextArgs...)
		test.AssertEqual(t, 1, len(rows), contextArgs...)
		outputHexEncoded := hex.EncodeToString(rows[0])
		test.AssertEqual(t, testCase.ExpectedOutput, outputHexEncoded, contextArgs...)
	}
}

func TestProtobufEncoderProto3SingleFieldValue(t *testing.T) {
	testCases := []struct {
		InputValue     int64
		ExpectedOutput string
	}{
		{
			2,
			"12020802",
		},
		{
			6,
			"12020806",
		},
		{
			0,
			"1200",
		},
	}

	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	for _, testCase := range testCases {
		contextArgs := []interface{}{
			fmt.Sprintf("[{%d} => %s]", testCase.InputValue, testCase.ExpectedOutput),
		}
		inputData := &testdata.SimpleMessageProto3{
			Value: &wrapperspb.Int64Value{Value: testCase.InputValue},
		}
		rows, err := encoder.EncodeRows(inputData)
		test.AssertNoError(t, err, contextArgs...)
		test.AssertEqual(t, 1, len(rows), contextArgs...)
		outputHexEncoded := hex.EncodeToString(rows[0])
		test.AssertEqual(t, testCase.ExpectedOutput, outputHexEncoded, contextArgs...)
	}
}

func TestProtobufEncoderProto3Empty(t *testing.T) {
	encoder := NewProtobufEncoder()
	test.AssertNotNil(t, encoder)

	inputData := new(testdata.SimpleMessageProto3)
	rows, err := encoder.EncodeRows(inputData)
	test.AssertNoError(t, err)
	test.AssertEqual(t, 1, len(rows))
	test.AssertEqual(t, 0, len(rows[0]))
}
