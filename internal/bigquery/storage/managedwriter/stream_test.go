// Copyright 2021 OTA Insight Ltd
// Copyright 2021 Google LLC
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

package managedwriter

import (
	"context"
	"testing"

	"github.com/OTA-Insight/bqwriter/internal/test"

	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestManagedStream_OpenWithRetry(t *testing.T) {
	testCases := []struct {
		desc     string
		errors   []error
		wantFail bool
	}{
		{
			desc:     "no error",
			errors:   []error{nil},
			wantFail: false,
		},
		{
			desc: "transient failures",
			errors: []error{
				status.Errorf(codes.Unavailable, "try 1"),
				status.Errorf(codes.Unavailable, "try 2"),
				nil},
			wantFail: false,
		},
		{
			desc:     "terminal error",
			errors:   []error{status.Errorf(codes.InvalidArgument, "bad args")},
			wantFail: true,
		},
	}

	for _, tc := range testCases {
		ms := &ManagedStream{
			ctx: context.Background(),
			open: func(s string) (storagepb.BigQueryWrite_AppendRowsClient, error) {
				if len(tc.errors) == 0 {
					panic("out of errors")
				}
				err := tc.errors[0]
				tc.errors = tc.errors[1:]
				if err == nil {
					return &testAppendRowsClient{}, nil
				}
				return nil, err
			},
		}
		arc, ch, err := ms.openWithRetry()
		if tc.wantFail {
			test.AssertError(t, err)
		} else {
			test.AssertNoError(t, err)
		}
		if !tc.wantFail && err != nil {
			t.Errorf("case %s: wanted success, got %v", tc.desc, err)
		}
		if err == nil {
			test.AssertNotNil(t, arc)
			test.AssertNotNil(t, ch)
		}
	}
}

func TestManagedStream_FirstAppendBehavior(t *testing.T) {
	ctx := context.Background()

	var testARC *testAppendRowsClient
	testARC = &testAppendRowsClient{
		recvF: func() (*storagepb.AppendRowsResponse, error) {
			return &storagepb.AppendRowsResponse{
				Response: &storagepb.AppendRowsResponse_AppendResult_{},
			}, nil
		},
		sendF: func(req *storagepb.AppendRowsRequest) error {
			testARC.requests = append(testARC.requests, req)
			return nil
		},
	}
	schema := &descriptorpb.DescriptorProto{
		Name: proto.String("testDescriptor"),
	}

	ms := &ManagedStream{
		ctx: ctx,
		open: func(s string) (storagepb.BigQueryWrite_AppendRowsClient, error) {
			testARC.openCount = testARC.openCount + 1
			return testARC, nil
		},
		streamSettings: defaultStreamSettings(),
		fc:             newFlowController(0, 0),
	}
	ms.streamSettings.streamID = "FOO"
	ms.streamSettings.TraceID = "TRACE"
	ms.schemaDescriptor = schema

	fakeData := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}

	wantReqs := 3

	for i := 0; i < wantReqs; i++ {
		_, err := ms.AppendRows(ctx, fakeData, NoStreamOffset)
		test.AssertNoError(t, err)
	}

	test.AssertEqual(t, 1, testARC.openCount)
	test.AssertEqual(t, wantReqs, len(testARC.requests))

	for k, v := range testARC.requests {
		test.AssertNotNil(t, v)
		if k == 0 {
			test.AssertNotEqual(t, "", v.GetTraceId())
			test.AssertNotEqual(t, "", v.GetWriteStream())
			test.AssertNotNil(t, v.GetProtoRows().GetWriterSchema().GetProtoDescriptor())
		} else {
			test.AssertEqual(t, "", v.GetTraceId())
			test.AssertEqual(t, "", v.GetWriteStream())
			test.AssertNil(t, v.GetProtoRows().GetWriterSchema().GetProtoDescriptor())
		}
	}
}

type testAppendRowsClient struct {
	storagepb.BigQueryWrite_AppendRowsClient
	openCount int
	requests  []*storagepb.AppendRowsRequest
	sendF     func(*storagepb.AppendRowsRequest) error
	recvF     func() (*storagepb.AppendRowsResponse, error)
}

func (tarc *testAppendRowsClient) Send(req *storagepb.AppendRowsRequest) error {
	return tarc.sendF(req)
}

func (tarc *testAppendRowsClient) Recv() (*storagepb.AppendRowsResponse, error) {
	return tarc.recvF()
}
