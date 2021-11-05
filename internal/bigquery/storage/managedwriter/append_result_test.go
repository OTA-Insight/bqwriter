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

// nolint: goerr113
package managedwriter

import (
	"errors"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter/internal/test"
)

func TestAppendResult(t *testing.T) {
	wantRowBytes := [][]byte{[]byte("rowdata")}

	gotAR := newAppendResult(wantRowBytes)
	if test.AssertEqual(t, len(gotAR.rowData), len(wantRowBytes)) {
		for i := 0; i < len(gotAR.rowData); i++ {
			test.AssertBytesEqual(t, gotAR.rowData[i], wantRowBytes[i])
		}
	}
}

func TestPendingWrite(t *testing.T) {
	wantRowData := [][]byte{
		[]byte("row1"),
		[]byte("row2"),
		[]byte("row3"),
	}

	var wantOffset int64 = 99

	// first, verify no offset behavior
	pending := newPendingWrite(wantRowData, NoStreamOffset)
	test.AssertNil(t, pending.request.GetOffset())
	pending.markDone(NoStreamOffset, nil, nil)
	test.AssertEqual(t, pending.result.offset, NoStreamOffset)
	test.AssertNoError(t, pending.result.err)

	// now, verify behavior with a valid offset
	pending = newPendingWrite(wantRowData, 99)
	if test.AssertNotNil(t, pending.request.GetOffset()) {
		test.AssertEqual(t, pending.request.GetOffset().GetValue(), wantOffset)
	}

	// check request shape
	test.AssertEqual(t, len(pending.request.GetProtoRows().GetRows().GetSerializedRows()), len(wantRowData))

	// verify AppendResult

	gotData := pending.result.rowData
	test.AssertEqual(t, len(gotData), len(wantRowData))
	for i := 0; i < len(gotData); i++ {
		test.AssertBytesEqual(t, gotData[i], wantRowData[i])
	}
	select {
	case <-pending.result.Ready():
		t.Error("got Ready() on incomplete AppendResult")
	case <-time.After(100 * time.Millisecond):
		// pass
	}

	// verify completion behavior
	reportedOffset := int64(101)
	wantErr := errors.New("foo")
	pending.markDone(reportedOffset, wantErr, nil)

	test.AssertNil(t, pending.request)
	gotData = pending.result.rowData
	if test.AssertEqual(t, len(gotData), len(wantRowData)) {
		for i := 0; i < len(gotData); i++ {
			test.AssertBytesEqual(t, gotData[i], wantRowData[i])
		}
	}

	select {
	case <-time.After(100 * time.Millisecond):
		t.Error("possible blocking on completed AppendResult")
	case <-pending.result.Ready():
		if pending.result.offset != reportedOffset {
			t.Errorf("mismatch on completed AppendResult offset: got %d want %d", pending.result.offset, reportedOffset)
		}
		if !errors.Is(pending.result.err, wantErr) {
			t.Errorf("mismatch in errors, got %v want %v", pending.result.err, wantErr)
		}
	}
}
