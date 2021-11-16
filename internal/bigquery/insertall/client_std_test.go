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

package insertall

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/OTA-Insight/bqwriter/internal/test"
)

func TestBQInsertAllStdClientPutError(t *testing.T) {
	c := &stdBQClient{
		client:    new(bigquery.Client),
		dataSetID: "a",
		tableID:   "b",
	}
	err := c.Put(context.Background(), "foo")
	test.AssertError(t, err, "expected error as string values aren't allowed")
}

func TestBQInsertAllStdClientCloseError(t *testing.T) {
	c := &stdBQClient{
		client:    new(bigquery.Client),
		dataSetID: "a",
		tableID:   "b",
	}
	err := c.Close()
	test.AssertNoError(t, err, "expected BQ Client to never return error for closing")
}
