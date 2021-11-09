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

package batch

import (
	"testing"

	"github.com/OTA-Insight/bqwriter/internal/test"

	"cloud.google.com/go/bigquery"
)

type TestClientConfig struct {
	BigQuerySchema   *bigquery.Schema
	SourceFormat     bigquery.DataFormat
	WriteDisposition bigquery.TableWriteDisposition
}

func newTestClient(t *testing.T, cfg *TestClientConfig) (*Client, error) {
	t.Helper()

	bqClient := new(bigquery.Client)

	if cfg == nil {
		cfg = new(TestClientConfig)
	}
	client, err := newClient(
		bqClient, "test", "test",
		false, cfg.SourceFormat, cfg.WriteDisposition,
		cfg.BigQuerySchema)
	return client, err
}

func TestBatchClientValidConfig(t *testing.T) {
	client, err := newTestClient(t, &TestClientConfig{SourceFormat: bigquery.JSON, WriteDisposition: bigquery.WriteAppend})
	test.AssertNoError(t, err)

	test.AssertEqual(t, client.sourceFormat, bigquery.JSON)
	test.AssertNil(t, client.schema)
	test.AssertEqual(t, client.writeDisposition, bigquery.WriteAppend)
}

func TestBatchClientInvalidData(t *testing.T) {
	client, err := newTestClient(t, &TestClientConfig{SourceFormat: bigquery.JSON})
	test.AssertNoError(t, err)

	_, putErr := client.Put([]string{})
	test.AssertError(t, putErr)
}

func TestBatchClientFlushNop(t *testing.T) {
	client, err := newTestClient(t, &TestClientConfig{SourceFormat: bigquery.JSON})
	test.AssertNoError(t, err)

	flushErr := client.Flush()
	test.AssertNoError(t, flushErr)
}
