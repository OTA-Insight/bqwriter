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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/OTA-Insight/bqwriter"
	"github.com/loov/hrtime/hrtesting"
)

// you'll have to manually modify these in source code if you
// would look to benchmark it against your own tables,
// or feel free to open a PR to make this env or flag based instead.
const (
	benchmarkBigQueryProjectID = "oi-bigquery"
	benchmarkBigQueryDatasetID = "benchmarks_bqwriter"
	benchmarkBigQueryTableID   = "tmp"
)

type DataGenerator = func(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{}

var (
	randomNames       = []string{"Pripop", "Flutterprice", "Reudazzle", "Prireeka", "Pricemane"}
	randomBranchParam = []string{"main", "test", "staging", "dev"}
)

func BenchmarkStreamer(b *testing.B, streamerName string, testName string, streamer *bqwriter.Streamer, gen DataGenerator) {
	insertIDPrefix := fmt.Sprintf("bench-%d-", time.Now().Unix())
	bench := hrtesting.NewBenchmark(b)
	defer bench.Report()

	var i int
	for bench.Next() {
		data := gen(
			insertIDPrefix+strconv.Itoa(i),
			randomNames[i%len(randomNames)],
			int64(i)*42,
			time.Now(),
			i%15 != 0,
			map[string]string{
				"streamer":   streamerName,
				"testName":   testName,
				"fakeBranch": randomBranchParam[i%len(randomBranchParam)],
			},
		)

		err := streamer.Write(data)
		if err != nil {
			b.Fatalf("streamer write failed for data %v: %v", data, err)
		}

		i += 1
	}
}
