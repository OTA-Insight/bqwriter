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
	"context"
	"flag"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/OTA-Insight/bqwriter/log"
)

const (
	defaultIterations = 1000

	defaultBQProject = "oi-bigquery"
	defaultBQDataset = "benchmarks_bqwriter"
	defaultBQTable   = "tmp"
)

var (
	iterations = flag.Int("iterations", defaultIterations, "how many values to write to each of the different streamer tests")
	workers    = flag.Int("workers", runtime.NumCPU(), "how many workers to use to run tests in parallel")
	streamers  = flag.String("streamers", "", "csv of streamers to test, one or multiple of following options: insertall, storage, batch")

	bqProject = flag.String("project", defaultBQProject, "BigQuery project to write data to")
	bqDataset = flag.String("dataset", defaultBQDataset, "BigQuery dataset to write data to")
	bqTable   = flag.String("table", defaultBQTable, "BigQuery table to write data to")
)

func init() {
	flag.Parse()
}

type streamerTest = func(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error

func createTestsForStreamers(streamers string) []streamerTest {
	var tests []streamerTest
	for _, name := range strings.Split(streamers, ",") {
		switch strings.ToLower(name) {
		case "insertall":
			tests = append(
				tests,
				testInsertAllStreamerDefault,
				testInsertAllStreamerNoBatchSingleWorkerNoQueue,
				testInsertAllStreamerNoBatchSingleWorkerWithQueue,
				testkInsertAllStreamerNoBatchMultiWorkerNoQueue,
				testInsertAllStreamerNoBatchMultiWorkerQueue,
				testInsertAllStreamerBatch50SingleWorkerNoQueue,
				testInsertAllStreamerBatch50SingleWorkerWithQueue,
				testInsertAllStreamerBatch50MultiWorkerNoQueue,
				testInsertAllStreamerBatch50MultiWorkerQueue,
			)
		case "storage":
			tests = append(
				tests,
				testStorageStreamerDefault,
				testStorageStreamerDefaultJson,
				testStorageStreamerNoBatchSingleWorkerNoQueue,
				testStorageStreamerNoBatchSingleWorkerWithQueue,
				testStorageStreamerNoBatchMultiWorkerNoQueue,
				testStorageStreamerNoBatchMultiWorkerQueue,
				testJsonStorageStreamerNoBatchSingleWorkerNoQueue,
				testJsonStorageStreamerNoBatchSingleWorkerWithQueue,
				testJsonStorageStreamerNoBatchMultiWorkerNoQueue,
				testJsonStorageStreamerNoBatchMultiWorkerQueue,
			)
		case "batch":
			tests = append(
				tests,
				testBatchStreamerDefault,
			)
		}
	}
	return tests
}

func main() {
	// create tests
	tests := createTestsForStreamers(*streamers)
	if len(tests) == 0 {
		tests = createTestsForStreamers("insertall,storage,batch")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCh := make(chan streamerTest)
	resultCh := make(chan string)

	// nolint: ifshort
	iterations := *iterations
	if iterations <= 0 {
		iterations = defaultIterations
	}

	logger := Logger{}

	// nolint: ifshort
	bqProject := *bqProject
	if bqProject == "" {
		bqProject = defaultBQProject
	}
	// nolint: ifshort
	bqDataset := *bqDataset
	if bqDataset == "" {
		bqDataset = defaultBQDataset
	}
	// nolint: ifshort
	bqTable := *bqTable
	if bqTable == "" {
		bqTable = defaultBQTable
	}

	// ceate workers to run tests
	var workersWG sync.WaitGroup
	workers := *workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	for i := 0; i < workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			var wg sync.WaitGroup
			for {
				select {
				case <-ctx.Done():
					return
				case test := <-testCh:
					err := test(
						ctx,
						iterations, wg, logger,
						bqProject, bqDataset, bqTable,
					)
					if err != nil {
						select {
						case <-ctx.Done():
							return
						case resultCh <- fmt.Sprintf("test %T failed with error: %v", test, err):
						}
					}
					// wait until test is complete
					wg.Wait()
					select {
					case <-ctx.Done():
						return
					case resultCh <- fmt.Sprintf("test %Tfinished", test):
					}
				}
			}
		}()
	}

	// gather all results
	var results []string
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case result := <-resultCh:
				results = append(results, result)
			}
		}
	}()

	// spawn tests one by one
	for _, test := range tests {
		select {
		case <-ctx.Done():
			return
		case testCh <- test:
		}
	}

	// wait until all tests have completed
	workersWG.Wait()

	// print all results
	logger.Debug("")
	logger.Debug("----")
	logger.Debug("")
	for _, result := range results {
		logger.Debug(result)
	}
	logger.Debug("")
	logger.Debug("----")
	logger.Debugf("Bye!")
}
