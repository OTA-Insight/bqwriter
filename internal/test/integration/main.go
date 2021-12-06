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
	"time"
)

const (
	defaultIterations = 100

	defaultBQProject = "oi-bigquery"
	defaultBQDataset = "benchmarks_bqwriter"
	defaultBQTable   = "tmp"
)

var cliConfig struct {
	Iterations int
	Workers    int
	Streamers  string
	Debug      bool

	BQProject string
	BQDataset string
	BQTable   string
}

func init() {
	flag.IntVar(&cliConfig.Iterations, "iterations", defaultIterations, "how many values to write to each of the different streamer tests")
	flag.IntVar(&cliConfig.Workers, "workers", runtime.NumCPU(), "how many workers to use to run tests in parallel")
	flag.StringVar(&cliConfig.Streamers, "streamers", "", "csv of streamers to test, one or multiple of following options: insertall, storage, storage-json, batch")
	flag.BoolVar(&cliConfig.Debug, "debug", false, "enable to show debug logs")

	flag.StringVar(&cliConfig.BQProject, "project", defaultBQProject, "BigQuery project to write data to")
	flag.StringVar(&cliConfig.BQDataset, "dataset", defaultBQDataset, "BigQuery dataset to write data to")
	flag.StringVar(&cliConfig.BQTable, "table", defaultBQTable, "BigQuery table to write data to")

	flag.Parse()

	if cliConfig.Iterations <= 0 {
		cliConfig.Iterations = defaultIterations
	}
	if cliConfig.BQProject == "" {
		cliConfig.BQProject = defaultBQProject
	}
	if cliConfig.BQDataset == "" {
		cliConfig.BQDataset = defaultBQDataset
	}
	if cliConfig.BQTable == "" {
		cliConfig.BQTable = defaultBQTable
	}
	if cliConfig.Workers <= 0 {
		cliConfig.Workers = runtime.NumCPU()
	}
}

type streamerTest = func(ctx context.Context, iterations int, logger *Logger, projectID, datasetID, tableID string) error

func createTestsForStreamers(logger *Logger, streamers string) []streamerTest {
	var tests []streamerTest
	for _, name := range strings.Split(streamers, ",") {
		switch strings.ToLower(name) {
		case "insertall":
			logger.Info("enable tests for streamer: insertAll")
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
			logger.Info("enable tests for streamer: storage")
			tests = append(
				tests,
				testStorageStreamerDefault,
				testStorageStreamerNoBatchSingleWorkerNoQueue,
				testStorageStreamerNoBatchSingleWorkerWithQueue,
				testStorageStreamerNoBatchMultiWorkerNoQueue,
				testStorageStreamerNoBatchMultiWorkerQueue,
			)
		case "storage-json":
			logger.Info("enable tests for streamer: storage-json")
			tests = append(
				tests,
				testStorageStreamerDefaultJson,
				testJsonStorageStreamerNoBatchSingleWorkerNoQueue,
				testJsonStorageStreamerNoBatchSingleWorkerWithQueue,
				testJsonStorageStreamerNoBatchMultiWorkerNoQueue,
				testJsonStorageStreamerNoBatchMultiWorkerQueue,
			)
		case "batch":
			logger.Info("enable tests for streamer: batch")
			tests = append(
				tests,
				testBatchStreamerDefault,
			)
		}
	}
	return tests
}

func main() {
	logger := NewLogger(cliConfig.Debug)

	// create tests
	tests := createTestsForStreamers(logger, cliConfig.Streamers)
	if len(tests) == 0 {
		tests = createTestsForStreamers(logger, "insertall,storage,storage-json,batch")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		logger.Info("cancel global context")
		cancel()
	}()

	testCh := make(chan streamerTest)
	resultCh := make(chan string)

	// ceate workers to run tests
	var workersWG sync.WaitGroup
	for i := 0; i < cliConfig.Workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			testWorkerfunc(ctx, logger, testCh, resultCh)
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
				if len(results) == len(tests) {
					return
				}
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
	close(testCh)

	// wait until all tests have completed
	workersWG.Wait()

	// print all results
	logger.Info("")
	logger.Info("----")
	logger.Info("")
	for _, result := range results {
		logger.Info(result)
	}
	logger.Info("")
	logger.Info("----")
	logger.Info("Bye!")
}

func testWorkerfunc(ctx context.Context, logger *Logger, testCh <-chan streamerTest, resultCh chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		case test, ok := <-testCh:
			if !ok {
				return
			}
			// nolint: govet
			testCtx, _ := context.WithTimeout(ctx, time.Second*10)
			err := test(
				testCtx,
				cliConfig.Iterations, logger,
				cliConfig.BQProject, cliConfig.BQDataset, cliConfig.BQTable,
			)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case resultCh <- fmt.Sprintf("test failed with error: %v", err):
				}
			}
			select {
			case <-ctx.Done():
				return
			case resultCh <- "test finished":
			}
		}
	}
}
