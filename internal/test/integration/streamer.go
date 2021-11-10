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
	"fmt"
	"strconv"
	"time"

	"github.com/OTA-Insight/bqwriter"
	"github.com/OTA-Insight/bqwriter/log"
)

type dataGenerator = func(insertID string, name string, uuid int64, timestamp time.Time, truth bool, parameters map[string]string) interface{}

func newGenerator(ctx context.Context, streamerName, testName string, iterations int, gen dataGenerator) <-chan interface{} {
	ch := make(chan interface{})
	insertIDPrefix := fmt.Sprintf("test-%d-", time.Now().Unix())
	go func() {
		defer close(ch)
		for i := 0; i < iterations; i++ {
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

			select {
			case <-ctx.Done():
				return
			case ch <- data:
			}
		}
	}()
	return ch
}

var (
	randomNames       = []string{"Pripop", "Flutterprice", "Reudazzle", "Prireeka", "Pricemane"}
	randomBranchParam = []string{"main", "test", "staging", "dev"}
)

func testStreamer(ctx context.Context, iterations int, streamerName string, testName string, streamer *bqwriter.Streamer, gen dataGenerator, logger log.Logger) {
	startTime := time.Now()
	defer func() {
		logger.Debugf(
			"testStreamer: streamer=%s;testName=%s;iterations=%d: duration: %v",
			streamerName, testName, iterations, time.Since(startTime),
		)
	}()
	defer streamer.Close()

	for data := range newGenerator(ctx, streamerName, testName, iterations, gen) {
		err := streamer.Write(data)
		if err != nil {
			logger.Errorf("streamer write failed for data %v: %w", data, err)
		}
	}
}
