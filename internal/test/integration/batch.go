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
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/OTA-Insight/bqwriter"
	"github.com/OTA-Insight/bqwriter/log"
)

type genReader struct {
	ch       <-chan interface{}
	ctx      context.Context
	b        []byte
	consumed bool
}

func newGenReader(ctx context.Context, streamName, testName string, iterations int) *genReader {
	ch := newGenerator(ctx, streamName, testName, iterations, NewTmpData)
	return &genReader{
		ch:       ch,
		ctx:      ctx,
		b:        nil,
		consumed: false,
	}
}

// Read implements io.Reader.Read
func (gr *genReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	totalRead := 0
	targetRead := len(p)
	for {
		if len(gr.b) == 0 {
			if gr.consumed {
				p[0] = '\n'
				p = p[1:]
				totalRead += 1
			}
			select {
			case <-gr.ctx.Done():
				return totalRead, fmt.Errorf("genReader: read: %w", context.Canceled)
			case data, ok := <-gr.ch:
				if !ok {
					return totalRead, fmt.Errorf("genReader: read: gen ch closed: %w", io.EOF)
				}
				b, err := json.Marshal(data)
				if err != nil {
					return totalRead, fmt.Errorf("genReader: read: failed to Json marshal gen data: %w", err)
				}
				gr.b = b
				gr.consumed = true
			}
		}

		n := len(gr.b)
		if m := len(p); m < n {
			n = m
		}
		copy(p[:n], gr.b[:n])
		gr.b, p = gr.b[n:], p[n:]
		totalRead += n
		if totalRead == targetRead {
			return totalRead, nil
		}
	}
}

func testBatchStreamerDefault(ctx context.Context, iterations int, wg sync.WaitGroup, logger log.Logger, projectID, datasetID, tableID string) error {
	streamer, err := bqwriter.NewStreamer(
		ctx,
		projectID,
		datasetID,
		tableID,
		&bqwriter.StreamerConfig{
			BatchClient: new(bqwriter.BatchClientConfig),
			Logger:      logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create default Batch streamer: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		defer func() {
			logger.Debugf(
				"testStreamer: streamer=batch;testName=default;iterations=%d: duration: %v",
				iterations, time.Since(startTime),
			)
		}()
		defer streamer.Close()

		gr := newGenReader(ctx, "batch", "default", iterations)
		err := streamer.Write(gr)
		if err != nil {
			logger.Errorf("failed to write gen I/O reader: %w", err)
		}
	}()
	return nil
}
