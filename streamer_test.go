package bqwriter

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
)

func assertSignal(t *testing.T, signalCh <-chan struct{}) {
	select {
	case <-signalCh:
	case <-time.After(time.Millisecond * 500):
		t.Errorf("signal channel timed out after 500ms")
	}
}

// stubBQClient is an in-memory stub client for the bqClient interface,
// allowing us to see what data is written into it
type stubBQClient struct {
	rows       []interface{}
	nextErrors []error
	putSignal  chan<- struct{}
}

// Put implements bqClient::Put
func (sbqc *stubBQClient) Put(data interface{}) error {
	defer func() {
		if sbqc.putSignal != nil {
			sbqc.putSignal <- struct{}{}
		}
	}()
	if len(sbqc.nextErrors) > 0 {
		err := sbqc.nextErrors[0]
		sbqc.nextErrors = sbqc.nextErrors[1:]
		return err
	}
	if rows, ok := data.([]interface{}); ok {
		sbqc.rows = append(sbqc.rows, rows...)
	} else {
		sbqc.rows = append(sbqc.rows, data)
	}
	return nil
}

// Close implements bqClient::Close
func (sbqc *stubBQClient) Close() error {
	return nil
}

func (sbqc *stubBQClient) AddNextError(err error) {
	sbqc.nextErrors = append(sbqc.nextErrors, err)
}

func (sbqc *stubBQClient) SubscribeToPutSignal(ch chan<- struct{}) {
	sbqc.putSignal = ch
}

func (sbqc *stubBQClient) AssertStringSlice(t *testing.T, expected []string) {
	got := make([]string, 0, len(sbqc.rows))
	for _, row := range sbqc.rows {
		s, ok := row.(string)
		if !ok {
			t.Errorf("unexpected value (non-string): %v", row)
		} else {
			got = append(got, s)
		}
	}
	sort.Strings(got)
	sort.Strings(expected)
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("unexpected rows of data in client, expected %v, got %v", expected, got)
	}
}

func (sbqc *stubBQClient) AssertAnyStringSlice(t *testing.T, expectedSlice ...[]string) {
	got := make([]string, 0, len(sbqc.rows))
	for _, row := range sbqc.rows {
		s, ok := row.(string)
		if !ok {
			t.Errorf("unexpected value (non-string): %v", row)
		} else {
			got = append(got, s)
		}
	}
	sort.Strings(got)
	for _, expected := range expectedSlice {
		sort.Strings(expected)
		if reflect.DeepEqual(expected, got) {
			return
		}
	}
	t.Errorf("unexpected rows of data in client, expected any of %v, got %v", expectedSlice, got)
}

type testStreamerConfig struct {
	WorkerCount     int
	MaxRetries      uint64
	BatchSize       int
	MaxBatchDelay   time.Duration
	WorkerQueueSize int
}

func newTestStreamer(ctx context.Context, t *testing.T, cfg testStreamerConfig) (*stubBQClient, *Streamer) {
	client := new(stubBQClient)
	streamer, err := newStreamerWithClient(ctx, client, cfg.WorkerCount, cfg.MaxRetries, cfg.BatchSize, cfg.MaxBatchDelay, cfg.WorkerQueueSize, nil)
	if err != nil {
		t.Fatalf("streamer client creation didn't succeed: %v", err)
	}
	return client, streamer
}

func TestNewStreamerInputErrors(t *testing.T) {
	testCases := []struct {
		ProjectID string
		DataSetID string
		TableID   string
	}{
		{"", "", ""},
		{"a", "", ""},
		{"", "a", ""},
		{"", "", "a"},
		{"a", "b", ""},
		{"a", "", "b"},
		{"", "a", "b"},
		{"", "a", "b"},
	}
	for testCaseIndex, testCase := range testCases {
		streamer, err := NewStreamer(
			context.Background(),
			testCase.ProjectID, testCase.DataSetID, testCase.TableID,
			StreamerConfig{},
		)
		if err == nil {
			t.Errorf("testCase #%d: expected an error to be returned, non was received", testCaseIndex)
		}
		if streamer != nil {
			t.Errorf("testCase #%d: expected streamer client to be nil, received one", testCaseIndex)
			streamer.Close()
		}
	}
}

func TestStreamerFlowStandard(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{})
	streamer.Write("hello")
	streamer.Write("world")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})
	streamer.Close()
	client.AssertStringSlice(t, []string{"hello", "world"})
}

func TestStreamerRetryFailurePermanent(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		MaxRetries: 2,
	})

	client.AddNextError(errors.New("some kind of permanent error"))
	streamer.Write("hello")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})

	streamer.Close()
	// still empty as there was a failure (cannot retry permanent error)
	client.AssertStringSlice(t, []string{})
}

func TestStreamerRetryFailurePermanentGoogleAPI(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		MaxRetries: 2,
	})

	client.AddNextError(&googleapi.Error{Code: 404})
	streamer.Write("hello")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})

	streamer.Close()
	// still empty as there was a failure (cannot retry permanent error)
	client.AssertStringSlice(t, []string{})
}

func TestStreamerRetryFailureExhaust(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		MaxRetries: 1,
	})

	client.AddNextError(&googleapi.Error{
		Code: 500,
	})
	client.AddNextError(&googleapi.Error{
		Code: 503,
	})
	streamer.Write("hello")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})

	streamer.Close()
	// still empty as there was a failure too much
	client.AssertStringSlice(t, []string{})
}

func TestStreamerRetrySuccess(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		MaxRetries: 2,
	})

	client.AddNextError(&googleapi.Error{
		Code: 500,
	})
	client.AddNextError(&googleapi.Error{
		Code: 503,
	})
	streamer.Write("hello")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})

	streamer.Close()
	// after 2 retries it should have worked
	client.AssertStringSlice(t, []string{"hello"})
}

func TestStreamerBatchExhaustSingleWorker(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		WorkerCount: 1,
		BatchSize:   2,
	})
	putSignalCh := make(chan struct{}, 1)
	client.SubscribeToPutSignal(putSignalCh)

	streamer.Write("hello")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})

	streamer.Write("world")
	assertSignal(t, putSignalCh)
	client.AssertStringSlice(t, []string{"hello", "world"})

	streamer.Write("!")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{"hello", "world"})

	streamer.Close()
	client.AssertStringSlice(t, []string{"hello", "world", "!"})
}

func TestStreamerBatchExhaustMultiWorker(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		WorkerCount: 2,
		BatchSize:   2,
	})

	streamer.Write("hello")
	time.Sleep(time.Millisecond * 500)
	client.AssertStringSlice(t, []string{})
	streamer.Write("world")
	time.Sleep(time.Millisecond * 500)
	client.AssertAnyStringSlice(
		t,
		[]string{},
		[]string{"hello", "world"},
	)

	streamer.Write("!")
	time.Sleep(time.Millisecond * 500)
	client.AssertAnyStringSlice(
		t,
		[]string{"hello", "world"},
		[]string{"world", "!"},
		[]string{"hello", "!"},
	)

	streamer.Close()
	client.AssertStringSlice(t, []string{"hello", "world", "!"})
}

func TestStreamerBatchExhaustMaxDelaySingleWorker(t *testing.T) {
	client, streamer := newTestStreamer(context.Background(), t, testStreamerConfig{
		WorkerCount:   1,
		BatchSize:     100,
		MaxBatchDelay: time.Millisecond * 100,
	})

	streamer.Write("hello")
	time.Sleep(time.Millisecond * 200)
	client.AssertStringSlice(t, []string{"hello"})
	streamer.Write("world")
	time.Sleep(time.Millisecond * 200)
	client.AssertStringSlice(t, []string{"hello", "world"})

	streamer.Write("!")
	streamer.Close()
	client.AssertStringSlice(t, []string{"hello", "world", "!"})
}
