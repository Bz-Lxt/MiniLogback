package minilogback_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

type fullWriteErrorWriter struct {
	bytes.Buffer
	failed bool
}

func (w *fullWriteErrorWriter) Write(data []byte) (int, error) {
	written, err := w.Buffer.Write(data)
	if err != nil {
		return written, err
	}
	if !w.failed {
		w.failed = true
		return written, errors.New("storage reported a post-write failure")
	}
	return written, nil
}

func TestAppenderRetryDoesNotReplayFullyWrittenRecord(t *testing.T) {
	output := &fullWriteErrorWriter{}
	appender, err := minilogback.New(minilogback.Config{
		RingCapacity:    8,
		BatchSize:       1,
		FlushInterval:   time.Hour,
		RetryBackoff:    time.Millisecond,
		MaxRetryBackoff: time.Millisecond,
		Sink:            minilogback.NewWriterSink(output),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result := appender.Info("archive-ready"); result != minilogback.Accepted {
		t.Fatalf("Info result = %v; want accepted", result)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := appender.Close(ctx); err != nil {
		t.Fatalf("Close after retry = %v", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var event struct {
		Message string `json:"message"`
	}
	if err := decoder.Decode(&event); err != nil {
		t.Fatalf("decode written event: %v; output=%q", err, output.String())
	}
	if event.Message != "archive-ready" {
		t.Fatalf("message = %q; want archive-ready", event.Message)
	}
	var duplicate json.RawMessage
	if err := decoder.Decode(&duplicate); !errors.Is(err, io.EOF) {
		t.Fatalf("writer output contains extra data: %s (decode error: %v)", duplicate, err)
	}
}
