package sink

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

type lifecycleWriter struct {
	bytes.Buffer
	flushes int
	closed  int
}

func (w *lifecycleWriter) Flush() error { w.flushes++; return nil }
func (w *lifecycleWriter) Close() error { w.closed++; return nil }

type shortWriter struct {
	buffer bytes.Buffer
	limit  int
}

func (w *shortWriter) Write(data []byte) (int, error) {
	if len(data) > w.limit {
		data = data[:w.limit]
	}
	return w.buffer.Write(data)
}

type stalledWriter struct{}

func (stalledWriter) Write([]byte) (int, error) { return 0, nil }

type partialErrorWriter struct {
	buffer bytes.Buffer
	failed bool
}

func (w *partialErrorWriter) Write(data []byte) (int, error) {
	if !w.failed {
		w.failed = true
		limit := min(2, len(data))
		written, _ := w.buffer.Write(data[:limit])
		return written, errors.New("injected partial failure")
	}
	return w.buffer.Write(data)
}

func TestWriterSinkCompletesShortWritesInOrder(t *testing.T) {
	w := &shortWriter{limit: 2}
	s := NewWriter(w)
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("abc"), nil, []byte("defg")}); err != nil {
		t.Fatal(err)
	}
	if got := w.buffer.String(); got != "abcdefg" {
		t.Fatalf("written bytes = %q", got)
	}
	stats := s.Stats()
	if stats.Batches != 1 || stats.Records != 3 || stats.Bytes != 7 || stats.ShortWrites == 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestWriterSinkRejectsNoProgress(t *testing.T) {
	s := NewWriter(stalledWriter{})
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("x")}); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("WriteBatch error = %v; want io.ErrNoProgress", err)
	}
}

func TestWriterSinkResumesAfterPartialCommitWithoutReplay(t *testing.T) {
	w := &partialErrorWriter{}
	s := NewWriter(w)
	records := [][]byte{[]byte("abcd"), []byte("ef")}
	if err := s.WriteBatch(context.Background(), records); err == nil {
		t.Fatal("first partial write unexpectedly succeeded")
	}
	if err := s.WriteBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if got := w.buffer.String(); got != "abcdef" {
		t.Fatalf("resumed bytes = %q; want one exact batch", got)
	}
	if stats := s.Stats(); stats.Batches != 1 || stats.Records != 2 || stats.Bytes != 6 {
		t.Fatalf("resumed stats = %+v", stats)
	}
}

func TestWriterSinkHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := NewWriter(io.Discard)
	if err := s.WriteBatch(ctx, [][]byte{[]byte("x")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteBatch error = %v; want context.Canceled", err)
	}
}

func TestWriterSinkLifecycleAndCapabilities(t *testing.T) {
	w := &lifecycleWriter{}
	s := NewOwnedWriter(w)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if w.flushes != 1 || w.closed != 1 {
		t.Fatalf("writer lifecycle flushes=%d closed=%d", w.flushes, w.closed)
	}
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("closed")}); err == nil {
		t.Fatal("closed writer sink accepted a batch")
	}
	if capability := s.Capabilities(); !capability.CopyAvoiding || capability.BatchMode != "sequential" {
		t.Fatalf("capability = %+v", capability)
	}
}
