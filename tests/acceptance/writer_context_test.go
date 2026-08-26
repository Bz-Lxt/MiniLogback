package acceptance_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

type stagedShortWriter struct {
	firstWrite   chan struct{}
	releaseFirst chan struct{}
	releaseOnce  sync.Once
	calls        int
}

func newStagedShortWriter() *stagedShortWriter {
	return &stagedShortWriter{firstWrite: make(chan struct{}), releaseFirst: make(chan struct{})}
}

func (w *stagedShortWriter) Write(payload []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		close(w.firstWrite)
		<-w.releaseFirst
		return 1, nil
	}
	return len(payload), nil
}

func (w *stagedShortWriter) release() {
	w.releaseOnce.Do(func() { close(w.releaseFirst) })
}

func TestWriterSinkStopsAfterCancellationBetweenShortWrites(t *testing.T) {
	writer := newStagedShortWriter()
	defer writer.release()
	sink := minilogback.NewWriterSink(writer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- sink.WriteBatch(ctx, [][]byte{[]byte("partially written record")})
	}()

	select {
	case <-writer.firstWrite:
	case <-time.After(time.Second):
		t.Fatal("writer did not receive the first chunk")
	}
	cancel()
	writer.release()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WriteBatch error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WriteBatch did not return after cancellation")
	}
	if writer.calls != 1 {
		t.Fatalf("writer calls = %d, want 1 after cancellation", writer.calls)
	}
}
