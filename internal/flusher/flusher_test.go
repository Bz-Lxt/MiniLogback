package flusher

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/pool"
	"github.com/xavskye/minilogback/internal/ring"
	"github.com/xavskye/minilogback/internal/sink"
)

type recordedBatch struct {
	records [][]byte
}

type fakeSink struct {
	mu       sync.Mutex
	batches  []recordedBatch
	calls    chan int
	failed   chan struct{}
	failGate chan struct{}
	failures int
	flushes  int
	closed   int
}

type partialFlusherWriter struct {
	buffer bytes.Buffer
	failed bool
}

func (w *partialFlusherWriter) Write(data []byte) (int, error) {
	if !w.failed {
		w.failed = true
		written, _ := w.buffer.Write(data[:min(2, len(data))])
		return written, errors.New("injected partial commit")
	}
	return w.buffer.Write(data)
}

func newFakeSink() *fakeSink {
	return &fakeSink{calls: make(chan int, 16), failed: make(chan struct{}, 16)}
}

func (s *fakeSink) WriteBatch(_ context.Context, records [][]byte) error {
	s.mu.Lock()
	if s.failures > 0 {
		s.failures--
		gate := s.failGate
		s.mu.Unlock()
		s.failed <- struct{}{}
		if gate != nil {
			<-gate
		}
		return errors.New("injected sink failure")
	}
	copyBatch := recordedBatch{records: make([][]byte, len(records))}
	for index := range records {
		copyBatch.records[index] = append([]byte(nil), records[index]...)
	}
	s.batches = append(s.batches, copyBatch)
	s.mu.Unlock()
	s.calls <- len(records)
	return nil
}

func (s *fakeSink) Flush(context.Context) error {
	s.mu.Lock()
	s.flushes++
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) Close() error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}

func TestFlusherFlushesOnBatchSize(t *testing.T) {
	q, p, f, sink := newHarness(t, Config{BatchSize: 4, FlushInterval: time.Hour, PollInterval: 100 * time.Microsecond})
	f.Start()
	for i := 0; i < 4; i++ {
		publishLease(t, q, p, string(rune('a'+i)))
	}
	select {
	case size := <-sink.calls:
		if size != 4 {
			t.Fatalf("batch size = %d", size)
		}
	case <-time.After(time.Second):
		t.Fatal("batch threshold did not flush")
	}
	closeFlusher(t, f)
	if stats := p.Stats(); stats.Outstanding != 0 {
		t.Fatalf("outstanding leases = %d", stats.Outstanding)
	}
}

func TestFlusherFlushesFromFirstRecordDeadline(t *testing.T) {
	q, p, f, sink := newHarness(t, Config{BatchSize: 4, FlushInterval: 20 * time.Millisecond, PollInterval: 100 * time.Microsecond})
	f.Start()
	started := time.Now()
	publishLease(t, q, p, "one")
	select {
	case size := <-sink.calls:
		if size != 1 {
			t.Fatalf("batch size = %d", size)
		}
		if elapsed := time.Since(started); elapsed < 15*time.Millisecond || elapsed > 500*time.Millisecond {
			t.Fatalf("deadline flush elapsed = %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("time threshold did not flush")
	}
	closeFlusher(t, f)
}

func TestFlusherRetainsLeaseAcrossTransientSinkError(t *testing.T) {
	q, p, f, sink := newHarness(t, Config{
		BatchSize:       1,
		FlushInterval:   time.Second,
		PollInterval:    100 * time.Microsecond,
		RetryBackoff:    time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
	})
	sink.failures = 1
	sink.failGate = make(chan struct{})
	f.Start()
	publishLease(t, q, p, "retry")
	select {
	case <-sink.failed:
	case <-time.After(time.Second):
		t.Fatal("sink failure was not reached")
	}
	gate := sink.failGate
	gateClosed := false
	defer func() {
		if !gateClosed {
			close(gate)
		}
	}()
	if p.Stats().Outstanding != 1 {
		t.Fatal("lease returned before failed batch was retried")
	}
	close(gate)
	gateClosed = true
	select {
	case <-sink.calls:
	case <-time.After(time.Second):
		t.Fatal("failed batch was not retried")
	}
	eventually(t, time.Second, func() bool { return p.Stats().Outstanding == 0 })
	if retries := f.Stats().Retries; retries != 1 {
		t.Fatalf("retry attempts = %d; want 1", retries)
	}
	closeFlusher(t, f)
}

func TestFlusherRetryWithResumableSinkDoesNotDuplicateCommittedPrefix(t *testing.T) {
	q, err := ring.New[pool.Lease](64)
	if err != nil {
		t.Fatal(err)
	}
	p, err := pool.New(pool.Config{Classes: []int{64}, MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	writer := &partialFlusherWriter{}
	f, err := New(q, p, sink.NewWriter(writer), Config{BatchSize: 2, FlushInterval: time.Hour, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	f.Start()
	publishLease(t, q, p, "abcd")
	publishLease(t, q, p, "ef")
	eventually(t, time.Second, func() bool { return p.Stats().Outstanding == 0 })
	if got := writer.buffer.String(); got != "abcdef" {
		t.Fatalf("flusher replayed committed prefix: %q", got)
	}
	if retries := f.Stats().Retries; retries != 1 {
		t.Fatalf("retry attempts = %d; want 1", retries)
	}
	closeFlusher(t, f)
	p.Close()
}

func TestCloseDrainsPartialBatchAndIsIdempotent(t *testing.T) {
	q, p, f, sink := newHarness(t, Config{BatchSize: 10, FlushInterval: time.Hour, PollInterval: 100 * time.Microsecond})
	f.Start()
	publishLease(t, q, p, "drain")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := f.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case size := <-sink.calls:
		if size != 1 {
			t.Fatalf("drain size = %d", size)
		}
	default:
		t.Fatal("close did not drain pending event")
	}
	if p.Stats().Outstanding != 0 || !q.Closed() {
		t.Fatalf("close state: pool=%+v ring=%+v", p.Stats(), q.Stats())
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.flushes != 1 || sink.closed != 1 {
		t.Fatalf("sink lifecycle flush=%d close=%d", sink.flushes, sink.closed)
	}
}

func TestCloseTimeoutReportsRemaining(t *testing.T) {
	q, p, f, sink := newHarness(t, Config{BatchSize: 1, RetryBackoff: time.Second, MaxRetryBackoff: time.Second})
	sink.failures = 100
	f.Start()
	publishLease(t, q, p, "blocked")
	eventually(t, time.Second, func() bool { return f.Stats().Errors > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := f.Close(ctx)
	var timeout *CloseTimeoutError
	if !errors.As(err, &timeout) || timeout.InFlight == 0 {
		t.Fatalf("Close error = %#v; want remaining in-flight", err)
	}
}

func newHarness(t *testing.T, config Config) (*ring.Queue[pool.Lease], *pool.BytePool, *Flusher, *fakeSink) {
	t.Helper()
	q, err := ring.New[pool.Lease](64)
	if err != nil {
		t.Fatal(err)
	}
	p, err := pool.New(pool.Config{Classes: []int{64}, MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	s := newFakeSink()
	f, err := New(q, p, s, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !f.Stats().Closed {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			_ = f.Close(ctx)
		}
		p.Close()
	})
	return q, p, f, s
}

func publishLease(t *testing.T, q *ring.Queue[pool.Lease], p *pool.BytePool, value string) {
	t.Helper()
	lease, err := p.Acquire(len(value))
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.SetBytes([]byte(value)); err != nil {
		t.Fatal(err)
	}
	if err := lease.MarkQueued(); err != nil {
		t.Fatal(err)
	}
	if result := q.TryPublish(lease); result != ring.PublishAccepted {
		t.Fatalf("publish = %v", result)
	}
}

func closeFlusher(t *testing.T, f *Flusher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := f.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
	t.Fatal("condition did not become true")
}
