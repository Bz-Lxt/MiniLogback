package ring

import (
	"runtime"
	"sync"
	"testing"
)

func TestNewRejectsInvalidCapacity(t *testing.T) {
	for _, capacity := range []uint64{0, 1, 3, 6, 1 << 31} {
		t.Run(string(rune(capacity)), func(t *testing.T) {
			if _, err := New[int](capacity); err == nil {
				t.Fatalf("New(%d) succeeded; want an error", capacity)
			}
		})
	}
}

func TestResultAndSnapshotHelpers(t *testing.T) {
	results := map[PublishResult]string{
		PublishAccepted:  "accepted",
		PublishFull:      "queue_full",
		PublishClosed:    "closed",
		PublishInvalid:   "invalid",
		PublishResult(9): "unknown",
	}
	for result, want := range results {
		if got := result.String(); got != want {
			t.Fatalf("%d.String() = %q; want %q", result, got, want)
		}
	}
	q, err := New[int](4)
	if err != nil {
		t.Fatal(err)
	}
	if q.Capacity() != 4 || !q.Empty() {
		t.Fatalf("new queue capacity/empty = %d/%v", q.Capacity(), q.Empty())
	}
	stats := q.Stats()
	if stats.Dropped() != 0 || stats.Watermark() != 0 {
		t.Fatalf("empty helpers = %+v", stats)
	}
	if watermark := (Stats{Depth: 1, Capacity: 4}).Watermark(); watermark != 0.25 {
		t.Fatalf("watermark = %v", watermark)
	}
}

func TestQueueFIFOFullWrapAndStats(t *testing.T) {
	q, err := New[int](4)
	if err != nil {
		t.Fatal(err)
	}

	values := []*int{intPtr(0), intPtr(1), intPtr(2), intPtr(3)}
	for _, value := range values {
		if got := q.TryPublish(value); got != PublishAccepted {
			t.Fatalf("TryPublish() = %v; want accepted", got)
		}
	}
	if got := q.TryPublish(intPtr(4)); got != PublishFull {
		t.Fatalf("full TryPublish() = %v; want full", got)
	}
	if got := q.TryPublish(nil); got != PublishInvalid {
		t.Fatalf("nil TryPublish() = %v; want invalid", got)
	}

	for want := 0; want < 2; want++ {
		got, ok := q.TryConsume()
		if !ok || *got != want {
			t.Fatalf("TryConsume() = %v, %v; want %d, true", got, ok, want)
		}
	}
	for value := 4; value < 6; value++ {
		if got := q.TryPublish(intPtr(value)); got != PublishAccepted {
			t.Fatalf("wrapped TryPublish(%d) = %v", value, got)
		}
	}
	for want := 2; want < 6; want++ {
		got, ok := q.TryConsume()
		if !ok || *got != want {
			t.Fatalf("wrapped TryConsume() = %v, %v; want %d, true", got, ok, want)
		}
	}
	if value, ok := q.TryConsume(); ok || value != nil {
		t.Fatalf("empty TryConsume() = %v, %v; want nil, false", value, ok)
	}

	stats := q.Stats()
	if stats.Capacity != 4 || stats.Depth != 0 || stats.HighWater != 4 {
		t.Fatalf("unexpected size stats: %+v", stats)
	}
	if stats.PublishAttempts != 8 || stats.Published != 6 || stats.Consumed != 6 || stats.RejectedFull != 1 || stats.RejectedInvalid != 1 {
		t.Fatalf("unexpected counters: %+v", stats)
	}
}

func TestQueueCloseRejectsNewAndPreservesAccepted(t *testing.T) {
	q, err := New[int](2)
	if err != nil {
		t.Fatal(err)
	}
	value := 42
	if got := q.TryPublish(&value); got != PublishAccepted {
		t.Fatalf("publish = %v", got)
	}
	q.Close()
	q.Close()
	if !q.Closed() {
		t.Fatal("queue did not close")
	}
	if got := q.TryPublish(intPtr(7)); got != PublishClosed {
		t.Fatalf("publish after close = %v; want closed", got)
	}
	got, ok := q.TryConsume()
	if !ok || *got != value {
		t.Fatalf("consume accepted item = %v, %v", got, ok)
	}
}

func TestQueueSequenceWrapNearUint64Maximum(t *testing.T) {
	q, err := New[int](4)
	if err != nil {
		t.Fatal(err)
	}
	q.seedEmptyForTest(^uint64(0) - 2)
	for i := 0; i < 12; i++ {
		value := i
		if got := q.TryPublish(&value); got != PublishAccepted {
			t.Fatalf("publish %d across wrap = %v", i, got)
		}
		item, ok := q.TryConsume()
		if !ok || *item != i {
			t.Fatalf("consume %d across wrap = %v, %v", i, item, ok)
		}
	}
}

func TestQueueConcurrentProducersNoLossOrDuplicates(t *testing.T) {
	const (
		producerCount = 64
		perProducer   = 400
		total         = producerCount * perProducer
	)
	q, err := New[int](1024)
	if err != nil {
		t.Fatal(err)
	}

	seen := make([]bool, total)
	done := make(chan struct{})
	go func() {
		consumed := 0
		for consumed < total {
			value, ok := q.TryConsume()
			if !ok {
				runtime.Gosched()
				continue
			}
			if *value < 0 || *value >= total || seen[*value] {
				t.Errorf("invalid or duplicate value %d", *value)
				continue
			}
			seen[*value] = true
			consumed++
		}
		close(done)
	}()

	var producers sync.WaitGroup
	producers.Add(producerCount)
	for producer := 0; producer < producerCount; producer++ {
		producer := producer
		go func() {
			defer producers.Done()
			for sequence := 0; sequence < perProducer; sequence++ {
				value := producer*perProducer + sequence
				for q.TryPublish(&value) != PublishAccepted {
					runtime.Gosched()
				}
			}
		}()
	}
	producers.Wait()
	<-done

	for value, found := range seen {
		if !found {
			t.Fatalf("value %d was lost", value)
		}
	}
	stats := q.Stats()
	if stats.Published != total || stats.Consumed != total || stats.Depth != 0 {
		t.Fatalf("counter invariant failed: %+v", stats)
	}
}

func intPtr(value int) *int { return &value }
