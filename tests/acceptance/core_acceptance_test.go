package acceptance_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/collector"
	"github.com/xavskye/minilogback/internal/protocol"
	"github.com/xavskye/minilogback/internal/ring"
	"github.com/xavskye/minilogback/internal/sink"
	"github.com/xavskye/minilogback/pkg/minilogback"
)

type sequenceItem struct {
	producer int
	sequence int
}

func TestMPSCProducerMatrixNoLossDuplicate(t *testing.T) {
	for _, producerCount := range []int{1, 2, 8, 32, 64} {
		producerCount := producerCount
		t.Run(fmt.Sprintf("producers_%d", producerCount), func(t *testing.T) {
			const perProducer = 300
			total := producerCount * perProducer
			queue, err := ring.New[sequenceItem](256)
			if err != nil {
				t.Fatal(err)
			}
			seen := make([][]bool, producerCount)
			for index := range seen {
				seen[index] = make([]bool, perProducer)
			}

			consumed := make(chan struct{})
			go func() {
				defer close(consumed)
				for count := 0; count < total; {
					item, ok := queue.TryConsume()
					if !ok {
						runtime.Gosched()
						continue
					}
					if item.producer < 0 || item.producer >= producerCount || item.sequence < 0 || item.sequence >= perProducer {
						t.Errorf("out-of-range item: %+v", *item)
						count++
						continue
					}
					if seen[item.producer][item.sequence] {
						t.Errorf("duplicate item: %+v", *item)
						count++
						continue
					}
					seen[item.producer][item.sequence] = true
					count++
				}
			}()

			var producers sync.WaitGroup
			producers.Add(producerCount)
			for producer := 0; producer < producerCount; producer++ {
				producer := producer
				go func() {
					defer producers.Done()
					for sequence := 0; sequence < perProducer; sequence++ {
						item := sequenceItem{producer: producer, sequence: sequence}
						for queue.TryPublish(&item) != ring.PublishAccepted {
							runtime.Gosched()
						}
						if (producer+sequence)%7 == 0 {
							runtime.Gosched()
						}
					}
				}()
			}
			producers.Wait()
			select {
			case <-consumed:
			case <-time.After(5 * time.Second):
				t.Fatal("consumer timed out")
			}
			for producer := range seen {
				for sequence, found := range seen[producer] {
					if !found {
						t.Fatalf("lost item producer=%d sequence=%d", producer, sequence)
					}
				}
			}
			stats := queue.Stats()
			if stats.Published != uint64(total) || stats.Consumed != uint64(total) || stats.Depth != 0 {
				t.Fatalf("counter invariant failed: %+v", stats)
			}
		})
	}
}

func TestMPSCCloseRacePreservesAcceptedItems(t *testing.T) {
	queue, err := ring.New[uint64](1024)
	if err != nil {
		t.Fatal(err)
	}
	var nextID atomic.Uint64
	var accepted atomic.Uint64
	var consumed atomic.Uint64
	producerDone := make(chan struct{})
	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)
		for {
			if _, ok := queue.TryConsume(); ok {
				consumed.Add(1)
				continue
			}
			select {
			case <-producerDone:
				if queue.Empty() {
					return
				}
			default:
				runtime.Gosched()
			}
		}
	}()

	var producers sync.WaitGroup
	producers.Add(32)
	for index := 0; index < 32; index++ {
		go func() {
			defer producers.Done()
			for {
				value := nextID.Add(1)
				switch queue.TryPublish(&value) {
				case ring.PublishAccepted:
					accepted.Add(1)
				case ring.PublishClosed:
					return
				case ring.PublishFull:
					runtime.Gosched()
				default:
					t.Errorf("unexpected publish result")
					return
				}
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	queue.Close()
	producers.Wait()
	close(producerDone)

	deadline := time.Now().Add(5 * time.Second)
	for consumed.Load() != accepted.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if accepted.Load() == 0 || consumed.Load() != accepted.Load() {
		t.Fatalf("accepted=%d consumed=%d stats=%+v", accepted.Load(), consumed.Load(), queue.Stats())
	}
	select {
	case <-consumerDone:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after close")
	}
}

type blockingSink struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingSink() *blockingSink {
	return &blockingSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingSink) WriteBatch(context.Context, [][]byte) error {
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.release
	return nil
}

func (*blockingSink) Flush(context.Context) error { return nil }
func (s *blockingSink) Close() error {
	s.unblock()
	return nil
}
func (s *blockingSink) unblock() { s.releaseOnce.Do(func() { close(s.release) }) }

func TestAppenderFullQueueRejectsImmediatelyAndReportsWatermark(t *testing.T) {
	blocked := newBlockingSink()
	appender, err := minilogback.New(minilogback.Config{
		RingCapacity: 64,
		BatchSize:    1,
		AuditMode:    minilogback.AuditOff,
		Sink:         blocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		blocked.unblock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = appender.Close(ctx)
	}()

	if got := appender.Info("occupy flusher"); got != minilogback.Accepted {
		t.Fatalf("first publish=%v", got)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("sink did not block the consumer")
	}
	for index := 0; index < 64; index++ {
		if got := appender.Info("fill", minilogback.Int("index", index)); got != minilogback.Accepted {
			t.Fatalf("fill publish %d=%v", index, got)
		}
	}
	started := time.Now()
	if got := appender.Error("must drop"); got != minilogback.QueueFull {
		t.Fatalf("full publish=%v", got)
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("full publish blocked for %v", elapsed)
	}
	stats := appender.Stats()
	if stats.Ring.HighWater != stats.Ring.Capacity || stats.Ring.Depth != stats.Ring.Capacity || stats.RejectedFull == 0 || stats.ByLevel["ERROR"].Dropped == 0 {
		t.Fatalf("full-queue telemetry mismatch: %+v", stats)
	}
}

type batchProbe struct{ calls chan int }

func newBatchProbe() *batchProbe { return &batchProbe{calls: make(chan int, 4)} }
func (s *batchProbe) WriteBatch(_ context.Context, records [][]byte) error {
	s.calls <- len(records)
	return nil
}
func (*batchProbe) Flush(context.Context) error { return nil }
func (*batchProbe) Close() error                { return nil }

func TestAppenderExactBatchAndDeadlineBoundaries(t *testing.T) {
	t.Run("1024_records", func(t *testing.T) {
		probe := newBatchProbe()
		appender, err := minilogback.New(minilogback.Config{
			RingCapacity:  2048,
			BatchSize:     1024,
			FlushInterval: time.Hour,
			AuditMode:     minilogback.AuditOff,
			Sink:          probe,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeAppender(t, appender)
		for index := 0; index < 1023; index++ {
			if result := appender.Info("boundary"); result != minilogback.Accepted {
				t.Fatalf("publish %d=%v", index, result)
			}
		}
		select {
		case size := <-probe.calls:
			t.Fatalf("flushed early with %d records", size)
		case <-time.After(15 * time.Millisecond):
		}
		if result := appender.Info("threshold"); result != minilogback.Accepted {
			t.Fatalf("threshold publish=%v", result)
		}
		select {
		case size := <-probe.calls:
			if size != 1024 {
				t.Fatalf("batch size=%d", size)
			}
		case <-time.After(time.Second):
			t.Fatal("1024-record threshold did not flush")
		}
	})

	t.Run("50ms_deadline", func(t *testing.T) {
		probe := newBatchProbe()
		appender, err := minilogback.New(minilogback.Config{
			RingCapacity:  2048,
			BatchSize:     1024,
			FlushInterval: 50 * time.Millisecond,
			AuditMode:     minilogback.AuditOff,
			Sink:          probe,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closeAppender(t, appender)
		started := time.Now()
		if result := appender.Info("deadline"); result != minilogback.Accepted {
			t.Fatalf("publish=%v", result)
		}
		select {
		case size := <-probe.calls:
			elapsed := time.Since(started)
			if size != 1 || elapsed < 40*time.Millisecond || elapsed > 500*time.Millisecond {
				t.Fatalf("deadline flush size=%d elapsed=%v", size, elapsed)
			}
		case <-time.After(time.Second):
			t.Fatal("50ms deadline did not flush")
		}
	})
}

func TestLostACKAcrossClientBudgetsKeepsBatchIDAndReleasesAfterDuplicateACK(t *testing.T) {
	var attempts atomic.Int32
	var sinkWrites atomic.Int32
	keys := make(chan collector.BatchKey, 2)
	errorsFromPeer := make(chan error, 2)
	dedupe := collector.NewDedupe(16)
	dial := func(context.Context, string, string) (net.Conn, error) {
		attempt := attempts.Add(1)
		client, peer := net.Pipe()
		go func() {
			defer peer.Close()
			batch, err := protocol.ReadBatch(peer, protocol.DefaultLimits())
			if err != nil {
				errorsFromPeer <- err
				return
			}
			key := collector.BatchKey{ClientID: batch.Header.ClientID, BatchID: batch.Header.BatchID}
			keys <- key
			duplicate, err := dedupe.Do(context.Background(), key, func() error {
				sinkWrites.Add(1)
				return nil
			})
			if err != nil {
				errorsFromPeer <- err
				return
			}
			if attempt == 1 {
				return
			}
			status := protocol.StatusAccepted
			if duplicate {
				status = protocol.StatusDuplicate
			}
			if err := protocol.WriteAck(peer, protocol.Ack{Status: status, ClientID: key.ClientID, BatchID: key.BatchID}); err != nil {
				errorsFromPeer <- err
			}
		}()
		return client, nil
	}
	client, err := protocol.NewClient(protocol.ClientConfig{
		Address:      "pipe",
		ClientID:     77,
		IOTimeout:    100 * time.Millisecond,
		RetryInitial: time.Millisecond,
		RetryMaximum: time.Millisecond,
		MaxAttempts:  1,
		DialContext:  dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := minilogback.New(minilogback.Config{
		RingCapacity: 64,
		BatchSize:    1,
		AuditMode:    minilogback.AuditFull,
		Sink:         sink.NewNetworkAdapter(client),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeAppender(t, appender)
	lease, err := appender.Acquire(64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Write([]byte("retry only after a lost acknowledgement")); err != nil {
		t.Fatal(err)
	}
	if result := appender.TryPublishLease(minilogback.InfoLevel, lease); result != minilogback.Accepted {
		t.Fatalf("publish=%v", result)
	}

	deadline := time.Now().Add(time.Second)
	for appender.Stats().Pool.Outstanding != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if appender.Stats().Pool.Outstanding != 0 {
		t.Fatal("lease was not returned after duplicate ACK")
	}
	if attempts.Load() != 2 || sinkWrites.Load() != 1 {
		t.Fatalf("attempts=%d sink_writes=%d", attempts.Load(), sinkWrites.Load())
	}
	first, second := <-keys, <-keys
	if first != second {
		t.Fatalf("retried identity changed: first=%+v second=%+v", first, second)
	}
	select {
	case err := <-errorsFromPeer:
		t.Fatalf("peer error: %v", err)
	default:
	}
}

func closeAppender(t *testing.T, appender *minilogback.Appender) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := appender.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
