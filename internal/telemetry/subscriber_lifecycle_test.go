package telemetry_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/telemetry"
)

func TestCanceledSubscriptionsDoNotRaceWithSampling(t *testing.T) {
	const subscriberCount = 512
	dropped := make(map[string]uint64, 4096)
	for index := 0; index < 4096; index++ {
		dropped[string(rune(index+128))] = uint64(index)
	}
	sampler, err := telemetry.NewSampler(telemetry.SourceFunc(func() telemetry.RawSnapshot {
		return telemetry.RawSnapshot{Ring: telemetry.RingMetrics{DroppedByLevel: dropped}}
	}), time.Hour, subscriberCount)
	if err != nil {
		t.Fatal(err)
	}

	cancels := make([]context.CancelFunc, 0, subscriberCount)
	updates := make([]<-chan telemetry.Snapshot, 0, subscriberCount)
	for index := 0; index < subscriberCount; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream, subscribeErr := sampler.Subscribe(ctx)
		if subscribeErr != nil {
			t.Fatal(subscribeErr)
		}
		<-stream
		cancels = append(cancels, cancel)
		updates = append(updates, stream)
	}

	firstUpdate := make(chan struct{})
	var firstOnce sync.Once
	var readers sync.WaitGroup
	readers.Add(len(updates))
	for _, stream := range updates {
		go func(stream <-chan telemetry.Snapshot) {
			defer readers.Done()
			if _, ok := <-stream; ok {
				firstOnce.Do(func() { close(firstUpdate) })
			}
		}(stream)
	}

	sampleResult := make(chan any, 1)
	go func() {
		defer func() { sampleResult <- recover() }()
		sampler.SampleNow()
	}()

	select {
	case <-firstUpdate:
	case <-time.After(5 * time.Second):
		t.Fatal("sampling did not publish an update")
	}
	for _, cancel := range cancels {
		cancel()
	}

	select {
	case panicValue := <-sampleResult:
		if panicValue != nil {
			t.Fatalf("sampling panicked while subscriptions were being canceled: %v", panicValue)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sampling did not finish")
	}

	readersDone := make(chan struct{})
	go func() {
		readers.Wait()
		close(readersDone)
	}()
	select {
	case <-readersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("subscribers did not observe sampling completion")
	}
}
