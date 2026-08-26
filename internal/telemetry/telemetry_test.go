package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestSamplerNormalizesSnapshotAndPublishesLatest(t *testing.T) {
	accepted := uint64(1)
	sampler, err := NewSampler(SourceFunc(func() RawSnapshot {
		return RawSnapshot{Ring: RingMetrics{Capacity: 100, Depth: 25, Accepted: accepted, DroppedByLevel: map[string]uint64{"error": 2}}}
	}), time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	current := sampler.Current()
	if current.Ring.WatermarkPercent != 25 || len(current.Ring.DroppedByLevel) != 4 || current.Pool.Classes == nil {
		t.Fatalf("snapshot not normalized: %+v", current)
	}
	ctx, cancel := context.WithCancel(context.Background())
	updates, err := sampler.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	<-updates
	accepted = 5
	sampler.SampleNow()
	select {
	case update := <-updates:
		if update.Ring.Accepted != 5 {
			t.Fatalf("accepted=%d", update.Ring.Accepted)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
	if _, err := sampler.Subscribe(context.Background()); err != ErrTooManySubscribers {
		t.Fatalf("expected subscriber limit, got %v", err)
	}
	cancel()
}
