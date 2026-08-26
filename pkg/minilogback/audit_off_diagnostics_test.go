package minilogback_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

func TestAuditOffLeaseQueriesRemainAvailable(t *testing.T) {
	appender, err := minilogback.New(minilogback.Config{
		RingCapacity: 8,
		AuditMode:    minilogback.AuditOff,
		Sink:         minilogback.NewWriterSink(io.Discard),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := appender.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("SnapshotLeases panicked with audit disabled: %v", recovered)
		}
	}()
	if snapshots := appender.SnapshotLeases("", 10); len(snapshots) != 0 {
		t.Fatalf("SnapshotLeases returned %d entries; want none", len(snapshots))
	}
}
