package acceptance_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

func TestLeaseSnapshotMutationDoesNotCorruptAuditHistory(t *testing.T) {
	appender, err := minilogback.New(minilogback.Config{
		RingCapacity:      64,
		AuditMode:         minilogback.AuditFull,
		AuditScanInterval: time.Hour,
		AuditStackDepth:   8,
		Sink:              minilogback.NewWriterSink(io.Discard),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = appender.Close(ctx)
	})

	lease, err := appender.AcquireFor(minilogback.InfoLevel, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if err := lease.SetBytes([]byte("pending audit record")); err != nil {
		t.Fatal(err)
	}

	first := appender.SnapshotLeases("BORROWED", 1)
	if len(first) != 1 || len(first[0].Stack) == 0 {
		t.Fatalf("initial snapshot did not contain an active lease stack: %+v", first)
	}
	original := first[0].Stack[0]
	first[0].Stack[0].Function = "redacted by diagnostics caller"
	first[0].Stack[0].File = "redacted.go"

	second, ok := appender.LeaseByID(lease.ID())
	if !ok || len(second.Stack) == 0 {
		t.Fatalf("second lookup did not contain the active lease stack: %+v, found=%v", second, ok)
	}
	if second.Stack[0] != original {
		t.Fatalf("mutating one returned snapshot changed the appender's audit history: got %+v, want %+v", second.Stack[0], original)
	}
}
