package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/config"
	"github.com/xavskye/minilogback/internal/httpapi"
)

func TestDemoUsesRealRingAndAuditPool(t *testing.T) {
	cfg := config.Defaults()
	cfg.RingCapacity = 64
	cfg.BatchSize = 1
	cfg.FlushInterval = time.Millisecond
	cfg.AuditMode = "full"
	cfg.LeaseTimeout = time.Millisecond
	cfg.AuditInterval = time.Millisecond
	cfg.LogPath = filepath.Join(t.TempDir(), "demo.log")
	runtime, err := newRuntime(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	runtime.publish("info", 128)
	deadline := time.Now().Add(time.Second)
	for runtime.appender.Stats().Flusher.Records == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if runtime.appender.Stats().Flusher.Records != 1 {
		t.Fatalf("real flusher records=%d", runtime.appender.Stats().Flusher.Records)
	}

	lease, err := runtime.RetainLease(context.Background(), httpapi.DemoLeaseRequest{SizeBytes: 128, Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	var detail httpapi.Lease
	for time.Now().Before(deadline) {
		detail, err = runtime.LeaseByID(context.Background(), lease.ID)
		if err == nil && detail.State == "overdue" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if detail.State != "overdue" || len(detail.Stack) == 0 {
		t.Fatalf("expected audited overdue lease, got %+v", detail)
	}
	if err := runtime.ReleaseLease(context.Background(), lease.ID); err != nil {
		t.Fatal(err)
	}
}
