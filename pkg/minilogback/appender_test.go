package minilogback

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type captureSink struct {
	mu      sync.Mutex
	records [][]byte
	notify  chan struct{}
	closed  int
}

func newCaptureSink() *captureSink { return &captureSink{notify: make(chan struct{}, 32)} }

func (s *captureSink) WriteBatch(_ context.Context, records [][]byte) error {
	s.mu.Lock()
	for _, record := range records {
		s.records = append(s.records, append([]byte(nil), record...))
	}
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

func (s *captureSink) Flush(context.Context) error { return nil }

func (s *captureSink) Close() error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}

func TestAppenderConvenienceAPIEncodesStructuredEvent(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{Name: "service", BatchSize: 1, Sink: s})
	if result := a.Info("started", String("region", "cn"), Int("workers", 8)); result != Accepted {
		t.Fatalf("Info result = %v", result)
	}
	select {
	case <-s.notify:
	case <-time.After(time.Second):
		t.Fatal("event was not flushed")
	}
	closeAppender(t, a)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.records) != 1 {
		t.Fatalf("records = %d", len(s.records))
	}
	var event map[string]any
	if err := json.Unmarshal(s.records[0], &event); err != nil {
		t.Fatalf("decode event: %v; payload=%q", err, s.records[0])
	}
	if event["level"] != "INFO" || event["logger"] != "service" || event["message"] != "started" {
		t.Fatalf("event = %+v", event)
	}
	if event["caller"] == nil || event["sequence"].(float64) == 0 {
		t.Fatalf("missing caller/sequence: %+v", event)
	}
}

func TestNamedLoggerAndLevelFiltering(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{Name: "root", MinLevel: WarnLevel, BatchSize: 1, Sink: s})
	logger := a.Named("database")
	if got := logger.Debug("hidden"); got != Filtered {
		t.Fatalf("Debug result = %v", got)
	}
	if got := logger.Error("failed"); got != Accepted {
		t.Fatalf("Error result = %v", got)
	}
	select {
	case <-s.notify:
	case <-time.After(time.Second):
		t.Fatal("event was not flushed")
	}
	closeAppender(t, a)
	s.mu.Lock()
	defer s.mu.Unlock()
	var event map[string]any
	if err := json.Unmarshal(s.records[0], &event); err != nil {
		t.Fatal(err)
	}
	if event["logger"] != "database" {
		t.Fatalf("logger = %v", event["logger"])
	}
}

func TestExplicitLeaseHonorsLevelFilterAndKeepsCallerOwnership(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{MinLevel: WarnLevel, Sink: s})
	lease, err := a.Acquire(8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Write([]byte("debug")); err != nil {
		t.Fatal(err)
	}
	if result := a.TryPublishLease(DebugLevel, lease); result != Filtered {
		t.Fatalf("filtered explicit result = %v", result)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("filtered lease ownership was not retained: %v", err)
	}
	if stats := a.Stats(); stats.PublishAttempts != 0 || stats.Filtered != 1 {
		t.Fatalf("filtered stats = %+v", stats)
	}
	closeAppender(t, a)
}

func TestExplicitLeaseOwnershipAndPublish(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{BatchSize: 1, Sink: s, AuditMode: AuditFull})
	lease, err := a.Acquire(32)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Write([]byte("pre-encoded\n")); err != nil {
		t.Fatal(err)
	}
	if got := a.TryPublishLease(ErrorLevel, lease); got != Accepted {
		t.Fatalf("TryPublishLease = %v", got)
	}
	if err := lease.Release(); !errors.Is(err, ErrLeaseTransferred) {
		t.Fatalf("Release after publish error = %v", err)
	}
	select {
	case <-s.notify:
	case <-time.After(time.Second):
		t.Fatal("explicit lease did not flush")
	}
	eventuallyAppender(t, time.Second, func() bool { return a.Stats().Pool.Outstanding == 0 })
	closeAppender(t, a)
}

func TestExplicitRejectedLeaseRemainsWithCaller(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{BatchSize: 1, Sink: s})
	lease, err := a.Acquire(8)
	if err != nil {
		t.Fatal(err)
	}
	lease.Write([]byte("payload"))
	a.closed.Store(true)
	a.queue.Close()
	if got := a.TryPublishLease(InfoLevel, lease); got != Closed {
		t.Fatalf("TryPublishLease after close = %v", got)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("caller could not release rejected lease: %v", err)
	}
	closeAppender(t, a)
}

func TestAppenderOversizedInvalidAndStatsInvariant(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{MaxEventBytes: 64, PoolClasses: []int{32, 64}, Sink: s})
	if got := a.Info(string(make([]byte, 100))); got != Oversized {
		t.Fatalf("oversized result = %v", got)
	}
	if got := a.TryPublishLease(Level(99), nil); got != InvalidLease {
		t.Fatalf("invalid lease result = %v", got)
	}
	stats := a.Stats()
	if stats.PublishAttempts != stats.Accepted+stats.RejectedFull+stats.RejectedClosed+stats.RejectedOversized+stats.RejectedInvalid {
		t.Fatalf("publish invariant failed: %+v", stats)
	}
	closeAppender(t, a)
}

func TestAppenderCloseDrainsAndIsIdempotent(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{BatchSize: 10, FlushInterval: time.Hour, Sink: s})
	if got := a.Warn("drain"); got != Accepted {
		t.Fatalf("Warn = %v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := a.Info("closed"); got != Closed {
		t.Fatalf("Info after Close = %v", got)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.records) != 1 || s.closed != 1 {
		t.Fatalf("sink records/closed = %d/%d", len(s.records), s.closed)
	}
}

func TestZeroConfigUsesSafeDefaults(t *testing.T) {
	a, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	closeAppender(t, a)
}

func TestSmallMaxEventBuildsCompatibleDefaultSlab(t *testing.T) {
	s := newCaptureSink()
	a, err := New(Config{MaxEventBytes: 64, Sink: s})
	if err != nil {
		t.Fatal(err)
	}
	closeAppender(t, a)
}

func TestPublicLoggingVariantsAndWriterAdapter(t *testing.T) {
	s := newCaptureSink()
	a, err := New(Config{RingCapacity: 64, BatchSize: 1}, WithSink(s), WithName("variants"))
	if err != nil {
		t.Fatal(err)
	}
	results := []PublishResult{
		a.Debug("debug"), a.Info("info"), a.Warn("warn"), a.Error("error"), a.Log(InfoLevel, "log"),
		a.Debugf("debug-%d", 1), a.Infof("info-%d", 2), a.Warnf("warn-%d", 3), a.Errorf("error-%d", 4),
	}
	logger := a.Named("")
	results = append(results,
		logger.Debug("debug"), logger.Info("info"), logger.Warn("warn"), logger.Error("error"),
		logger.Debugf("debug-%d", 5), logger.Infof("info-%d", 6), logger.Warnf("warn-%d", 7), logger.Errorf("error-%d", 8),
	)
	writer := a.Writer(InfoLevel)
	if _, err := writer.Write([]byte("standard log\n")); err != nil {
		t.Fatal(err)
	}
	for index, result := range results {
		if result != Accepted {
			t.Fatalf("variant %d result = %v", index, result)
		}
	}
	closeAppender(t, a)
	s.mu.Lock()
	recordCount := len(s.records)
	s.mu.Unlock()
	if recordCount != len(results)+1 {
		t.Fatalf("records = %d; want %d", recordCount, len(results)+1)
	}
	if _, err := writer.Write([]byte("after close")); err == nil {
		t.Fatal("writer hid a closed publish")
	}
}

func TestPublicLeaseAuditAndQueryHelpers(t *testing.T) {
	s := newCaptureSink()
	a := newTestAppender(t, Config{AuditMode: AuditFull, Sink: s})
	lease, err := a.AcquireFor(ErrorLevel, 32)
	if err != nil {
		t.Fatal(err)
	}
	if lease.ID() == 0 || lease.Capacity() < 32 || lease.ClassSize() < 32 {
		t.Fatalf("lease identity/capacity = %d/%d/%d", lease.ID(), lease.Capacity(), lease.ClassSize())
	}
	buffer, err := lease.Buffer()
	if err != nil {
		t.Fatal(err)
	}
	copy(buffer, "audited")
	if err := lease.Commit(len("audited")); err != nil {
		t.Fatal(err)
	}
	if err := lease.SetAuditContext(ErrorLevel, "worker"); err != nil {
		t.Fatal(err)
	}
	active := a.SnapshotLeasesBefore("BORROWED", 0, 10)
	if len(active) != 1 || active[0].ID != lease.ID() || active[0].Level != "ERROR" || active[0].Logger != "worker" {
		t.Fatalf("active audit = %+v", active)
	}
	if detail, ok := a.LeaseByID(lease.ID()); !ok || detail.Length != len("audited") {
		t.Fatalf("lease detail = %+v, %v", detail, ok)
	}
	if err := lease.SetBytes([]byte("replaced")); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	history := a.SnapshotLeases("RETURNED", 10)
	if len(history) != 1 || history[0].Length != len("replaced") {
		t.Fatalf("history = %+v", history)
	}
	closeAppender(t, a)
}

func TestPublicValidationAndFieldHelpers(t *testing.T) {
	s := newCaptureSink()
	invalidConfigs := []Config{
		{MinLevel: Level(99), Sink: s},
		{AuditMode: AuditMode("BAD"), Sink: s},
		{MaxEventBytes: -1, Sink: s},
		{RingCapacity: 3, Sink: s},
		{BatchSize: -1, Sink: s},
	}
	for index, config := range invalidConfigs {
		if _, err := New(config); err == nil {
			t.Fatalf("invalid config %d was accepted", index)
		}
	}
	if _, err := New(Config{Sink: s}, WithSink(nil)); err == nil {
		t.Fatal("nil sink option was accepted")
	}
	if _, err := New(Config{Sink: s}, WithName("")); err == nil {
		t.Fatal("empty name option was accepted")
	}
	if DebugLevel.String() != "DEBUG" || Level(99).String() != "UNKNOWN" {
		t.Fatal("level strings are unstable")
	}
	fields := []Field{Int64("i64", 1), Uint64("u64", 2), Bool("ok", true), Any("value", "x"), Err(nil), Err(errors.New("failed"))}
	if len(fields) != 6 || fields[4].Value != nil || fields[5].Value != "failed" {
		t.Fatalf("field helpers = %+v", fields)
	}
	a := newTestAppender(t, Config{Sink: s})
	if got := a.Info("invalid field", Any("channel", make(chan int))); got != InvalidLease {
		t.Fatalf("unencodable field result = %v", got)
	}
	empty, err := a.Acquire(8)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.TryPublishLease(InfoLevel, empty); got != InvalidLease {
		t.Fatalf("empty lease result = %v", got)
	}
	if err := empty.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AcquireFor(Level(99), 8); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("invalid AcquireFor error = %v", err)
	}
	closeAppender(t, a)
}

func TestPublicSinkConstructors(t *testing.T) {
	if _, err := NewNetworkSink(NetworkSinkConfig{}); err == nil {
		t.Fatal("empty public network config must fail")
	}
	writer := NewWriterSink(io.Discard)
	if err := writer.WriteBatch(context.Background(), [][]byte{[]byte("x")}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "public.log")
	fileSink, err := NewFileSink(FileSinkConfig{Path: path, SyncPolicy: FileSyncManual})
	if err != nil {
		t.Fatal(err)
	}
	if err := fileSink.WriteBatch(context.Background(), [][]byte{[]byte("public")}); err != nil {
		t.Fatal(err)
	}
	if err := fileSink.Close(); err != nil {
		t.Fatal(err)
	}
}

func newTestAppender(t *testing.T, config Config) *Appender {
	t.Helper()
	if config.RingCapacity == 0 {
		config.RingCapacity = 64
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = 10 * time.Millisecond
	}
	if config.AuditScanInterval == 0 {
		config.AuditScanInterval = time.Hour
	}
	a, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !a.Stats().Closed {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = a.Close(ctx)
		}
	})
	return a
}

func closeAppender(t *testing.T, a *Appender) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventuallyAppender(t *testing.T, timeout time.Duration, condition func() bool) {
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
