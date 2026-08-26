package pool

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPoolAcquireUsesClassesAndLargeObjectPath(t *testing.T) {
	p := newTestPool(t, Config{Classes: []int{16, 64}, MaxBytes: 128})

	first, err := p.Acquire(12)
	if err != nil {
		t.Fatal(err)
	}
	if first.Capacity() != 16 || first.ClassSize() != 16 {
		t.Fatalf("small lease capacity/class = %d/%d", first.Capacity(), first.ClassSize())
	}
	if _, err := first.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := string(first.Payload()); got != "hello" {
		t.Fatalf("payload = %q", got)
	}
	if err := p.Release(first); err != nil {
		t.Fatal(err)
	}

	second, err := p.Acquire(96)
	if err != nil {
		t.Fatal(err)
	}
	if second.Capacity() != 96 || second.ClassSize() != 0 {
		t.Fatalf("large lease capacity/class = %d/%d", second.Capacity(), second.ClassSize())
	}
	if err := p.Release(second); err != nil {
		t.Fatal(err)
	}

	if _, err := p.Acquire(129); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized Acquire error = %v; want ErrTooLarge", err)
	}
	stats := p.Stats()
	if stats.Borrowed != 2 || stats.Returned != 2 || stats.Outstanding != 0 || stats.Oversized != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestLeaseStateMachineAndReturnProtection(t *testing.T) {
	p := newTestPool(t, Config{Classes: []int{32}, MaxBytes: 64})
	lease, err := p.Acquire(8)
	if err != nil {
		t.Fatal(err)
	}
	if lease.State() != StateBorrowed || lease.ID() == 0 {
		t.Fatalf("new lease state/id = %v/%d", lease.State(), lease.ID())
	}
	if err := lease.Resize(9); err != nil {
		t.Fatal(err)
	}
	writable, err := lease.Writable()
	if err != nil {
		t.Fatal(err)
	}
	copy(writable, "123456789")
	if err := lease.MarkQueued(); err != nil {
		t.Fatal(err)
	}
	if err := p.Release(lease); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("early Release error = %v; want ErrInvalidState", err)
	}
	if err := lease.MarkInFlight(); err != nil {
		t.Fatal(err)
	}
	if err := p.Release(lease); err != nil {
		t.Fatal(err)
	}
	if lease.State() != StateReturned {
		t.Fatalf("returned state = %v", lease.State())
	}
	if err := p.Release(lease); !errors.Is(err, ErrDoubleReturn) {
		t.Fatalf("second Release error = %v; want ErrDoubleReturn", err)
	}

	other := newTestPool(t, Config{Classes: []int{32}, MaxBytes: 64})
	foreign, err := other.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Release(foreign); !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("foreign Release error = %v; want ErrUnknownLease", err)
	}
	if err := other.Release(foreign); err != nil {
		t.Fatal(err)
	}

	stats := p.Stats()
	if stats.DoubleReturns != 1 || stats.InvalidReturns != 1 || stats.StateErrors == 0 {
		t.Fatalf("return protection counters = %+v", stats)
	}
}

func TestRejectedPublishCanRollbackToBorrower(t *testing.T) {
	p := newTestPool(t, Config{Classes: []int{32}, MaxBytes: 64})
	lease, err := p.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.MarkQueued(); err != nil {
		t.Fatal(err)
	}
	if err := lease.RollbackQueued(); err != nil {
		t.Fatal(err)
	}
	if lease.State() != StateBorrowed {
		t.Fatalf("state after rollback = %v", lease.State())
	}
	if err := p.Release(lease); err != nil {
		t.Fatal(err)
	}
}

func TestFullAuditCapturesOriginOverdueAndBoundedHistory(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	p := newTestPool(t, Config{
		Classes:      []int{32},
		MaxBytes:     64,
		AuditMode:    AuditFull,
		LeaseTimeout: time.Millisecond,
		ScanInterval: time.Hour,
		HistoryLimit: 1,
		StackDepth:   16,
		ProjectRoot:  root,
	})
	lease, err := p.AcquireContext(8, AuditContext{Level: "ERROR", Logger: "test"})
	if err != nil {
		t.Fatal(err)
	}
	lease.Append([]byte("audited"))

	p.ScanOverdue(time.Now().Add(time.Second))
	snapshots := p.SnapshotLeases("OVERDUE", 10)
	if len(snapshots) != 1 {
		t.Fatalf("overdue snapshots = %d", len(snapshots))
	}
	snapshot := snapshots[0]
	if snapshot.ID != lease.ID() || snapshot.State != "OVERDUE" || snapshot.Level != "ERROR" || snapshot.Logger != "test" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.Function == "" || snapshot.File == "" || filepath.IsAbs(snapshot.File) || len(snapshot.Stack) == 0 {
		t.Fatalf("origin was not captured/sanitized: %+v", snapshot)
	}
	if !strings.Contains(snapshot.Function, "TestFullAudit") {
		t.Fatalf("origin function = %q", snapshot.Function)
	}

	if err := p.Release(lease); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats().Overdue; got != 0 {
		t.Fatalf("overdue after release = %d", got)
	}
	history := p.SnapshotLeases("RETURNED", 10)
	if len(history) != 1 || history[0].ID != lease.ID() || history[0].Length != len("audited") {
		t.Fatalf("history = %+v", history)
	}
	if _, ok := p.LeaseByID(lease.ID()); !ok {
		t.Fatal("LeaseByID did not find bounded history item")
	}

	another, err := p.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Release(another); err != nil {
		t.Fatal(err)
	}
	history = p.SnapshotLeases("RETURNED", 10)
	if len(history) != 1 || history[0].ID != another.ID() {
		t.Fatalf("history bound not enforced: %+v", history)
	}
}

func TestSampledAuditTracksOnlyConfiguredFraction(t *testing.T) {
	p := newTestPool(t, Config{
		Classes:      []int{16},
		MaxBytes:     16,
		AuditMode:    AuditSampled,
		SampleEvery:  2,
		ScanInterval: time.Hour,
	})
	first, _ := p.Acquire(1)
	second, _ := p.Acquire(1)
	if got := len(p.SnapshotLeases("", 10)); got != 1 {
		t.Fatalf("sampled active leases = %d; want 1", got)
	}
	if err := p.Release(first); err != nil {
		t.Fatal(err)
	}
	if err := p.Release(second); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotLeasesBeforeAppliesCursorBeforeLimit(t *testing.T) {
	p := newTestPool(t, Config{
		Classes:      []int{16},
		MaxBytes:     16,
		AuditMode:    AuditFull,
		ScanInterval: time.Hour,
		HistoryLimit: 5,
	})
	for index := 0; index < 5; index++ {
		lease, err := p.Acquire(1)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Release(lease); err != nil {
			t.Fatal(err)
		}
	}
	first := p.SnapshotLeasesBefore("RETURNED", 0, 2)
	if len(first) != 2 || first[0].ID != 5 || first[1].ID != 4 {
		t.Fatalf("first page = %+v", first)
	}
	second := p.SnapshotLeasesBefore("RETURNED", 4, 2)
	if len(second) != 2 || second[0].ID != 3 || second[1].ID != 2 {
		t.Fatalf("second page = %+v", second)
	}
}

func TestAuditDetectsMutationAfterPublish(t *testing.T) {
	p := newTestPool(t, Config{Classes: []int{32}, MaxBytes: 32, AuditMode: AuditFull, ScanInterval: time.Hour})
	lease, _ := p.Acquire(8)
	lease.Append([]byte("before"))
	retained := lease.Payload()
	if err := lease.MarkQueued(); err != nil {
		t.Fatal(err)
	}
	retained[0] = 'x'
	if err := lease.MarkInFlight(); !errors.Is(err, ErrPayloadModified) {
		t.Fatalf("MarkInFlight error = %v; want ErrPayloadModified", err)
	}
	if err := p.Release(lease); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats().UseAfterPublish; got != 1 {
		t.Fatalf("use-after-publish count = %d", got)
	}
}

func TestAuditDetectsMutationWhileInFlightBeforeReturn(t *testing.T) {
	p := newTestPool(t, Config{Classes: []int{32}, MaxBytes: 32, AuditMode: AuditFull, ScanInterval: time.Hour})
	lease, _ := p.Acquire(8)
	lease.Append([]byte("before"))
	retained := lease.Payload()
	if err := lease.MarkQueued(); err != nil {
		t.Fatal(err)
	}
	if err := lease.MarkInFlight(); err != nil {
		t.Fatal(err)
	}
	retained[1] = 'X'
	if err := p.Release(lease); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats().UseAfterPublish; got != 1 {
		t.Fatalf("in-flight use-after-publish count = %d", got)
	}
	history := p.SnapshotLeases("RETURNED", 1)
	if len(history) != 1 || !history[0].PayloadMutated {
		t.Fatalf("returned audit history = %+v", history)
	}
}

func TestPoolConcurrentBorrowReturn(t *testing.T) {
	p := newTestPool(t, Config{Classes: []int{64}, MaxBytes: 64})
	const workers = 32
	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				lease, err := p.Acquire(32)
				if err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				if _, err := lease.Write([]byte("payload")); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
				if err := p.Release(lease); err != nil {
					t.Errorf("Release: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	stats := p.Stats()
	if stats.Borrowed != workers*iterations || stats.Returned != workers*iterations || stats.Outstanding != 0 {
		t.Fatalf("concurrent counters: %+v", stats)
	}
	if stats.PoolHits == 0 {
		t.Fatal("expected reusable slab hits")
	}
}

func TestPoolCloseIsIdempotent(t *testing.T) {
	p, err := New(Config{AuditMode: AuditFull, ScanInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close()
	if _, err := p.Acquire(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire after Close error = %v; want ErrClosed", err)
	}
}

func newTestPool(t *testing.T, config Config) *BytePool {
	t.Helper()
	if config.ScanInterval == 0 {
		config.ScanInterval = time.Hour
	}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
