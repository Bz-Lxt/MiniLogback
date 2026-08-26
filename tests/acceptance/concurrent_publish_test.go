package acceptance_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

func TestAppenderConcurrentAcceptedRecordsAreAllFlushed(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previousProcs)

	const publishers = 1024
	var output bytes.Buffer
	appender, err := minilogback.New(minilogback.Config{
		RingCapacity:  2048,
		BatchSize:     2048,
		FlushInterval: time.Hour,
		AuditMode:     minilogback.AuditOff,
		Sink:          minilogback.NewWriterSink(&output),
	})
	if err != nil {
		t.Fatal(err)
	}

	leases := make([]*minilogback.Lease, publishers)
	for index := range leases {
		lease, acquireErr := appender.Acquire(64)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		if writeErr := lease.SetBytes([]byte(fmt.Sprintf("record-%04d\n", index))); writeErr != nil {
			t.Fatal(writeErr)
		}
		leases[index] = lease
	}

	start := make(chan struct{})
	results := make(chan minilogback.PublishResult, publishers)
	releaseErrors := make(chan error, publishers)
	var wait sync.WaitGroup
	wait.Add(publishers)
	for _, lease := range leases {
		lease := lease
		go func() {
			defer wait.Done()
			<-start
			result := appender.TryPublishLease(minilogback.InfoLevel, lease)
			if result != minilogback.Accepted {
				if releaseErr := lease.Release(); releaseErr != nil {
					releaseErrors <- releaseErr
				}
			}
			results <- result
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(releaseErrors)

	accepted := 0
	rejected := 0
	for result := range results {
		if result == minilogback.Accepted {
			accepted++
		} else {
			rejected++
		}
	}
	for releaseErr := range releaseErrors {
		t.Errorf("release rejected lease: %v", releaseErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := appender.Close(ctx); err != nil {
		t.Fatal(err)
	}

	stats := appender.Stats()
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	seen := make(map[string]int, len(lines))
	duplicates := 0
	for _, line := range lines {
		key := string(line)
		seen[key]++
		if seen[key] > 1 {
			duplicates++
		}
	}
	missing := 0
	for index := range leases {
		if seen[fmt.Sprintf("record-%04d", index)] == 0 {
			missing++
		}
	}
	if accepted != publishers || len(lines) != accepted || missing != 0 || duplicates != 0 || stats.Flusher.Records != uint64(accepted) || stats.Pool.Outstanding != 0 {
		t.Fatalf("accepted=%d rejected=%d flushed_records=%d output_lines=%d unique_records=%d missing=%d duplicates=%d outstanding_leases=%d", accepted, rejected, stats.Flusher.Records, len(lines), len(seen), missing, duplicates, stats.Pool.Outstanding)
	}
}
