package minilogback

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"
)

func BenchmarkAppenderConvenienceInfo256B(b *testing.B) {
	a, err := New(Config{RingCapacity: 1 << 16, BatchSize: 1024, Sink: NewWriterSink(io.Discard)})
	if err != nil {
		b.Fatal(err)
	}
	message := string(make([]byte, 180))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for a.Info(message) == QueueFull {
			runtime.Gosched()
		}
	}
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTryPublishLease(b *testing.B) {
	a, err := New(Config{RingCapacity: 1024, BatchSize: 1, Sink: NewWriterSink(io.Discard)})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		lease, acquireErr := a.Acquire(256)
		if acquireErr != nil {
			b.Fatal(acquireErr)
		}
		if _, writeErr := lease.Write(make([]byte, 256)); writeErr != nil {
			b.Fatal(writeErr)
		}
		b.StartTimer()
		for a.TryPublishLease(InfoLevel, lease) == QueueFull {
			runtime.Gosched()
		}
		b.StopTimer()
		for a.Stats().Pool.Outstanding != 0 {
			runtime.Gosched()
		}
		b.StartTimer()
	}
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		b.Fatal(err)
	}
}
