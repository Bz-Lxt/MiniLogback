package ring

import (
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkTryPublishConsume(b *testing.B) {
	q, err := New[int](1 << 16)
	if err != nil {
		b.Fatal(err)
	}
	var stopped atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stopped.Load() || q.Depth() != 0 {
			if _, ok := q.TryConsume(); !ok {
				runtime.Gosched()
			}
		}
	}()
	value := 1
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for q.TryPublish(&value) != PublishAccepted {
			runtime.Gosched()
		}
	}
	b.StopTimer()
	stopped.Store(true)
	<-done
}

func BenchmarkTryPublishConsumeParallel(b *testing.B) {
	q, err := New[int](1 << 16)
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		consumed := 0
		for consumed < b.N {
			if _, ok := q.TryConsume(); ok {
				consumed++
			} else {
				runtime.Gosched()
			}
		}
		close(done)
	}()
	value := 1
	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for q.TryPublish(&value) != PublishAccepted {
				runtime.Gosched()
			}
		}
	})
	b.StopTimer()
	<-done
}
