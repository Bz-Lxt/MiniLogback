package main

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/xavskye/minilogback/internal/sink"
)

type observedSink struct {
	inner sink.BatchSink
	mode  string
	mu    sync.Mutex
	times []uint64
	next  int
}

func newObservedSink(inner sink.BatchSink, mode string) *observedSink {
	return &observedSink{inner: inner, mode: mode, times: make([]uint64, 0, 2048)}
}

func (s *observedSink) WriteBatch(ctx context.Context, records [][]byte) error {
	started := time.Now()
	err := s.inner.WriteBatch(ctx, records)
	elapsed := uint64(time.Since(started))
	s.mu.Lock()
	if len(s.times) < cap(s.times) {
		s.times = append(s.times, elapsed)
	} else {
		s.times[s.next] = elapsed
		s.next = (s.next + 1) % len(s.times)
	}
	s.mu.Unlock()
	return err
}

func (s *observedSink) Flush(ctx context.Context) error { return s.inner.Flush(ctx) }
func (s *observedSink) Close() error                    { return s.inner.Close() }

func (s *observedSink) p95Micros() float64 {
	s.mu.Lock()
	values := append([]uint64(nil), s.times...)
	s.mu.Unlock()
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := (95*len(values) + 99) / 100
	if index > 0 {
		index--
	}
	return float64(values[index]) / float64(time.Microsecond)
}
