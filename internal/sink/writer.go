package sink

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// WriterSink adapts an io.Writer while preserving record slice boundaries.
type WriterSink struct {
	writer          io.Writer
	closeUnderlying bool
	mu              sync.Mutex
	pending         batchCursor
	counters        counters
	closeOnce       sync.Once
	closeErr        error
}

func NewWriter(writer io.Writer) *WriterSink {
	if writer == nil {
		writer = io.Discard
	}
	return &WriterSink{writer: writer}
}

// NewOwnedWriter creates a sink that also closes writer. NewWriter treats the
// supplied writer as borrowed, which is appropriate for os.Stdout/os.Stderr.
func NewOwnedWriter(writer io.Writer) *WriterSink {
	sink := NewWriter(writer)
	sink.closeUnderlying = true
	return sink
}

func (s *WriterSink) WriteBatch(ctx context.Context, records [][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counters.closed.Load() {
		return errors.New("sink is closed")
	}
	if err := s.pending.begin(records); err != nil {
		s.counters.errors.Add(1)
		return err
	}
	started := time.Now()
	written, shorts, err := writeSequential(ctx, s.writer, s.pending.remaining())
	s.counters.bytes.Add(uint64(written))
	s.counters.shortWrites.Add(shorts)
	s.counters.lastWriteNanos.Store(uint64(time.Since(started)))
	if advanceErr := s.pending.advance(written); advanceErr != nil {
		s.counters.errors.Add(1)
		return advanceErr
	}
	if err != nil {
		s.counters.errors.Add(1)
		return err
	}
	if !s.pending.done() {
		s.counters.errors.Add(1)
		return io.ErrShortWrite
	}
	s.counters.batches.Add(1)
	s.counters.records.Add(uint64(len(records)))
	s.pending.reset()
	return nil
}

func (s *WriterSink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	switch writer := s.writer.(type) {
	case interface{ Flush() error }:
		err = writer.Flush()
	}
	if err != nil {
		s.counters.errors.Add(1)
		return err
	}
	s.counters.flushes.Add(1)
	return nil
}

func (s *WriterSink) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.counters.closed.Store(true)
		if closer, ok := s.writer.(io.Closer); ok && s.closeUnderlying {
			s.closeErr = closer.Close()
			if s.closeErr != nil {
				s.counters.errors.Add(1)
			}
		}
	})
	return s.closeErr
}

func (s *WriterSink) Stats() Stats { return s.counters.snapshot() }

func (s *WriterSink) Capabilities() Capabilities {
	return Capabilities{BatchMode: "sequential", CopyAvoiding: true}
}
