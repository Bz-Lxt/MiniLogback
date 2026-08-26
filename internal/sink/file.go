package sink

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SyncPolicy string

const (
	SyncManual     SyncPolicy = "MANUAL"
	SyncEveryBatch SyncPolicy = "EVERY_BATCH"
)

type FileConfig struct {
	Path           string
	MaxBytes       int64
	SyncPolicy     SyncPolicy
	FilePermission os.FileMode
	DirPermission  os.FileMode
}

type FileSink struct {
	config    FileConfig
	mu        sync.Mutex
	file      *os.File
	size      int64
	sequence  uint64
	pending   batchCursor
	writeFn   func(context.Context, *os.File, [][]byte) (int64, uint64, error)
	syncFn    func(*os.File) error
	counters  counters
	closeOnce sync.Once
	closeErr  error
}

func NewFile(config FileConfig) (*FileSink, error) {
	if config.Path == "" {
		return nil, errors.New("file sink path is required")
	}
	if config.MaxBytes < 0 {
		return nil, errors.New("file sink max bytes must not be negative")
	}
	if config.SyncPolicy == "" {
		config.SyncPolicy = SyncManual
	}
	if config.SyncPolicy != SyncManual && config.SyncPolicy != SyncEveryBatch {
		return nil, fmt.Errorf("unknown sync policy %q", config.SyncPolicy)
	}
	if config.FilePermission == 0 {
		config.FilePermission = 0o640
	}
	if config.DirPermission == 0 {
		config.DirPermission = 0o750
	}
	if err := os.MkdirAll(filepath.Dir(config.Path), config.DirPermission); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	s := &FileSink{config: config, writeFn: writeFileBatch, syncFn: func(file *os.File) error { return file.Sync() }}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileSink) WriteBatch(ctx context.Context, records [][]byte) error {
	started := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.counters.closed.Load() {
		return errors.New("file sink is closed")
	}
	if err := s.pending.begin(records); err != nil {
		s.counters.errors.Add(1)
		return err
	}

	for !s.pending.done() {
		if err := ctx.Err(); err != nil {
			s.counters.errors.Add(1)
			return err
		}
		remaining := s.pending.remaining()
		if s.pending.offset == 0 && s.shouldRotate(len(remaining[0])) {
			if err := s.rotate(); err != nil {
				s.counters.errors.Add(1)
				return err
			}
		}
		end := 1
		chunkBytes := int64(len(remaining[0]))
		for end < len(remaining) {
			next := int64(len(remaining[end]))
			if s.config.MaxBytes > 0 && s.size+chunkBytes > 0 && s.size+chunkBytes+next > s.config.MaxBytes {
				break
			}
			chunkBytes += next
			end++
		}
		written, shorts, err := s.writeFn(ctx, s.file, remaining[:end])
		s.size += written
		s.counters.bytes.Add(uint64(written))
		s.counters.shortWrites.Add(shorts)
		if advanceErr := s.pending.advance(written); advanceErr != nil {
			s.counters.errors.Add(1)
			return advanceErr
		}
		if err != nil {
			s.counters.errors.Add(1)
			return fmt.Errorf("write log batch: %w", err)
		}
	}

	if s.config.SyncPolicy == SyncEveryBatch {
		if err := ctx.Err(); err != nil {
			s.counters.errors.Add(1)
			return err
		}
		if err := s.syncFn(s.file); err != nil {
			s.counters.errors.Add(1)
			return fmt.Errorf("sync log file: %w", err)
		}
		s.counters.flushes.Add(1)
	}
	s.counters.batches.Add(1)
	s.counters.records.Add(uint64(len(records)))
	s.counters.lastWriteNanos.Store(uint64(time.Since(started)))
	s.pending.reset()
	return nil
}

func (s *FileSink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	if err := s.syncFn(s.file); err != nil {
		s.counters.errors.Add(1)
		return fmt.Errorf("sync log file: %w", err)
	}
	s.counters.flushes.Add(1)
	return nil
}

func (s *FileSink) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.counters.closed.Store(true)
		if s.file == nil {
			return
		}
		if err := s.syncFn(s.file); err != nil {
			s.closeErr = err
		}
		if err := s.file.Close(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.file = nil
		if s.closeErr != nil {
			s.counters.errors.Add(1)
		}
	})
	return s.closeErr
}

func (s *FileSink) Stats() Stats { return s.counters.snapshot() }

func (s *FileSink) Capabilities() Capabilities {
	return Capabilities{BatchMode: fileBatchMode, CopyAvoiding: true, DurableFlush: true}
}

func (s *FileSink) shouldRotate(nextRecord int) bool {
	return s.config.MaxBytes > 0 && s.size > 0 && s.size+int64(nextRecord) > s.config.MaxBytes
}

func (s *FileSink) rotate() error {
	if err := s.syncFn(s.file); err != nil {
		return fmt.Errorf("sync before rotation: %w", err)
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close before rotation: %w", err)
	}
	s.sequence++
	rotated := fmt.Sprintf("%s.%s.%06d", s.config.Path, time.Now().UTC().Format("20060102T150405.000000000Z"), s.sequence)
	if err := os.Rename(s.config.Path, rotated); err != nil {
		return fmt.Errorf("rotate log file: %w", err)
	}
	if err := s.open(); err != nil {
		return err
	}
	s.counters.rotations.Add(1)
	return nil
}

func (s *FileSink) open() error {
	file, err := os.OpenFile(s.config.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, s.config.FilePermission)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("stat log file: %w", err)
	}
	s.file = file
	s.size = info.Size()
	return nil
}
