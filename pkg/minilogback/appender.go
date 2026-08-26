package minilogback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavskye/minilogback/internal/flusher"
	"github.com/xavskye/minilogback/internal/pool"
	"github.com/xavskye/minilogback/internal/ring"
	internalsink "github.com/xavskye/minilogback/internal/sink"
)

type levelCounters struct {
	attempts atomic.Uint64
	accepted atomic.Uint64
	dropped  atomic.Uint64
}

type Appender struct {
	config   Config
	queue    *ring.Queue[pool.Lease]
	bytePool *pool.BytePool
	flusher  *flusher.Flusher
	closed   atomic.Bool
	sequence atomic.Uint64

	publishAttempts   atomic.Uint64
	accepted          atomic.Uint64
	rejectedFull      atomic.Uint64
	rejectedClosed    atomic.Uint64
	rejectedOversized atomic.Uint64
	rejectedInvalid   atomic.Uint64
	filtered          atomic.Uint64
	levels            [4]levelCounters

	closeOnce sync.Once
	closeDone chan struct{}
	closeMu   sync.RWMutex
	closeErr  error
}

func New(config Config, options ...Option) (*Appender, error) {
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	if config.Sink == nil {
		config.Sink = internalsink.NewWriter(os.Stderr)
	}
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	queue, err := ring.New[pool.Lease](normalized.RingCapacity)
	if err != nil {
		return nil, err
	}
	bytePool, err := pool.New(pool.Config{
		Classes:      normalized.PoolClasses,
		MaxBytes:     normalized.MaxEventBytes,
		AuditMode:    pool.AuditMode(normalized.AuditMode),
		SampleEvery:  normalized.AuditSampleEvery,
		LeaseTimeout: normalized.LeaseTimeout,
		ScanInterval: normalized.AuditScanInterval,
		HistoryLimit: normalized.AuditHistoryLimit,
		StackDepth:   normalized.AuditStackDepth,
		ProjectRoot:  normalized.ProjectRoot,
	})
	if err != nil {
		return nil, err
	}
	batcher, err := flusher.New(queue, bytePool, normalized.Sink, flusher.Config{
		BatchSize:       normalized.BatchSize,
		FlushInterval:   normalized.FlushInterval,
		PollInterval:    normalized.PollInterval,
		RetryBackoff:    normalized.RetryBackoff,
		MaxRetryBackoff: normalized.MaxRetryBackoff,
	})
	if err != nil {
		bytePool.Close()
		return nil, err
	}
	a := &Appender{
		config:    normalized,
		queue:     queue,
		bytePool:  bytePool,
		flusher:   batcher,
		closeDone: make(chan struct{}),
	}
	batcher.Start()
	return a, nil
}

func (a *Appender) Debug(message string, fields ...Field) PublishResult {
	return a.log(a.config.Name, DebugLevel, message, fields)
}

func (a *Appender) Info(message string, fields ...Field) PublishResult {
	return a.log(a.config.Name, InfoLevel, message, fields)
}

func (a *Appender) Warn(message string, fields ...Field) PublishResult {
	return a.log(a.config.Name, WarnLevel, message, fields)
}

func (a *Appender) Error(message string, fields ...Field) PublishResult {
	return a.log(a.config.Name, ErrorLevel, message, fields)
}

func (a *Appender) Log(level Level, message string, fields ...Field) PublishResult {
	return a.log(a.config.Name, level, message, fields)
}

func (a *Appender) Debugf(format string, args ...any) PublishResult {
	return a.log(a.config.Name, DebugLevel, fmt.Sprintf(format, args...), nil)
}

func (a *Appender) Infof(format string, args ...any) PublishResult {
	return a.log(a.config.Name, InfoLevel, fmt.Sprintf(format, args...), nil)
}

func (a *Appender) Warnf(format string, args ...any) PublishResult {
	return a.log(a.config.Name, WarnLevel, fmt.Sprintf(format, args...), nil)
}

func (a *Appender) Errorf(format string, args ...any) PublishResult {
	return a.log(a.config.Name, ErrorLevel, fmt.Sprintf(format, args...), nil)
}

func (a *Appender) Acquire(capacity int) (*Lease, error) {
	return a.acquire(capacity, nil)
}

// AcquireFor records the intended level immediately, so an explicit lease
// that is retained before publication still has useful audit metadata.
func (a *Appender) AcquireFor(level Level, capacity int) (*Lease, error) {
	if !level.valid() {
		return nil, ErrInvalidLease
	}
	return a.acquire(capacity, &level)
}

func (a *Appender) acquire(capacity int, level *Level) (*Lease, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}
	context := pool.AuditContext{Logger: a.config.Name, CallerSkip: 2}
	if level != nil {
		context.Level = level.String()
	}
	inner, err := a.bytePool.AcquireContext(capacity, context)
	if err != nil {
		return nil, err
	}
	return &Lease{appender: a, inner: inner, id: inner.ID(), logger: a.config.Name}, nil
}

// TryPublishLease transfers ownership only when it returns Accepted. Every
// other result leaves the lease with the caller, who must Release it.
func (a *Appender) TryPublishLease(level Level, lease *Lease) PublishResult {
	if !level.valid() || lease == nil || lease.appender != a || lease.inner == nil || lease.id != lease.inner.ID() || lease.released.Load() || lease.transferred.Load() || lease.Len() <= 0 {
		a.publishAttempts.Add(1)
		a.rejectedInvalid.Add(1)
		return InvalidLease
	}
	if level < a.config.MinLevel {
		a.filtered.Add(1)
		return Filtered
	}
	a.publishAttempts.Add(1)
	a.levels[level].attempts.Add(1)
	if a.closed.Load() {
		a.rejectedClosed.Add(1)
		a.levels[level].dropped.Add(1)
		return Closed
	}
	if lease.Len() > a.config.MaxEventBytes {
		a.rejectedOversized.Add(1)
		a.levels[level].dropped.Add(1)
		return Oversized
	}
	if err := lease.inner.SetAuditContext(pool.AuditContext{Level: level.String(), Logger: lease.logger}); err != nil {
		a.rejectedInvalid.Add(1)
		a.levels[level].dropped.Add(1)
		return InvalidLease
	}
	if err := lease.inner.MarkQueued(); err != nil {
		a.rejectedInvalid.Add(1)
		a.levels[level].dropped.Add(1)
		return InvalidLease
	}

	result := a.queue.TryPublish(lease.inner)
	switch result {
	case ring.PublishAccepted:
		lease.transferred.Store(true)
		a.accepted.Add(1)
		a.levels[level].accepted.Add(1)
		return Accepted
	case ring.PublishFull:
		_ = lease.inner.RollbackQueued()
		a.rejectedFull.Add(1)
		a.levels[level].dropped.Add(1)
		return QueueFull
	case ring.PublishClosed:
		_ = lease.inner.RollbackQueued()
		a.rejectedClosed.Add(1)
		a.levels[level].dropped.Add(1)
		return Closed
	default:
		_ = lease.inner.RollbackQueued()
		a.rejectedInvalid.Add(1)
		a.levels[level].dropped.Add(1)
		return InvalidLease
	}
}

func (a *Appender) Named(name string) *Logger {
	if name == "" {
		name = a.config.Name
	}
	return &Logger{appender: a, name: name}
}

func (a *Appender) SnapshotLeases(state string, limit int) []LeaseSnapshot {
	return a.bytePool.SnapshotLeases(state, limit)
}

func (a *Appender) SnapshotLeasesBefore(state string, beforeID uint64, limit int) []LeaseSnapshot {
	return a.bytePool.SnapshotLeasesBefore(state, beforeID, limit)
}

func (a *Appender) LeaseByID(id uint64) (LeaseSnapshot, bool) {
	return a.bytePool.LeaseByID(id)
}

func (a *Appender) Stats() Stats {
	flusherStats := a.flusher.Stats()
	state := "OPEN"
	if a.closed.Load() {
		state = "CLOSING"
	}
	if flusherStats.Closed {
		state = "CLOSED"
	}
	byLevel := make(map[string]LevelStats, len(a.levels))
	for level := DebugLevel; level <= ErrorLevel; level++ {
		counter := &a.levels[level]
		byLevel[level.String()] = LevelStats{
			Attempts: counter.attempts.Load(),
			Accepted: counter.accepted.Load(),
			Dropped:  counter.dropped.Load(),
		}
	}
	return Stats{
		State:             state,
		PublishAttempts:   a.publishAttempts.Load(),
		Accepted:          a.accepted.Load(),
		RejectedFull:      a.rejectedFull.Load(),
		RejectedClosed:    a.rejectedClosed.Load(),
		RejectedOversized: a.rejectedOversized.Load(),
		RejectedInvalid:   a.rejectedInvalid.Load(),
		Filtered:          a.filtered.Load(),
		ByLevel:           byLevel,
		Ring:              a.queue.Stats(),
		Pool:              a.bytePool.Stats(),
		Flusher:           flusherStats,
		Closed:            flusherStats.Closed,
	}
}

func (a *Appender) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.closeOnce.Do(func() {
		a.closed.Store(true)
		go func(closeContext context.Context) {
			err := a.flusher.Close(closeContext)
			a.bytePool.Close()
			a.closeMu.Lock()
			a.closeErr = err
			a.closeMu.Unlock()
			close(a.closeDone)
		}(ctx)
	})
	select {
	case <-a.closeDone:
		a.closeMu.RLock()
		err := a.closeErr
		a.closeMu.RUnlock()
		return err
	case <-ctx.Done():
		select {
		case <-a.closeDone:
			a.closeMu.RLock()
			err := a.closeErr
			a.closeMu.RUnlock()
			return err
		default:
			return &CloseTimeoutError{Queued: a.queue.Depth(), InFlight: a.flusher.Stats().InFlight, Cause: ctx.Err()}
		}
	}
}

type eventRecord struct {
	Sequence          uint64        `json:"sequence"`
	Timestamp         string        `json:"timestamp"`
	TimestampUnixNano int64         `json:"timestamp_unix_nano"`
	Level             string        `json:"level"`
	Logger            string        `json:"logger"`
	Message           string        `json:"message"`
	Caller            *callerRecord `json:"caller,omitempty"`
	Fields            []Field       `json:"fields"`
}

type callerRecord struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

func (a *Appender) log(name string, level Level, message string, fields []Field) PublishResult {
	if !level.valid() {
		a.publishAttempts.Add(1)
		a.rejectedInvalid.Add(1)
		return InvalidLease
	}
	if level < a.config.MinLevel {
		a.filtered.Add(1)
		return Filtered
	}
	if a.closed.Load() {
		a.publishAttempts.Add(1)
		a.levels[level].attempts.Add(1)
		a.rejectedClosed.Add(1)
		a.levels[level].dropped.Add(1)
		return Closed
	}
	now := time.Now()
	event := eventRecord{
		Sequence:          a.sequence.Add(1),
		Timestamp:         now.UTC().Format(time.RFC3339Nano),
		TimestampUnixNano: now.UnixNano(),
		Level:             level.String(),
		Logger:            name,
		Message:           message,
		Caller:            a.caller(3),
		Fields:            fields,
	}
	if event.Fields == nil {
		event.Fields = []Field{}
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		a.publishAttempts.Add(1)
		a.levels[level].attempts.Add(1)
		a.rejectedInvalid.Add(1)
		a.levels[level].dropped.Add(1)
		return InvalidLease
	}
	encoded = append(encoded, '\n')
	if len(encoded) > a.config.MaxEventBytes {
		a.publishAttempts.Add(1)
		a.levels[level].attempts.Add(1)
		a.rejectedOversized.Add(1)
		a.levels[level].dropped.Add(1)
		return Oversized
	}
	inner, err := a.bytePool.AcquireContext(len(encoded), pool.AuditContext{Level: level.String(), Logger: name, CallerSkip: 2})
	if err != nil {
		a.publishAttempts.Add(1)
		a.levels[level].attempts.Add(1)
		if errors.Is(err, pool.ErrTooLarge) {
			a.rejectedOversized.Add(1)
			a.levels[level].dropped.Add(1)
			return Oversized
		}
		if errors.Is(err, pool.ErrClosed) {
			a.rejectedClosed.Add(1)
			a.levels[level].dropped.Add(1)
			return Closed
		}
		a.rejectedInvalid.Add(1)
		a.levels[level].dropped.Add(1)
		return InvalidLease
	}
	if err := inner.SetBytes(encoded); err != nil {
		_ = a.bytePool.Release(inner)
		a.publishAttempts.Add(1)
		a.levels[level].attempts.Add(1)
		a.rejectedInvalid.Add(1)
		a.levels[level].dropped.Add(1)
		return InvalidLease
	}
	lease := &Lease{appender: a, inner: inner, id: inner.ID(), logger: name}
	result := a.TryPublishLease(level, lease)
	if result != Accepted {
		_ = lease.Release()
	}
	return result
}

func (a *Appender) caller(skip int) *callerRecord {
	programCounter, file, line, ok := runtime.Caller(skip)
	if !ok {
		return nil
	}
	function := ""
	if fn := runtime.FuncForPC(programCounter); fn != nil {
		function = fn.Name()
	}
	if a.config.ProjectRoot != "" {
		if relative, err := filepath.Rel(a.config.ProjectRoot, file); err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			file = filepath.ToSlash(relative)
		} else {
			file = filepath.Base(file)
		}
	} else {
		file = filepath.Base(file)
	}
	return &callerRecord{Function: function, File: file, Line: line}
}
