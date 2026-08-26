package flusher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavskye/minilogback/internal/pool"
	"github.com/xavskye/minilogback/internal/ring"
	"github.com/xavskye/minilogback/internal/sink"
)

type Flusher struct {
	queue  *ring.Queue[pool.Lease]
	pool   *pool.BytePool
	sink   sink.BatchSink
	config Config

	startOnce sync.Once
	stopOnce  sync.Once
	abortOnce sync.Once
	stop      chan struct{}
	abort     chan struct{}
	done      chan struct{}
	workCtx   context.Context
	cancel    context.CancelFunc

	batches         atomic.Uint64
	records         atomic.Uint64
	bytes           atomic.Uint64
	errors          atomic.Uint64
	retries         atomic.Uint64
	inFlight        atomic.Uint64
	lastBatchSize   atomic.Uint64
	lastFlushNanos  atomic.Uint64
	totalFlushNanos atomic.Uint64
	running         atomic.Bool
	closing         atomic.Bool
	closed          atomic.Bool
	degraded        atomic.Bool

	errMu    sync.RWMutex
	lastErr  error
	finalErr error
}

func New(queue *ring.Queue[pool.Lease], bytePool *pool.BytePool, batchSink sink.BatchSink, config Config) (*Flusher, error) {
	if queue == nil || bytePool == nil || batchSink == nil {
		return nil, errors.New("flusher requires queue, pool, and sink")
	}
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	workCtx, cancel := context.WithCancel(context.Background())
	return &Flusher{
		queue:   queue,
		pool:    bytePool,
		sink:    batchSink,
		config:  normalized,
		stop:    make(chan struct{}),
		abort:   make(chan struct{}),
		done:    make(chan struct{}),
		workCtx: workCtx,
		cancel:  cancel,
	}, nil
}

func (f *Flusher) Start() {
	f.startOnce.Do(func() {
		f.running.Store(true)
		go f.run()
	})
}

func (f *Flusher) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	f.Start()
	f.queue.Close()
	f.closing.Store(true)
	f.stopOnce.Do(func() { close(f.stop) })
	select {
	case <-f.done:
		f.errMu.RLock()
		err := f.finalErr
		f.errMu.RUnlock()
		return err
	case <-ctx.Done():
		stats := f.Stats()
		f.abortOnce.Do(func() {
			close(f.abort)
			f.cancel()
		})
		return &CloseTimeoutError{Queued: f.queue.Depth(), InFlight: stats.InFlight, Cause: closeCause(ctx)}
	}
}

func (f *Flusher) Stats() Stats {
	f.errMu.RLock()
	lastError := ""
	if f.lastErr != nil {
		lastError = f.lastErr.Error()
	}
	f.errMu.RUnlock()
	return Stats{
		Batches:         f.batches.Load(),
		Records:         f.records.Load(),
		Bytes:           f.bytes.Load(),
		Errors:          f.errors.Load(),
		Retries:         f.retries.Load(),
		InFlight:        f.inFlight.Load(),
		LastBatchSize:   f.lastBatchSize.Load(),
		LastFlushNanos:  f.lastFlushNanos.Load(),
		TotalFlushNanos: f.totalFlushNanos.Load(),
		Running:         f.running.Load(),
		Closing:         f.closing.Load(),
		Closed:          f.closed.Load(),
		Degraded:        f.degraded.Load(),
		LastError:       lastError,
	}
}

func (f *Flusher) run() {
	defer close(f.done)
	defer f.cancel()
	defer f.running.Store(false)

	pending := make([]*pool.Lease, 0, f.config.BatchSize)
	payloads := make([][]byte, 0, f.config.BatchSize)
	var deadline time.Time
	var retryAt time.Time
	retryBackoff := f.config.RetryBackoff
	retrying := false
	closing := false
	aborting := false
	stopChannel := f.stop
	abortChannel := f.abort
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-abortChannel:
			aborting = true
			closing = true
			abortChannel = nil
		case <-stopChannel:
			closing = true
			stopChannel = nil
		default:
		}

		if aborting {
			f.abandon(pending)
			pending = pending[:0]
			for {
				lease, ok := f.queue.TryConsume()
				if !ok {
					break
				}
				_ = lease.MarkInFlight()
				_ = f.pool.Release(lease)
			}
			f.inFlight.Store(0)
			f.setFinalError(f.sink.Close())
			f.closed.Store(true)
			return
		}

		if !retrying {
			for len(pending) < f.config.BatchSize {
				lease, ok := f.queue.TryConsume()
				if !ok {
					break
				}
				if err := lease.MarkInFlight(); err != nil && !errors.Is(err, pool.ErrPayloadModified) {
					f.recordError(err)
					_ = f.pool.Release(lease)
					continue
				} else if err != nil {
					f.recordError(err)
				}
				if len(pending) == 0 {
					deadline = time.Now().Add(f.config.FlushInterval)
				}
				pending = append(pending, lease)
			}
			f.inFlight.Store(uint64(len(pending)))
		}

		now := time.Now()
		shouldFlush := len(pending) > 0 && ((retrying && !now.Before(retryAt)) || (!retrying && (len(pending) >= f.config.BatchSize || closing || !now.Before(deadline))))
		if shouldFlush {
			payloads = payloads[:0]
			for _, lease := range pending {
				payloads = append(payloads, lease.Payload())
			}
			started := time.Now()
			wasRetry := retrying
			if wasRetry {
				f.retries.Add(1)
			}
			err := f.sink.WriteBatch(f.workCtx, payloads)
			elapsed := time.Since(started)
			f.lastFlushNanos.Store(uint64(elapsed))
			f.totalFlushNanos.Add(uint64(elapsed))
			if err == nil {
				var bytes uint64
				for _, lease := range pending {
					bytes += uint64(lease.Len())
					if releaseErr := f.pool.Release(lease); releaseErr != nil {
						f.recordError(releaseErr)
					}
				}
				f.batches.Add(1)
				f.records.Add(uint64(len(pending)))
				f.bytes.Add(bytes)
				f.lastBatchSize.Store(uint64(len(pending)))
				pending = pending[:0]
				f.inFlight.Store(0)
				deadline = time.Time{}
				retryAt = time.Time{}
				retrying = false
				retryBackoff = f.config.RetryBackoff
				f.degraded.Store(false)
				f.clearLastError()
				continue
			}

			f.recordError(err)
			f.degraded.Store(true)
			retrying = true
			retryAt = time.Now().Add(retryBackoff)
			if retryBackoff < f.config.MaxRetryBackoff {
				retryBackoff *= 2
				if retryBackoff > f.config.MaxRetryBackoff {
					retryBackoff = f.config.MaxRetryBackoff
				}
			}
		}

		if closing && len(pending) == 0 && f.queue.Empty() {
			flushErr := f.sink.Flush(context.Background())
			closeErr := f.sink.Close()
			f.setFinalError(errors.Join(flushErr, closeErr))
			f.closed.Store(true)
			return
		}

		wait := f.config.PollInterval
		if retrying {
			wait = time.Until(retryAt)
		} else if len(pending) > 0 {
			untilDeadline := time.Until(deadline)
			if untilDeadline < wait {
				wait = untilDeadline
			}
		}
		if wait < 0 {
			wait = 0
		}
		timer.Reset(wait)
		select {
		case <-timer.C:
		case <-stopChannel:
			closing = true
			stopChannel = nil
			timer.Stop()
		case <-abortChannel:
			aborting = true
			closing = true
			abortChannel = nil
			timer.Stop()
		}
	}
}

func (f *Flusher) abandon(pending []*pool.Lease) {
	for _, lease := range pending {
		_ = f.pool.Release(lease)
	}
}

func (f *Flusher) recordError(err error) {
	if err == nil {
		return
	}
	f.errors.Add(1)
	f.errMu.Lock()
	f.lastErr = err
	f.errMu.Unlock()
}

func (f *Flusher) clearLastError() {
	f.errMu.Lock()
	f.lastErr = nil
	f.errMu.Unlock()
}

func (f *Flusher) setFinalError(err error) {
	if err == nil {
		return
	}
	f.recordError(err)
	f.errMu.Lock()
	if f.finalErr == nil {
		f.finalErr = err
	}
	f.errMu.Unlock()
}
