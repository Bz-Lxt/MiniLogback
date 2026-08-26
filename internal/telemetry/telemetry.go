package telemetry

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var ErrTooManySubscribers = errors.New("too many telemetry subscribers")

type RingMetrics struct {
	Capacity         uint64            `json:"capacity"`
	Depth            uint64            `json:"depth"`
	WatermarkPercent float64           `json:"watermark_percent"`
	HighWatermark    uint64            `json:"high_watermark"`
	PublishAttempts  uint64            `json:"publish_attempts"`
	Accepted         uint64            `json:"accepted"`
	Consumed         uint64            `json:"consumed"`
	DroppedTotal     uint64            `json:"dropped_total"`
	DroppedByLevel   map[string]uint64 `json:"dropped_by_level"`
	PublishRate      float64           `json:"publish_rate"`
	ConsumeRate      float64           `json:"consume_rate"`
}

type FlusherMetrics struct {
	Batches        uint64  `json:"batches"`
	Records        uint64  `json:"records"`
	Bytes          uint64  `json:"bytes"`
	InFlight       uint64  `json:"in_flight"`
	LastBatchSize  int     `json:"last_batch_size"`
	FlushP95Micros float64 `json:"flush_p95_micros"`
	Errors         uint64  `json:"errors"`
	Mode           string  `json:"mode"`
}

type PoolClass struct {
	Size      int    `json:"size"`
	InUse     uint64 `json:"in_use"`
	Available uint64 `json:"available"`
}

type PoolMetrics struct {
	BorrowedTotal  uint64      `json:"borrowed_total"`
	ReturnedTotal  uint64      `json:"returned_total"`
	Outstanding    uint64      `json:"outstanding"`
	Overdue        uint64      `json:"overdue"`
	DoubleReturns  uint64      `json:"double_returns"`
	InvalidReturns uint64      `json:"invalid_returns"`
	HitPercent     float64     `json:"hit_percent"`
	Classes        []PoolClass `json:"classes"`
}

type CollectorMetrics struct {
	Connections      uint64 `json:"connections"`
	AcceptedBatches  uint64 `json:"accepted_batches"`
	DuplicateBatches uint64 `json:"duplicate_batches"`
	InvalidFrames    uint64 `json:"invalid_frames"`
	Overloaded       uint64 `json:"overloaded"`
	SinkErrors       uint64 `json:"sink_errors"`
}

type RuntimeMetrics struct {
	Goroutines int    `json:"goroutines"`
	HeapBytes  uint64 `json:"heap_bytes"`
	DemoMode   bool   `json:"demo_mode"`
	AuditMode  string `json:"audit_mode"`
}

type StatusMetrics struct {
	Sink      string `json:"sink"`
	Collector string `json:"collector"`
	Transport string `json:"transport"`
}

type Snapshot struct {
	Sequence  uint64           `json:"sequence"`
	SampledAt time.Time        `json:"sampled_at"`
	Ring      RingMetrics      `json:"ring"`
	Flusher   FlusherMetrics   `json:"flusher"`
	Pool      PoolMetrics      `json:"pool"`
	Collector CollectorMetrics `json:"collector"`
	Runtime   RuntimeMetrics   `json:"runtime"`
	Status    StatusMetrics    `json:"status"`
}

type RawSnapshot struct {
	Ring      RingMetrics
	Flusher   FlusherMetrics
	Pool      PoolMetrics
	Collector CollectorMetrics
	DemoMode  bool
	AuditMode string
	Status    StatusMetrics
}

type Source interface {
	TelemetrySnapshot() RawSnapshot
}

type SourceFunc func() RawSnapshot

func (f SourceFunc) TelemetrySnapshot() RawSnapshot { return f() }

type Sampler struct {
	source      Source
	interval    time.Duration
	maxClients  int
	sequence    atomic.Uint64
	current     atomic.Pointer[Snapshot]
	mu          sync.Mutex
	subscribers map[uint64]chan Snapshot
	nextSubID   uint64
}

func NewSampler(source Source, interval time.Duration, maxClients int) (*Sampler, error) {
	if source == nil || interval <= 0 || maxClients <= 0 {
		return nil, errors.New("telemetry source, positive interval, and max clients are required")
	}
	s := &Sampler{source: source, interval: interval, maxClients: maxClients, subscribers: make(map[uint64]chan Snapshot)}
	s.sample(time.Now().UTC())
	return s, nil
}

func (s *Sampler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			s.sample(now.UTC())
		}
	}
}

func (s *Sampler) SampleNow() Snapshot { return s.sample(time.Now().UTC()) }

func (s *Sampler) Current() Snapshot {
	current := s.current.Load()
	if current == nil {
		return Snapshot{}
	}
	return cloneSnapshot(*current)
}

func (s *Sampler) Subscribe(ctx context.Context) (<-chan Snapshot, error) {
	s.mu.Lock()
	if len(s.subscribers) >= s.maxClients {
		s.mu.Unlock()
		return nil, ErrTooManySubscribers
	}
	s.nextSubID++
	id := s.nextSubID
	updates := make(chan Snapshot, 1)
	s.subscribers[id] = updates
	s.mu.Unlock()

	updates <- s.Current()
	go func() {
		<-ctx.Done()
		close(updates)
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
	}()
	return updates, nil
}

func (s *Sampler) sample(now time.Time) Snapshot {
	raw := s.source.TelemetrySnapshot()
	previous := s.current.Load()
	if raw.Ring.Capacity > 0 {
		raw.Ring.WatermarkPercent = float64(raw.Ring.Depth) * 100 / float64(raw.Ring.Capacity)
	}
	if previous != nil {
		seconds := now.Sub(previous.SampledAt).Seconds()
		if seconds > 0 {
			raw.Ring.PublishRate = counterRate(raw.Ring.Accepted, previous.Ring.Accepted, seconds)
			raw.Ring.ConsumeRate = counterRate(raw.Ring.Consumed, previous.Ring.Consumed, seconds)
		}
	}
	raw.Ring.DroppedByLevel = normalizeLevels(raw.Ring.DroppedByLevel)
	if raw.Pool.Classes == nil {
		raw.Pool.Classes = []PoolClass{}
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	snapshot := Snapshot{
		Sequence: s.sequence.Add(1), SampledAt: now, Ring: raw.Ring, Flusher: raw.Flusher,
		Pool: raw.Pool, Collector: raw.Collector,
		Runtime: RuntimeMetrics{Goroutines: runtime.NumGoroutine(), HeapBytes: memory.HeapAlloc, DemoMode: raw.DemoMode, AuditMode: raw.AuditMode},
		Status:  raw.Status,
	}
	stored := cloneSnapshot(snapshot)
	s.current.Store(&stored)
	s.broadcast(snapshot)
	return cloneSnapshot(snapshot)
}

func (s *Sampler) broadcast(snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, subscriber := range s.subscribers {
		value := cloneSnapshot(snapshot)
		select {
		case subscriber <- value:
		default:
			select {
			case <-subscriber:
			default:
			}
			select {
			case subscriber <- value:
			default:
			}
		}
	}
}

func counterRate(current, previous uint64, seconds float64) float64 {
	if current < previous {
		return 0
	}
	return float64(current-previous) / seconds
}

func normalizeLevels(input map[string]uint64) map[string]uint64 {
	output := map[string]uint64{"debug": 0, "info": 0, "warn": 0, "error": 0}
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneSnapshot(input Snapshot) Snapshot {
	input.Ring.DroppedByLevel = normalizeLevels(input.Ring.DroppedByLevel)
	input.Pool.Classes = append([]PoolClass{}, input.Pool.Classes...)
	return input
}
