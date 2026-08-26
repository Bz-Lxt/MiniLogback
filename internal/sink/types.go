package sink

import (
	"context"
	"sync/atomic"
)

// BatchSink is the completion contract used by the flusher. A nil error means
// every record has reached this sink's completion boundary. After an error the
// flusher retries the same logical batch and backing views; implementations
// that can partially commit must retain their byte offset and resume without
// replaying the committed prefix. FileSink and WriterSink implement this rule.
type BatchSink interface {
	WriteBatch(context.Context, [][]byte) error
	Flush(context.Context) error
	Close() error
}

type Capabilities struct {
	BatchMode    string `json:"batch_mode"`
	CopyAvoiding bool   `json:"copy_avoiding"`
	DurableFlush bool   `json:"durable_flush"`
	Acknowledged bool   `json:"acknowledged"`
}

type Stats struct {
	Batches        uint64 `json:"batches"`
	Records        uint64 `json:"records"`
	Bytes          uint64 `json:"bytes"`
	Errors         uint64 `json:"errors"`
	ShortWrites    uint64 `json:"short_writes"`
	Flushes        uint64 `json:"flushes"`
	Rotations      uint64 `json:"rotations"`
	LastWriteNanos uint64 `json:"last_write_nanos"`
	Closed         bool   `json:"closed"`
}

type counters struct {
	batches        atomic.Uint64
	records        atomic.Uint64
	bytes          atomic.Uint64
	errors         atomic.Uint64
	shortWrites    atomic.Uint64
	flushes        atomic.Uint64
	rotations      atomic.Uint64
	lastWriteNanos atomic.Uint64
	closed         atomic.Bool
}

func (c *counters) snapshot() Stats {
	return Stats{
		Batches:        c.batches.Load(),
		Records:        c.records.Load(),
		Bytes:          c.bytes.Load(),
		Errors:         c.errors.Load(),
		ShortWrites:    c.shortWrites.Load(),
		Flushes:        c.flushes.Load(),
		Rotations:      c.rotations.Load(),
		LastWriteNanos: c.lastWriteNanos.Load(),
		Closed:         c.closed.Load(),
	}
}
