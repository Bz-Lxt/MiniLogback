package flusher

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultBatchSize       = 1024
	defaultFlushInterval   = 50 * time.Millisecond
	defaultPollInterval    = 100 * time.Microsecond
	defaultRetryBackoff    = 10 * time.Millisecond
	defaultMaxRetryBackoff = time.Second
)

type Config struct {
	BatchSize       int
	FlushInterval   time.Duration
	PollInterval    time.Duration
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = defaultFlushInterval
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = defaultRetryBackoff
	}
	if c.MaxRetryBackoff == 0 {
		c.MaxRetryBackoff = defaultMaxRetryBackoff
	}
	if c.BatchSize < 1 || c.FlushInterval <= 0 || c.PollInterval <= 0 || c.RetryBackoff <= 0 || c.MaxRetryBackoff < c.RetryBackoff {
		return Config{}, errors.New("invalid flusher batching or retry limits")
	}
	return c, nil
}

type Stats struct {
	Batches         uint64 `json:"batches"`
	Records         uint64 `json:"records"`
	Bytes           uint64 `json:"bytes"`
	Errors          uint64 `json:"errors"`
	Retries         uint64 `json:"retries"`
	InFlight        uint64 `json:"in_flight"`
	LastBatchSize   uint64 `json:"last_batch_size"`
	LastFlushNanos  uint64 `json:"last_flush_nanos"`
	TotalFlushNanos uint64 `json:"total_flush_nanos"`
	Running         bool   `json:"running"`
	Closing         bool   `json:"closing"`
	Closed          bool   `json:"closed"`
	Degraded        bool   `json:"degraded"`
	LastError       string `json:"last_error"`
}

type CloseTimeoutError struct {
	Queued   uint64
	InFlight uint64
	Cause    error
}

func (e *CloseTimeoutError) Error() string {
	return fmt.Sprintf("flusher close timed out: queued=%d in_flight=%d: %v", e.Queued, e.InFlight, e.Cause)
}

func (e *CloseTimeoutError) Unwrap() error { return e.Cause }

func closeCause(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return context.DeadlineExceeded
}
