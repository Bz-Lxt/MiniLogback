package minilogback

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xavskye/minilogback/internal/flusher"
	"github.com/xavskye/minilogback/internal/pool"
	"github.com/xavskye/minilogback/internal/ring"
)

const (
	defaultRingCapacity = uint64(1 << 16)
	defaultMaxEvent     = 1 << 20
)

var (
	ErrLeaseTransferred = errors.New("lease ownership was transferred to the appender")
	ErrInvalidLease     = errors.New("invalid or foreign lease")
	ErrClosed           = errors.New("appender is closed")
)

type Level uint8

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func (l Level) valid() bool { return l <= ErrorLevel }

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

type PublishResult string

const (
	Accepted     PublishResult = "accepted"
	QueueFull    PublishResult = "queue_full"
	Closed       PublishResult = "closed"
	Oversized    PublishResult = "oversized"
	InvalidLease PublishResult = "invalid_lease"
	Filtered     PublishResult = "filtered"
)

type AuditMode string

const (
	AuditOff     AuditMode = "OFF"
	AuditSampled AuditMode = "SAMPLED"
	AuditFull    AuditMode = "FULL"
)

// Sink receives one logical batch at a time. If WriteBatch partially commits
// data and returns an error, it must resume the exact batch on the next call
// without replaying the committed prefix; the Appender retains every lease
// until a retry returns nil.
type Sink interface {
	WriteBatch(context.Context, [][]byte) error
	Flush(context.Context) error
	Close() error
}

type Config struct {
	Name              string
	MinLevel          Level
	RingCapacity      uint64
	BatchSize         int
	FlushInterval     time.Duration
	PollInterval      time.Duration
	RetryBackoff      time.Duration
	MaxRetryBackoff   time.Duration
	MaxEventBytes     int
	PoolClasses       []int
	AuditMode         AuditMode
	AuditSampleEvery  uint64
	LeaseTimeout      time.Duration
	AuditScanInterval time.Duration
	AuditHistoryLimit int
	AuditStackDepth   int
	ProjectRoot       string
	Sink              Sink
}

func (c Config) normalized() (Config, error) {
	if c.Name == "" {
		c.Name = "root"
	}
	if !c.MinLevel.valid() {
		return Config{}, fmt.Errorf("invalid minimum log level %d", c.MinLevel)
	}
	if c.RingCapacity == 0 {
		c.RingCapacity = defaultRingCapacity
	}
	if c.MaxEventBytes == 0 {
		c.MaxEventBytes = defaultMaxEvent
	}
	if c.MaxEventBytes < 1 {
		return Config{}, errors.New("max event bytes must be positive")
	}
	if len(c.PoolClasses) == 0 {
		for _, size := range pool.DefaultClasses {
			if size <= c.MaxEventBytes {
				c.PoolClasses = append(c.PoolClasses, size)
			}
		}
		if len(c.PoolClasses) == 0 {
			c.PoolClasses = []int{c.MaxEventBytes}
		}
	}
	if c.AuditMode == "" {
		c.AuditMode = AuditOff
	}
	if c.AuditMode != AuditOff && c.AuditMode != AuditSampled && c.AuditMode != AuditFull {
		return Config{}, fmt.Errorf("invalid audit mode %q", c.AuditMode)
	}
	return c, nil
}

type Option func(*Config) error

func WithSink(batchSink Sink) Option {
	return func(config *Config) error {
		if batchSink == nil {
			return errors.New("sink must not be nil")
		}
		config.Sink = batchSink
		return nil
	}
}

func WithName(name string) Option {
	return func(config *Config) error {
		if name == "" {
			return errors.New("logger name must not be empty")
		}
		config.Name = name
		return nil
	}
}

type Field struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func String(key, value string) Field        { return Field{Key: key, Value: value} }
func Int(key string, value int) Field       { return Field{Key: key, Value: value} }
func Int64(key string, value int64) Field   { return Field{Key: key, Value: value} }
func Uint64(key string, value uint64) Field { return Field{Key: key, Value: value} }
func Bool(key string, value bool) Field     { return Field{Key: key, Value: value} }
func Any(key string, value any) Field       { return Field{Key: key, Value: value} }
func Err(err error) Field {
	if err == nil {
		return Field{Key: "error", Value: nil}
	}
	return Field{Key: "error", Value: err.Error()}
}

type LevelStats struct {
	Attempts uint64 `json:"attempts"`
	Accepted uint64 `json:"accepted"`
	Dropped  uint64 `json:"dropped"`
}

type RingStats = ring.Stats
type PoolStats = pool.Stats
type FlusherStats = flusher.Stats
type LeaseSnapshot = pool.LeaseSnapshot
type CloseTimeoutError = flusher.CloseTimeoutError

type Stats struct {
	State             string                `json:"state"`
	PublishAttempts   uint64                `json:"publish_attempts"`
	Accepted          uint64                `json:"accepted"`
	RejectedFull      uint64                `json:"rejected_full"`
	RejectedClosed    uint64                `json:"rejected_closed"`
	RejectedOversized uint64                `json:"rejected_oversized"`
	RejectedInvalid   uint64                `json:"rejected_invalid"`
	Filtered          uint64                `json:"filtered"`
	ByLevel           map[string]LevelStats `json:"by_level"`
	Ring              RingStats             `json:"ring"`
	Pool              PoolStats             `json:"pool"`
	Flusher           FlusherStats          `json:"flusher"`
	Closed            bool                  `json:"closed"`
}
