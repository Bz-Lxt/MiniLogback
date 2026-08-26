package pool

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultMaxBytes     = 1 << 20
	defaultLeaseTimeout = 2 * time.Second
	defaultScanInterval = 100 * time.Millisecond
	defaultHistoryLimit = 1000
	defaultStackDepth   = 24
	maxStackDepth       = 128
)

var DefaultClasses = []int{256, 1024, 4096, 16 * 1024, 64 * 1024}

var (
	ErrClosed          = errors.New("byte pool is closed")
	ErrTooLarge        = errors.New("requested byte lease exceeds maximum")
	ErrInvalidSize     = errors.New("byte lease size must not be negative")
	ErrInvalidState    = errors.New("invalid lease state transition")
	ErrDoubleReturn    = errors.New("lease was already returned")
	ErrUnknownLease    = errors.New("lease does not belong to this pool")
	ErrCapacity        = errors.New("lease capacity exceeded")
	ErrPayloadModified = errors.New("lease payload changed after publication")
)

type AuditMode string

const (
	AuditOff     AuditMode = "OFF"
	AuditSampled AuditMode = "SAMPLED"
	AuditFull    AuditMode = "FULL"
)

func (m AuditMode) valid() bool {
	return m == AuditOff || m == AuditSampled || m == AuditFull
}

type Config struct {
	Classes      []int
	MaxBytes     int
	AuditMode    AuditMode
	SampleEvery  uint64
	LeaseTimeout time.Duration
	ScanInterval time.Duration
	HistoryLimit int
	StackDepth   int
	ProjectRoot  string
}

func (c Config) normalized() (Config, error) {
	if len(c.Classes) == 0 {
		c.Classes = append([]int(nil), DefaultClasses...)
	} else {
		c.Classes = append([]int(nil), c.Classes...)
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = defaultMaxBytes
	}
	if c.AuditMode == "" {
		c.AuditMode = AuditOff
	}
	if !c.AuditMode.valid() {
		return Config{}, fmt.Errorf("invalid audit mode %q", c.AuditMode)
	}
	if c.SampleEvery == 0 {
		c.SampleEvery = 100
	}
	if c.LeaseTimeout == 0 {
		c.LeaseTimeout = defaultLeaseTimeout
	}
	if c.ScanInterval == 0 {
		c.ScanInterval = defaultScanInterval
	}
	if c.HistoryLimit == 0 {
		c.HistoryLimit = defaultHistoryLimit
	}
	if c.StackDepth == 0 {
		c.StackDepth = defaultStackDepth
	}
	if c.MaxBytes <= 0 || c.LeaseTimeout <= 0 || c.ScanInterval <= 0 || c.HistoryLimit < 0 || c.StackDepth < 1 || c.StackDepth > maxStackDepth {
		return Config{}, errors.New("invalid byte pool limits")
	}
	previous := 0
	for _, size := range c.Classes {
		if size <= previous || size > c.MaxBytes {
			return Config{}, errors.New("pool classes must be positive, strictly increasing, and no greater than max bytes")
		}
		previous = size
	}
	return c, nil
}

type LeaseState uint32

const (
	StateBorrowed LeaseState = iota + 1
	StateQueued
	StateInFlight
	StateReturned
)

func (s LeaseState) String() string {
	switch s {
	case StateBorrowed:
		return "BORROWED"
	case StateQueued:
		return "QUEUED"
	case StateInFlight:
		return "IN_FLIGHT"
	case StateReturned:
		return "RETURNED"
	default:
		return "UNKNOWN"
	}
}

func ParseLeaseState(value string) string { return strings.ToUpper(strings.TrimSpace(value)) }

type AuditContext struct {
	Level      string
	Logger     string
	CallerSkip int
}

type Frame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type LeaseSnapshot struct {
	ID             uint64    `json:"id"`
	State          string    `json:"state"`
	ClassSize      int       `json:"class_size"`
	Capacity       int       `json:"capacity"`
	Length         int       `json:"length"`
	BorrowedAt     time.Time `json:"borrowed_at"`
	Deadline       time.Time `json:"deadline"`
	ReturnedAt     time.Time `json:"returned_at,omitempty"`
	AgeNanos       int64     `json:"age_nanos"`
	GoroutineID    uint64    `json:"goroutine_id,omitempty"`
	Level          string    `json:"level"`
	Logger         string    `json:"logger"`
	Function       string    `json:"function"`
	File           string    `json:"file"`
	Line           int       `json:"line"`
	Stack          []Frame   `json:"stack"`
	PayloadMutated bool      `json:"payload_mutated"`
}

type ClassStats struct {
	Size     int    `json:"size"`
	Borrowed uint64 `json:"borrowed"`
	InUse    uint64 `json:"in_use"`
	Hits     uint64 `json:"hits"`
	Misses   uint64 `json:"misses"`
}

type Stats struct {
	Borrowed         uint64       `json:"borrowed"`
	Returned         uint64       `json:"returned"`
	Outstanding      uint64       `json:"outstanding"`
	PoolHits         uint64       `json:"pool_hits"`
	PoolMisses       uint64       `json:"pool_misses"`
	Oversized        uint64       `json:"oversized"`
	LargeAllocations uint64       `json:"large_allocations"`
	DoubleReturns    uint64       `json:"double_returns"`
	InvalidReturns   uint64       `json:"invalid_returns"`
	StateErrors      uint64       `json:"state_errors"`
	UseAfterPublish  uint64       `json:"use_after_publish"`
	Overdue          uint64       `json:"overdue"`
	Classes          []ClassStats `json:"classes"`
	Closed           bool         `json:"closed"`
}
