package minilogback

import (
	"sync/atomic"

	"github.com/xavskye/minilogback/internal/pool"
)

// Lease exposes borrower operations without leaking internal pool types.
type Lease struct {
	appender    *Appender
	inner       *pool.Lease
	id          uint64
	logger      string
	transferred atomic.Bool
	released    atomic.Bool
}

func (l *Lease) ID() uint64 {
	if l == nil {
		return 0
	}
	return l.id
}

func (l *Lease) Len() int {
	if l == nil || l.inner == nil {
		return 0
	}
	return l.inner.Len()
}

func (l *Lease) Capacity() int {
	if l == nil || l.inner == nil {
		return 0
	}
	return l.inner.Capacity()
}

func (l *Lease) ClassSize() int {
	if l == nil || l.inner == nil {
		return 0
	}
	return l.inner.ClassSize()
}

func (l *Lease) Write(data []byte) (int, error) {
	if l == nil || l.transferred.Load() || l.released.Load() {
		return 0, ErrInvalidLease
	}
	return l.inner.Write(data)
}

func (l *Lease) SetBytes(data []byte) error {
	if l == nil || l.transferred.Load() || l.released.Load() {
		return ErrInvalidLease
	}
	return l.inner.SetBytes(data)
}

func (l *Lease) Buffer() ([]byte, error) {
	if l == nil || l.transferred.Load() || l.released.Load() {
		return nil, ErrInvalidLease
	}
	return l.inner.Writable()
}

func (l *Lease) Commit(length int) error {
	if l == nil || l.transferred.Load() || l.released.Load() {
		return ErrInvalidLease
	}
	return l.inner.Resize(length)
}

// SetAuditContext enriches a borrowed lease before it is published or retained
// for diagnostics. Passing an empty logger keeps the Appender's logger name.
func (l *Lease) SetAuditContext(level Level, logger string) error {
	if l == nil || l.inner == nil || l.transferred.Load() || l.released.Load() || !level.valid() {
		return ErrInvalidLease
	}
	if logger != "" {
		l.logger = logger
	}
	return l.inner.SetAuditContext(pool.AuditContext{Level: level.String(), Logger: l.logger})
}

func (l *Lease) Release() error {
	if l == nil || l.appender == nil || l.inner == nil || l.id != l.inner.ID() {
		return ErrInvalidLease
	}
	if l.transferred.Load() {
		return ErrLeaseTransferred
	}
	if !l.released.CompareAndSwap(false, true) {
		return pool.ErrDoubleReturn
	}
	return l.appender.bytePool.Release(l.inner)
}
