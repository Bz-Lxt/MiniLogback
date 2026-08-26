package pool

import (
	"hash/crc32"
	"sync/atomic"
	"time"
)

type byteBlock struct {
	data []byte
}

// Lease is a uniquely identified ownership token for one byte block. A Lease
// object is never reused, so a stale pointer cannot affect a later borrower.
type Lease struct {
	id       uint64
	owner    *BytePool
	block    *byteBlock
	class    int
	length   atomic.Int64
	state    atomic.Uint32
	tracked  bool
	checksum uint32
	mutated  atomic.Bool
	borrowed time.Time
	deadline time.Time
}

func (l *Lease) ID() uint64 { return l.id }

func (l *Lease) State() LeaseState { return LeaseState(l.state.Load()) }

func (l *Lease) Capacity() int {
	if l == nil || l.block == nil {
		return 0
	}
	return cap(l.block.data)
}

func (l *Lease) ClassSize() int {
	if l == nil || l.owner == nil || l.class < 0 {
		return 0
	}
	return l.owner.classes[l.class].size
}

func (l *Lease) Len() int {
	if l == nil || l.State() == StateReturned {
		return 0
	}
	return int(l.length.Load())
}

// Payload returns the committed view without copying. Callers must stop using
// every previously obtained view after a successful publication.
func (l *Lease) Payload() []byte {
	if l == nil || l.block == nil || l.State() == StateReturned {
		return nil
	}
	length := int(l.length.Load())
	return l.block.data[:length:length]
}

// Writable returns the complete backing block only while the lease is owned by
// its borrower. Resize commits the number of meaningful bytes.
func (l *Lease) Writable() ([]byte, error) {
	if l == nil || l.State() != StateBorrowed {
		return nil, ErrInvalidState
	}
	return l.block.data[:cap(l.block.data)], nil
}

func (l *Lease) Resize(size int) error {
	if l == nil || l.State() != StateBorrowed {
		return ErrInvalidState
	}
	if size < 0 || size > cap(l.block.data) {
		return ErrCapacity
	}
	l.length.Store(int64(size))
	return nil
}

func (l *Lease) Reset() error { return l.Resize(0) }

func (l *Lease) Write(data []byte) (int, error) {
	if err := l.Append(data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (l *Lease) Append(data []byte) error {
	if l == nil || l.State() != StateBorrowed {
		return ErrInvalidState
	}
	length := int(l.length.Load())
	if len(data) > cap(l.block.data)-length {
		return ErrCapacity
	}
	copy(l.block.data[length:], data)
	l.length.Store(int64(length + len(data)))
	return nil
}

func (l *Lease) SetBytes(data []byte) error {
	if l == nil || l.State() != StateBorrowed {
		return ErrInvalidState
	}
	if len(data) > cap(l.block.data) {
		return ErrCapacity
	}
	copy(l.block.data, data)
	l.length.Store(int64(len(data)))
	return nil
}

func (l *Lease) SetAuditContext(context AuditContext) error {
	if l == nil || l.State() != StateBorrowed {
		return ErrInvalidState
	}
	if l.tracked {
		l.owner.audit.updateContext(l.id, context)
	}
	return nil
}

func (l *Lease) MarkQueued() error {
	if l == nil || !l.state.CompareAndSwap(uint32(StateBorrowed), uint32(StateQueued)) {
		if l != nil && l.owner != nil {
			l.owner.stateErrors.Add(1)
		}
		return ErrInvalidState
	}
	if l.tracked {
		l.checksum = crc32.ChecksumIEEE(l.Payload())
	}
	return nil
}

// RollbackQueued restores caller ownership after queue admission failed.
func (l *Lease) RollbackQueued() error {
	if l == nil || !l.state.CompareAndSwap(uint32(StateQueued), uint32(StateBorrowed)) {
		if l != nil && l.owner != nil {
			l.owner.stateErrors.Add(1)
		}
		return ErrInvalidState
	}
	return nil
}

func (l *Lease) MarkInFlight() error {
	if l == nil || !l.state.CompareAndSwap(uint32(StateQueued), uint32(StateInFlight)) {
		if l != nil && l.owner != nil {
			l.owner.stateErrors.Add(1)
		}
		return ErrInvalidState
	}
	if l.detectMutation() {
		return ErrPayloadModified
	}
	return nil
}

// detectMutation is sampled at both ownership-transfer boundaries: before the
// sink sees the payload and immediately before an in-flight lease is returned.
// The atomic latch keeps a single misuse from inflating diagnostics.
func (l *Lease) detectMutation() bool {
	if l == nil || !l.tracked || crc32.ChecksumIEEE(l.Payload()) == l.checksum {
		return false
	}
	if l.mutated.CompareAndSwap(false, true) {
		l.owner.useAfterPublish.Add(1)
		l.owner.audit.markMutated(l.id)
	}
	return true
}
