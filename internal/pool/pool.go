package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type slab struct {
	size     int
	store    sync.Pool
	borrowed atomic.Uint64
	inUse    atomic.Uint64
	hits     atomic.Uint64
	misses   atomic.Uint64
}

type BytePool struct {
	config  Config
	classes []slab
	nextID  atomic.Uint64
	closed  atomic.Bool

	borrowed         atomic.Uint64
	returned         atomic.Uint64
	outstanding      atomic.Uint64
	poolHits         atomic.Uint64
	poolMisses       atomic.Uint64
	oversized        atomic.Uint64
	largeAllocations atomic.Uint64
	doubleReturns    atomic.Uint64
	invalidReturns   atomic.Uint64
	stateErrors      atomic.Uint64
	useAfterPublish  atomic.Uint64
	overdue          atomic.Uint64

	audit *auditRegistry
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
}

func New(config Config) (*BytePool, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	p := &BytePool{
		config:  normalized,
		classes: make([]slab, len(normalized.Classes)),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	for index, size := range normalized.Classes {
		p.classes[index].size = size
	}
	if normalized.AuditMode == AuditOff {
		close(p.done)
	} else {
		p.audit = newAuditRegistry(p, normalized)
		go p.scanLoop()
	}
	return p, nil
}

func (p *BytePool) Acquire(capacity int) (*Lease, error) {
	return p.acquire(capacity, AuditContext{})
}

func (p *BytePool) AcquireContext(capacity int, context AuditContext) (*Lease, error) {
	return p.acquire(capacity, context)
}

func (p *BytePool) acquire(capacity int, context AuditContext) (*Lease, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if capacity < 0 {
		return nil, ErrInvalidSize
	}
	if capacity > p.config.MaxBytes {
		p.oversized.Add(1)
		return nil, fmt.Errorf("%w: requested=%d max=%d", ErrTooLarge, capacity, p.config.MaxBytes)
	}

	classIndex := p.classFor(capacity)
	var block *byteBlock
	if classIndex >= 0 {
		class := &p.classes[classIndex]
		if cached := class.store.Get(); cached != nil {
			block = cached.(*byteBlock)
			class.hits.Add(1)
			p.poolHits.Add(1)
		} else {
			block = &byteBlock{data: make([]byte, class.size)}
			class.misses.Add(1)
			p.poolMisses.Add(1)
		}
		class.borrowed.Add(1)
		class.inUse.Add(1)
	} else {
		block = &byteBlock{data: make([]byte, capacity)}
		p.poolMisses.Add(1)
		p.largeAllocations.Add(1)
	}

	now := time.Now()
	id := p.nextID.Add(1)
	if id == 0 {
		id = p.nextID.Add(1)
	}
	lease := &Lease{
		id:       id,
		owner:    p,
		block:    block,
		class:    classIndex,
		borrowed: now,
		deadline: now.Add(p.config.LeaseTimeout),
	}
	lease.state.Store(uint32(StateBorrowed))
	borrowNumber := p.borrowed.Add(1)
	p.outstanding.Add(1)
	lease.tracked = p.shouldTrack(borrowNumber)
	if lease.tracked {
		p.audit.register(lease, context)
	}
	return lease, nil
}

func (p *BytePool) Release(lease *Lease) error {
	if lease == nil || lease.owner != p {
		p.invalidReturns.Add(1)
		return ErrUnknownLease
	}

	for {
		state := lease.State()
		if state == StateInFlight {
			lease.detectMutation()
		}
		switch state {
		case StateReturned:
			p.doubleReturns.Add(1)
			return ErrDoubleReturn
		case StateQueued:
			p.stateErrors.Add(1)
			return ErrInvalidState
		case StateBorrowed, StateInFlight:
			if !lease.state.CompareAndSwap(uint32(state), uint32(StateReturned)) {
				continue
			}
		default:
			p.invalidReturns.Add(1)
			return ErrUnknownLease
		}
		break
	}

	if lease.tracked {
		p.audit.finalize(lease, time.Now())
	}
	if lease.class >= 0 {
		class := &p.classes[lease.class]
		class.inUse.Add(^uint64(0))
		lease.length.Store(0)
		class.store.Put(lease.block)
	}
	lease.block = nil
	p.returned.Add(1)
	p.outstanding.Add(^uint64(0))
	return nil
}

func (p *BytePool) ScanOverdue(now time.Time) int {
	if p.config.AuditMode == AuditOff {
		return 0
	}
	return p.audit.scanOverdue(now)
}

func (p *BytePool) SnapshotLeases(state string, limit int) []LeaseSnapshot {
	return p.audit.snapshots(ParseLeaseState(state), 0, limit, time.Now())
}

// SnapshotLeasesBefore applies the exclusive ID cursor before limiting the
// result, so every retained history item remains reachable by pagination.
func (p *BytePool) SnapshotLeasesBefore(state string, beforeID uint64, limit int) []LeaseSnapshot {
	return p.audit.snapshots(ParseLeaseState(state), beforeID, limit, time.Now())
}

func (p *BytePool) LeaseByID(id uint64) (LeaseSnapshot, bool) {
	return p.audit.byID(id, time.Now())
}

func (p *BytePool) Stats() Stats {
	classes := make([]ClassStats, len(p.classes))
	for index := range p.classes {
		class := &p.classes[index]
		classes[index] = ClassStats{
			Size:     class.size,
			Borrowed: class.borrowed.Load(),
			InUse:    class.inUse.Load(),
			Hits:     class.hits.Load(),
			Misses:   class.misses.Load(),
		}
	}
	return Stats{
		Borrowed:         p.borrowed.Load(),
		Returned:         p.returned.Load(),
		Outstanding:      p.outstanding.Load(),
		PoolHits:         p.poolHits.Load(),
		PoolMisses:       p.poolMisses.Load(),
		Oversized:        p.oversized.Load(),
		LargeAllocations: p.largeAllocations.Load(),
		DoubleReturns:    p.doubleReturns.Load(),
		InvalidReturns:   p.invalidReturns.Load(),
		StateErrors:      p.stateErrors.Load(),
		UseAfterPublish:  p.useAfterPublish.Load(),
		Overdue:          p.overdue.Load(),
		Classes:          classes,
		Closed:           p.closed.Load(),
	}
}

func (p *BytePool) Close() {
	p.once.Do(func() {
		p.closed.Store(true)
		if p.config.AuditMode != AuditOff {
			close(p.stop)
		}
		<-p.done
	})
}

func (p *BytePool) classFor(capacity int) int {
	for index := range p.classes {
		if capacity <= p.classes[index].size {
			return index
		}
	}
	return -1
}

func (p *BytePool) shouldTrack(borrowNumber uint64) bool {
	switch p.config.AuditMode {
	case AuditFull:
		return true
	case AuditSampled:
		return borrowNumber%p.config.SampleEvery == 0
	default:
		return false
	}
}

func (p *BytePool) scanLoop() {
	defer close(p.done)
	ticker := time.NewTicker(p.config.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			p.ScanOverdue(now)
		case <-p.stop:
			return
		}
	}
}
