package ring

import (
	"errors"
	"fmt"
	"sync/atomic"
)

const (
	cacheLineSize      = 64
	maxCapacity        = uint64(1 << 30)
	maxPublishAttempts = 128
)

var ErrInvalidCapacity = errors.New("ring capacity must be a power of two between 2 and 2^30")

type paddedCursor struct {
	value atomic.Uint64
	_     [cacheLineSize - 8]byte
}

type slot[T any] struct {
	sequence atomic.Uint64
	value    atomic.Pointer[T]
	_        [cacheLineSize - 16]byte
}

// Queue is a bounded MPSC ring. Exactly one goroutine may call TryConsume;
// any number of goroutines may call TryPublish concurrently.
type Queue[T any] struct {
	producer paddedCursor
	consumer paddedCursor

	capacity uint64
	mask     uint64
	slots    []slot[T]
	closed   atomic.Bool

	publishAttempts atomic.Uint64
	published       atomic.Uint64
	consumed        atomic.Uint64
	rejectedFull    atomic.Uint64
	rejectedClosed  atomic.Uint64
	rejectedInvalid atomic.Uint64
	highWater       atomic.Uint64
}

// New constructs an empty queue. A capacity limit prevents signed sequence
// comparisons from becoming ambiguous and avoids accidental multi-gigabyte
// allocations from untrusted configuration.
func New[T any](capacity uint64) (*Queue[T], error) {
	if capacity < 2 || capacity > maxCapacity || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidCapacity, capacity)
	}
	q := &Queue[T]{
		capacity: capacity,
		mask:     capacity - 1,
		slots:    make([]slot[T], int(capacity)),
	}
	for sequence := uint64(0); sequence < capacity; sequence++ {
		q.slots[sequence].sequence.Store(sequence)
	}
	return q, nil
}

// TryPublish attempts to admit value without blocking. Under extreme producer
// contention, the bounded CAS budget is treated as queue admission pressure so
// callers still receive in finite time.
func (q *Queue[T]) TryPublish(value *T) PublishResult {
	q.publishAttempts.Add(1)
	if value == nil {
		q.rejectedInvalid.Add(1)
		return PublishInvalid
	}
	if q.closed.Load() {
		q.rejectedClosed.Add(1)
		return PublishClosed
	}

	for attempt := 0; attempt < maxPublishAttempts; attempt++ {
		if q.closed.Load() {
			q.rejectedClosed.Add(1)
			return PublishClosed
		}
		position := q.producer.value.Load()
		cell := &q.slots[position&q.mask]
		sequence := cell.sequence.Load()
		difference := int64(sequence - position)

		switch {
		case difference == 0:
			cell.value.Store(value)
			cell.sequence.Store(position + 1)
			if !q.producer.value.CompareAndSwap(position, position+1) {
				continue
			}
			q.observeHighWater(position + 1 - q.consumer.value.Load())
			q.published.Add(1)
			return PublishAccepted
		case difference < 0:
			q.rejectedFull.Add(1)
			return PublishFull
		default:
			// Another producer advanced. Reload the cursor and retry.
		}
	}

	q.rejectedFull.Add(1)
	return PublishFull
}

// TryConsume removes one item if the next sequence has been published. It is
// deliberately non-blocking and must only be called by the single consumer.
func (q *Queue[T]) TryConsume() (*T, bool) {
	position := q.consumer.value.Load()
	cell := &q.slots[position&q.mask]
	if int64(cell.sequence.Load()-(position+1)) != 0 {
		return nil, false
	}
	value := cell.value.Swap(nil)
	if value == nil {
		// A published slot can never contain nil. Do not release a corrupt slot.
		return nil, false
	}
	cell.sequence.Store(position + q.capacity)
	q.consumer.value.Store(position + 1)
	q.consumed.Add(1)
	return value, true
}

// Close prevents subsequent admissions. Items accepted before or concurrent
// with Close remain available to the consumer.
func (q *Queue[T]) Close() { q.closed.Store(true) }

func (q *Queue[T]) Closed() bool { return q.closed.Load() }

func (q *Queue[T]) Capacity() uint64 { return q.capacity }

func (q *Queue[T]) Depth() uint64 {
	// Load the monotonically trailing cursor first. Loading in the opposite
	// order lets the consumer advance between loads and creates a transient
	// unsigned underflow that looks like a full queue.
	consumer := q.consumer.value.Load()
	depth := q.producer.value.Load() - consumer
	if depth > q.capacity {
		return q.capacity
	}
	return depth
}

func (q *Queue[T]) Empty() bool { return q.Depth() == 0 }

func (q *Queue[T]) Stats() Stats {
	return Stats{
		Capacity:        q.capacity,
		Depth:           q.Depth(),
		HighWater:       q.highWater.Load(),
		PublishAttempts: q.publishAttempts.Load(),
		Published:       q.published.Load(),
		Consumed:        q.consumed.Load(),
		RejectedFull:    q.rejectedFull.Load(),
		RejectedClosed:  q.rejectedClosed.Load(),
		RejectedInvalid: q.rejectedInvalid.Load(),
		Closed:          q.closed.Load(),
	}
}

func (q *Queue[T]) observeHighWater(depth uint64) {
	if depth > q.capacity {
		depth = q.capacity
	}
	for current := q.highWater.Load(); depth > current; current = q.highWater.Load() {
		if q.highWater.CompareAndSwap(current, depth) {
			return
		}
	}
}

// seedEmptyForTest moves an empty queue to an arbitrary cursor so wrap-around
// behavior can be tested without billions of operations.
func (q *Queue[T]) seedEmptyForTest(base uint64) {
	q.producer.value.Store(base)
	q.consumer.value.Store(base)
	for index := uint64(0); index < q.capacity; index++ {
		offset := (index - (base & q.mask)) & q.mask
		q.slots[index].value.Store(nil)
		q.slots[index].sequence.Store(base + offset)
	}
}
