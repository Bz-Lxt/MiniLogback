package collector

import (
	"container/list"
	"context"
	"sync"
)

type BatchKey struct {
	ClientID uint64
	BatchID  uint64
}

type completedEntry struct{ key BatchKey }

type pendingCall struct {
	done chan struct{}
	err  error
}

type Dedupe struct {
	mu        sync.Mutex
	capacity  int
	completed map[BatchKey]*list.Element
	lru       *list.List
	pending   map[BatchKey]*pendingCall
}

func NewDedupe(capacity int) *Dedupe {
	if capacity < 1 {
		capacity = 1
	}
	return &Dedupe{capacity: capacity, completed: make(map[BatchKey]*list.Element), lru: list.New(), pending: make(map[BatchKey]*pendingCall)}
}

// Do serializes concurrent attempts for key and records only successful work.
// duplicate is true when fn was not executed because a prior attempt completed.
func (d *Dedupe) Do(ctx context.Context, key BatchKey, fn func() error) (duplicate bool, err error) {
	d.mu.Lock()
	if element, ok := d.completed[key]; ok {
		d.lru.MoveToFront(element)
		d.mu.Unlock()
		return true, nil
	}
	if call, ok := d.pending[key]; ok {
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-call.done:
			return call.err == nil, call.err
		}
	}
	call := &pendingCall{done: make(chan struct{})}
	d.pending[key] = call
	d.mu.Unlock()

	err = fn()
	d.mu.Lock()
	delete(d.pending, key)
	call.err = err
	element := d.lru.PushFront(completedEntry{key: key})
	d.completed[key] = element
	if d.lru.Len() > d.capacity {
		oldest := d.lru.Back()
		delete(d.completed, oldest.Value.(completedEntry).key)
		d.lru.Remove(oldest)
	}
	close(call.done)
	d.mu.Unlock()
	return false, err
}

func (d *Dedupe) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.completed)
}
