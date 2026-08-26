package pool

import (
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type auditRecord struct {
	lease          *Lease
	level          string
	logger         string
	frames         []Frame
	function       string
	file           string
	line           int
	goroutineID    uint64
	overdue        bool
	payloadMutated bool
}

type auditRegistry struct {
	owner        *BytePool
	root         string
	stackDepth   int
	historyLimit int
	mu           sync.RWMutex
	active       map[uint64]*auditRecord
	history      []LeaseSnapshot
}

func newAuditRegistry(owner *BytePool, config Config) *auditRegistry {
	root := filepath.Clean(config.ProjectRoot)
	if config.ProjectRoot == "" {
		root = ""
	}
	return &auditRegistry{
		owner:        owner,
		root:         root,
		stackDepth:   config.StackDepth,
		historyLimit: config.HistoryLimit,
		active:       make(map[uint64]*auditRecord),
	}
}

func (r *auditRegistry) register(lease *Lease, context AuditContext) {
	frames := captureFrames(5+context.CallerSkip, r.stackDepth, r.root)
	record := &auditRecord{
		lease:       lease,
		level:       context.Level,
		logger:      context.Logger,
		frames:      frames,
		goroutineID: currentGoroutineID(),
	}
	if len(frames) > 0 {
		record.function = frames[0].Function
		record.file = frames[0].File
		record.line = frames[0].Line
	}
	r.mu.Lock()
	r.active[lease.id] = record
	r.mu.Unlock()
}

func (r *auditRegistry) updateContext(id uint64, context AuditContext) {
	r.mu.Lock()
	if record := r.active[id]; record != nil {
		record.level = context.Level
		record.logger = context.Logger
	}
	r.mu.Unlock()
}

func (r *auditRegistry) markMutated(id uint64) {
	r.mu.Lock()
	if record := r.active[id]; record != nil {
		record.payloadMutated = true
	}
	r.mu.Unlock()
}

func (r *auditRegistry) scanOverdue(now time.Time) int {
	newlyOverdue := 0
	r.mu.Lock()
	for _, record := range r.active {
		if !record.overdue && now.After(record.lease.deadline) {
			record.overdue = true
			newlyOverdue++
		}
	}
	r.mu.Unlock()
	if newlyOverdue != 0 {
		r.owner.overdue.Add(uint64(newlyOverdue))
	}
	return newlyOverdue
}

func (r *auditRegistry) finalize(lease *Lease, returnedAt time.Time) {
	r.mu.Lock()
	record := r.active[lease.id]
	if record == nil {
		r.mu.Unlock()
		return
	}
	delete(r.active, lease.id)
	if record.overdue {
		r.owner.overdue.Add(^uint64(0))
	}
	snapshot := r.snapshotRecord(record, returnedAt)
	snapshot.State = StateReturned.String()
	snapshot.ReturnedAt = returnedAt
	if r.historyLimit > 0 {
		r.history = append(r.history, snapshot)
		if overflow := len(r.history) - r.historyLimit; overflow > 0 {
			copy(r.history, r.history[overflow:])
			r.history = r.history[:r.historyLimit]
		}
	}
	r.mu.Unlock()
}

func (r *auditRegistry) snapshots(filter string, beforeID uint64, limit int, now time.Time) []LeaseSnapshot {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	r.mu.RLock()
	items := make([]LeaseSnapshot, 0, len(r.active)+len(r.history))
	for _, record := range r.active {
		snapshot := r.snapshotRecord(record, now)
		if filter == "" || snapshot.State == filter {
			items = append(items, snapshot)
		}
	}
	for _, snapshot := range r.history {
		if filter == "" || snapshot.State == filter {
			items = append(items, cloneSnapshot(snapshot))
		}
	}
	r.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	result := make([]LeaseSnapshot, 0, min(limit, len(items)))
	for _, item := range items {
		if beforeID != 0 && item.ID >= beforeID {
			continue
		}
		result = append(result, item)
		if len(result) == limit {
			break
		}
	}
	return result
}

func (r *auditRegistry) byID(id uint64, now time.Time) (LeaseSnapshot, bool) {
	r.mu.RLock()
	if record := r.active[id]; record != nil {
		snapshot := r.snapshotRecord(record, now)
		r.mu.RUnlock()
		return snapshot, true
	}
	for index := len(r.history) - 1; index >= 0; index-- {
		if r.history[index].ID == id {
			snapshot := cloneSnapshot(r.history[index])
			r.mu.RUnlock()
			return snapshot, true
		}
	}
	r.mu.RUnlock()
	return LeaseSnapshot{}, false
}

func (r *auditRegistry) snapshotRecord(record *auditRecord, now time.Time) LeaseSnapshot {
	lease := record.lease
	state := lease.State().String()
	if record.overdue {
		state = "OVERDUE"
	}
	return LeaseSnapshot{
		ID:             lease.id,
		State:          state,
		ClassSize:      lease.ClassSize(),
		Capacity:       lease.Capacity(),
		Length:         int(lease.length.Load()),
		BorrowedAt:     lease.borrowed,
		Deadline:       lease.deadline,
		AgeNanos:       now.Sub(lease.borrowed).Nanoseconds(),
		GoroutineID:    record.goroutineID,
		Level:          record.level,
		Logger:         record.logger,
		Function:       record.function,
		File:           record.file,
		Line:           record.line,
		Stack:          record.frames,
		PayloadMutated: record.payloadMutated,
	}
}

func cloneSnapshot(snapshot LeaseSnapshot) LeaseSnapshot {
	snapshot.Stack = append([]Frame(nil), snapshot.Stack...)
	return snapshot
}

func captureFrames(skip, depth int, root string) []Frame {
	pcs := make([]uintptr, depth)
	count := runtime.Callers(skip, pcs)
	iterator := runtime.CallersFrames(pcs[:count])
	frames := make([]Frame, 0, count)
	for {
		frame, more := iterator.Next()
		frames = append(frames, Frame{
			Function: frame.Function,
			File:     sanitizePath(frame.File, root),
			Line:     frame.Line,
		})
		if !more {
			break
		}
	}
	return frames
}

func sanitizePath(path, root string) string {
	if path == "" {
		return ""
	}
	if root != "" {
		if relative, err := filepath.Rel(root, path); err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return filepath.ToSlash(relative)
		}
	}
	return filepath.Base(path)
}

func currentGoroutineID() uint64 {
	var buffer [64]byte
	count := runtime.Stack(buffer[:], false)
	fields := strings.Fields(string(buffer[:count]))
	if len(fields) < 2 || fields[0] != "goroutine" {
		return 0
	}
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}
