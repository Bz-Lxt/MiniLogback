package ring

// Stats is a consistent-enough lock-free snapshot. Individual fields are
// atomic, though producers may advance between field loads.
type Stats struct {
	Capacity        uint64 `json:"capacity"`
	Depth           uint64 `json:"depth"`
	HighWater       uint64 `json:"high_water"`
	PublishAttempts uint64 `json:"publish_attempts"`
	Published       uint64 `json:"published"`
	Consumed        uint64 `json:"consumed"`
	RejectedFull    uint64 `json:"rejected_full"`
	RejectedClosed  uint64 `json:"rejected_closed"`
	RejectedInvalid uint64 `json:"rejected_invalid"`
	Closed          bool   `json:"closed"`
}

// Dropped reports admission failures caused by capacity or CAS contention.
func (s Stats) Dropped() uint64 { return s.RejectedFull }

// Watermark returns queue occupancy in the inclusive range [0, 1].
func (s Stats) Watermark() float64 {
	if s.Capacity == 0 {
		return 0
	}
	return float64(s.Depth) / float64(s.Capacity)
}
