package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

const (
	payloadSize = 256
	capacity    = 65536
	producers   = 32
	batchSize   = 1024
)

type environment struct {
	GoVersion  string `json:"go_version"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	CPUModel   string `json:"cpu_model"`
	NumCPU     int    `json:"num_cpu"`
	GOMAXPROCS int    `json:"gomaxprocs"`
}
type latency struct {
	Samples int   `json:"samples"`
	P50NS   int64 `json:"p50_ns"`
	P95NS   int64 `json:"p95_ns"`
	P99NS   int64 `json:"p99_ns"`
	P999NS  int64 `json:"p999_ns"`
	MaxNS   int64 `json:"max_ns"`
}
type benchmarkConfig struct {
	PayloadBytes int    `json:"payload_bytes"`
	RingCapacity uint64 `json:"ring_capacity"`
	Producers    int    `json:"publish_producers"`
	BatchSize    int    `json:"batch_size"`
}
type publishResult struct {
	Audit                  string  `json:"audit"`
	Duration               string  `json:"duration"`
	Warmup                 string  `json:"warmup"`
	Sampling               string  `json:"sampling"`
	Attempts               uint64  `json:"attempts"`
	Accepted               uint64  `json:"accepted"`
	QueueFull              uint64  `json:"queue_full"`
	RejectedOther          uint64  `json:"rejected_other"`
	ThroughputEPS          float64 `json:"throughput_eps"`
	Latency                latency `json:"latency"`
	Outstanding            uint64  `json:"outstanding"`
	AbsoluteGateApplicable bool    `json:"absolute_gate_applicable"`
	TargetPass             bool    `json:"target_pass"`
	Pass                   bool    `json:"pass"`
}
type comparison struct {
	ThroughputDegradationPercent float64 `json:"throughput_degradation_percent"`
	P99IncreasePercent           float64 `json:"p99_increase_percent"`
}
type soakResult struct {
	Audit                string  `json:"audit"`
	Duration             string  `json:"duration"`
	ActualElapsed        string  `json:"actual_elapsed"`
	Warmup               string  `json:"warmup"`
	Rate                 int     `json:"rate"`
	AchievedRate         float64 `json:"achieved_rate"`
	Attempts             uint64  `json:"attempts"`
	Accepted             uint64  `json:"accepted"`
	QueueFull            uint64  `json:"queue_full"`
	RejectedOther        uint64  `json:"rejected_other"`
	BaselineHeapBytes    uint64  `json:"baseline_heap_bytes"`
	EndHeapBytes         uint64  `json:"end_heap_bytes"`
	MaxHeapBytes         uint64  `json:"max_heap_bytes"`
	HeapDriftPercent     float64 `json:"heap_drift_percent"`
	BaselineGoroutines   int     `json:"baseline_goroutines"`
	EndGoroutines        int     `json:"end_goroutines"`
	MaxGoroutines        int     `json:"max_goroutines"`
	GoroutineDrift       int     `json:"goroutine_drift"`
	Samples              int     `json:"samples"`
	Outstanding          uint64  `json:"outstanding"`
	Drained              bool    `json:"drained"`
	ThirtyMinuteEvidence bool    `json:"thirty_minute_evidence"`
	Pass                 bool    `json:"pass"`
}
type report struct {
	SchemaVersion int             `json:"schema_version"`
	GeneratedAt   time.Time       `json:"generated_at"`
	Mode          string          `json:"mode"`
	Environment   environment     `json:"environment"`
	Config        benchmarkConfig `json:"config"`
	Publish       []publishResult `json:"publish,omitempty"`
	Comparison    *comparison     `json:"comparison,omitempty"`
	Soak          *soakResult     `json:"soak,omitempty"`
	Pass          bool            `json:"pass"`
}
type phase struct {
	attempts, accepted, full, other uint64
	elapsed                         time.Duration
	samples                         []int64
}

func run(opts options) (report, error) {
	if opts.Duration <= 0 || opts.Warmup <= 0 {
		return report{}, errors.New("duration and warmup must be positive")
	}
	r := report{SchemaVersion: 1, GeneratedAt: time.Now().UTC(), Mode: opts.Mode, Environment: env(), Config: benchmarkConfig{payloadSize, capacity, producers, batchSize}}
	switch opts.Mode {
	case "publish":
		modes := []string{opts.Audit}
		if opts.Audit == "both" {
			modes = []string{"off", "full"}
		}
		for _, mode := range modes {
			result, err := runPublish(mode, opts.Warmup, opts.Duration, r.Environment.NumCPU)
			if err != nil {
				return report{}, err
			}
			r.Publish = append(r.Publish, result)
		}
		if len(r.Publish) == 2 {
			off, full := r.Publish[0], r.Publish[1]
			r.Comparison = &comparison{percentDrop(off.ThroughputEPS, full.ThroughputEPS), percentRise(float64(off.Latency.P99NS), float64(full.Latency.P99NS))}
		}
		r.Pass = len(r.Publish) > 0
		for _, result := range r.Publish {
			r.Pass = r.Pass && result.Pass
		}
	case "soak":
		if opts.Audit == "both" || opts.Rate <= 0 {
			return report{}, errors.New("soak requires audit off/full and positive rate")
		}
		// drive is deliberately a single rate-controlled producer; do not label
		// this stability run with the 32-producer publish benchmark topology.
		r.Config.Producers = 1
		result, err := runSoak(opts.Audit, opts.Warmup, opts.Duration, opts.Rate)
		if err != nil {
			return report{}, err
		}
		r.Soak, r.Pass = &result, result.Pass
	default:
		return report{}, errors.New("mode must be publish or soak")
	}
	return r, nil
}

func newAppender(audit string) (*minilogback.Appender, error) {
	mode := minilogback.AuditOff
	if audit == "full" {
		mode = minilogback.AuditFull
	} else if audit != "off" {
		return nil, errors.New("audit must be off, full, or both")
	}
	return minilogback.New(minilogback.Config{RingCapacity: capacity, BatchSize: batchSize, FlushInterval: 50 * time.Millisecond, MaxEventBytes: 1 << 20, AuditMode: mode, Sink: minilogback.NewWriterSink(io.Discard)})
}

func runPublish(audit string, warmup, duration time.Duration, cpus int) (publishResult, error) {
	appender, err := newAppender(audit)
	if err != nil {
		return publishResult{}, err
	}
	_ = runPhase(appender, warmup, false)
	measured := runPhase(appender, duration, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	closeErr := appender.Close(ctx)
	stats := appender.Stats()
	q := summarize(measured.samples)
	throughput := float64(measured.accepted) / measured.elapsed.Seconds()
	applicable := audit == "off" && cpus >= 8
	target := !applicable || (throughput >= 1_000_000 && q.P99NS <= 10_000 && q.P999NS <= 50_000)
	pass := closeErr == nil && measured.other == 0 && q.Samples >= 100 && stats.Pool.Outstanding == 0 && target
	return publishResult{Audit: audit, Duration: measured.elapsed.String(), Warmup: warmup.String(), Sampling: "first 512/producer, then every 32nd, capped 32768/producer", Attempts: measured.attempts, Accepted: measured.accepted, QueueFull: measured.full, RejectedOther: measured.other, ThroughputEPS: throughput, Latency: q, Outstanding: stats.Pool.Outstanding, AbsoluteGateApplicable: applicable, TargetPass: target, Pass: pass}, nil
}

func runPhase(appender *minilogback.Appender, duration time.Duration, collect bool) phase {
	var workers [producers]phase
	var wait sync.WaitGroup
	start := make(chan struct{})
	deadline := time.Now().Add(duration)
	var payload [payloadSize]byte
	for i := range payload {
		payload[i] = byte(i)
	}
	for id := range workers {
		wait.Add(1)
		go func(result *phase) {
			defer wait.Done()
			<-start
		events:
			for iteration := uint64(0); ; iteration++ {
				if iteration&255 == 0 && time.Now().After(deadline) {
					return
				}
				lease, err := appender.AcquireFor(minilogback.InfoLevel, payloadSize)
				if err != nil {
					result.attempts++
					result.other++
					continue
				}
				buffer, err := lease.Buffer()
				if err != nil {
					result.attempts++
					result.other++
					_ = lease.Release()
					continue
				}
				copy(buffer, payload[:])
				_ = lease.Commit(payloadSize)
				for {
					var began time.Time
					if collect {
						began = time.Now()
					}
					outcome := appender.TryPublishLease(minilogback.InfoLevel, lease)
					result.attempts++
					switch outcome {
					case minilogback.Accepted:
						result.accepted++
						if collect && len(result.samples) < 32768 && (result.accepted <= 512 || result.accepted%32 == 0) {
							result.samples = append(result.samples, time.Since(began).Nanoseconds())
						}
						continue events
					case minilogback.QueueFull:
						result.full++
						if time.Now().After(deadline) {
							_ = lease.Release()
							return
						}
						runtime.Gosched()
					default:
						result.other++
						_ = lease.Release()
						continue events
					}
				}
			}
		}(&workers[id])
	}
	started := time.Now()
	deadline = started.Add(duration)
	close(start)
	wait.Wait()
	combined := phase{elapsed: time.Since(started)}
	for _, worker := range workers {
		combined.attempts += worker.attempts
		combined.accepted += worker.accepted
		combined.full += worker.full
		combined.other += worker.other
		combined.samples = append(combined.samples, worker.samples...)
	}
	return combined
}

func runSoak(audit string, warmup, duration time.Duration, rate int) (soakResult, error) {
	appender, err := newAppender(audit)
	if err != nil {
		return soakResult{}, err
	}
	_ = drive(appender, warmup, rate, nil)
	if !waitDrain(appender, 30*time.Second) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = appender.Close(ctx)
		cancel()
		return soakResult{}, errors.New("warmup did not drain")
	}
	heapSamples := make([]uint64, 0, 64)
	runtime.GC()
	baseHeap, baseGo := memory(), runtime.NumGoroutine()
	maxHeap, maxGo := baseHeap, baseGo
	formal := drive(appender, duration, rate, func() {
		runtime.GC()
		h, g := memory(), runtime.NumGoroutine()
		heapSamples = append(heapSamples, h)
		if h > maxHeap {
			maxHeap = h
		}
		if g > maxGo {
			maxGo = g
		}
	})
	drained := waitDrain(appender, 30*time.Second)
	stats := appender.Stats()
	runtime.GC()
	endHeap, endGo := memory(), runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	closeErr := appender.Close(ctx)
	cancel()
	if endHeap > maxHeap {
		maxHeap = endHeap
	}
	if endGo > maxGo {
		maxGo = endGo
	}
	drift := percentRise(float64(baseHeap), float64(endHeap))
	goDrift := endGo - baseGo
	pass := drained && closeErr == nil && formal.full == 0 && formal.other == 0 && stats.Pool.Outstanding == 0 && drift <= 10 && goDrift <= 1
	return soakResult{Audit: audit, Duration: duration.String(), ActualElapsed: formal.elapsed.String(), Warmup: warmup.String(), Rate: rate, AchievedRate: float64(formal.attempts) / formal.elapsed.Seconds(), Attempts: formal.attempts, Accepted: formal.accepted, QueueFull: formal.full, RejectedOther: formal.other, BaselineHeapBytes: baseHeap, EndHeapBytes: endHeap, MaxHeapBytes: maxHeap, HeapDriftPercent: drift, BaselineGoroutines: baseGo, EndGoroutines: endGo, MaxGoroutines: maxGo, GoroutineDrift: goDrift, Samples: len(heapSamples), Outstanding: stats.Pool.Outstanding, Drained: drained, ThirtyMinuteEvidence: duration >= 30*time.Minute, Pass: pass}, nil
}

func drive(appender *minilogback.Appender, duration time.Duration, rate int, sample func()) phase {
	result := phase{}
	publish := func() {
		lease, err := appender.AcquireFor(minilogback.InfoLevel, payloadSize)
		if err != nil {
			result.attempts++
			result.other++
			return
		}
		buffer, _ := lease.Buffer()
		for i := 0; i < payloadSize; i++ {
			buffer[i] = byte(i)
		}
		_ = lease.Commit(payloadSize)
		switch appender.TryPublishLease(minilogback.InfoLevel, lease) {
		case minilogback.Accepted:
			result.accepted++
		case minilogback.QueueFull:
			result.full++
			_ = lease.Release()
		default:
			result.other++
			_ = lease.Release()
		}
		result.attempts++
	}
	started := time.Now()
	tick := 10 * time.Millisecond
	if duration < 100*time.Millisecond {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	nextSample := started
	for now := range ticker.C {
		if now.Sub(started) >= duration {
			break
		}
		target := uint64(now.Sub(started)) * uint64(rate) / uint64(time.Second)
		for result.attempts < target {
			publish()
		}
		if sample != nil && !now.Before(nextSample) {
			sample()
			nextSample = now.Add(maxDuration(10*time.Millisecond, duration/20))
		}
	}
	target := uint64(duration) * uint64(rate) / uint64(time.Second)
	for result.attempts < target {
		publish()
	}
	result.elapsed = time.Since(started)
	if sample != nil {
		sample()
	}
	return result
}

func waitDrain(appender *minilogback.Appender, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := appender.Stats()
		if stats.Ring.Depth == 0 && stats.Flusher.InFlight == 0 && stats.Accepted == stats.Flusher.Records {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func summarize(values []int64) latency {
	if len(values) == 0 {
		return latency{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(q float64) int64 {
		index := int(q*float64(len(values))+.999999) - 1
		if index < 0 {
			index = 0
		}
		return values[index]
	}
	return latency{Samples: len(values), P50NS: at(.50), P95NS: at(.95), P99NS: at(.99), P999NS: at(.999), MaxNS: values[len(values)-1]}
}

func env() environment {
	return environment{runtime.Version(), runtime.GOOS, runtime.GOARCH, cpuModel(), runtime.NumCPU(), runtime.GOMAXPROCS(0)}
}
func memory() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}
func percentDrop(base, current float64) float64 {
	if base == 0 {
		return 0
	}
	return (base - current) * 100 / base
}
func percentRise(base, current float64) float64 {
	if base == 0 {
		return 0
	}
	return (current - base) * 100 / base
}
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func cpuModel() string {
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if fields := strings.SplitN(line, ":", 2); len(fields) == 2 && (strings.TrimSpace(fields[0]) == "model name" || strings.TrimSpace(fields[0]) == "Hardware") {
				return strings.TrimSpace(fields[1])
			}
		}
	}
	command := "sysctl"
	if runtime.GOOS == "darwin" {
		command = "/usr/sbin/sysctl"
	}
	if output, err := exec.Command(command, "-n", "machdep.cpu.brand_string").Output(); err == nil && strings.TrimSpace(string(output)) != "" {
		return strings.TrimSpace(string(output))
	}
	return "unavailable"
}
