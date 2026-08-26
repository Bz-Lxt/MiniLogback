package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr                string
	IngestAddr              string
	RingCapacity            uint64
	BatchSize               int
	FlushInterval           time.Duration
	MaxEventBytes           int
	AuditMode               string
	LeaseTimeout            time.Duration
	AuditInterval           time.Duration
	Sink                    string
	LogPath                 string
	NetworkAddr             string
	NetworkDialTimeout      time.Duration
	NetworkIOTimeout        time.Duration
	NetworkRetryInitial     time.Duration
	NetworkRetryMaximum     time.Duration
	NetworkMaxAttempts      int
	RotateBytes             int64
	SyncOnFlush             bool
	DemoMode                bool
	ContainerMode           bool
	ProjectRoot             string
	WebDir                  string
	ShutdownTimeout         time.Duration
	CollectorMaxConnections int
	CollectorReadTimeout    time.Duration
	CollectorDedupeEntries  int
	ProtocolMaxRecords      uint32
	ProtocolMaxPayload      uint32
	TelemetryInterval       time.Duration
	SSEMaxClients           int
	Version                 string
}

func Defaults() Config {
	return Config{
		HTTPAddr:                "127.0.0.1:28640",
		IngestAddr:              "127.0.0.1:28641",
		RingCapacity:            65536,
		BatchSize:               1024,
		FlushInterval:           50 * time.Millisecond,
		MaxEventBytes:           1 << 20,
		AuditMode:               "off",
		LeaseTimeout:            2 * time.Second,
		AuditInterval:           100 * time.Millisecond,
		Sink:                    "file",
		LogPath:                 "./data/minilogback.log",
		NetworkAddr:             "127.0.0.1:28641",
		NetworkDialTimeout:      2 * time.Second,
		NetworkIOTimeout:        5 * time.Second,
		NetworkRetryInitial:     50 * time.Millisecond,
		NetworkRetryMaximum:     2 * time.Second,
		NetworkMaxAttempts:      8,
		RotateBytes:             64 << 20,
		ShutdownTimeout:         5 * time.Second,
		WebDir:                  "./frontend/dist",
		CollectorMaxConnections: 256,
		CollectorReadTimeout:    10 * time.Second,
		CollectorDedupeEntries:  100000,
		ProtocolMaxRecords:      1024,
		ProtocolMaxPayload:      16 << 20,
		TelemetryInterval:       100 * time.Millisecond,
		SSEMaxClients:           64,
		Version:                 "dev",
	}
}

func Load() (Config, error) { return Parse(os.LookupEnv) }

func Parse(lookup func(string) (string, bool)) (Config, error) {
	cfg := Defaults()
	stringValue(lookup, "MINILOGBACK_HTTP_ADDR", &cfg.HTTPAddr)
	stringValue(lookup, "MINILOGBACK_INGEST_ADDR", &cfg.IngestAddr)
	stringValue(lookup, "MINILOGBACK_AUDIT_MODE", &cfg.AuditMode)
	stringValue(lookup, "MINILOGBACK_SINK", &cfg.Sink)
	stringValue(lookup, "MINILOGBACK_LOG_PATH", &cfg.LogPath)
	stringValue(lookup, "MINILOGBACK_NETWORK_ADDR", &cfg.NetworkAddr)
	stringValue(lookup, "MINILOGBACK_PROJECT_ROOT", &cfg.ProjectRoot)
	stringValue(lookup, "MINILOGBACK_WEB_DIR", &cfg.WebDir)
	stringValue(lookup, "MINILOGBACK_VERSION", &cfg.Version)

	parsers := []func() error{
		func() error { return uint64Value(lookup, "MINILOGBACK_RING_CAPACITY", &cfg.RingCapacity) },
		func() error { return intValue(lookup, "MINILOGBACK_BATCH_SIZE", &cfg.BatchSize) },
		func() error { return durationValue(lookup, "MINILOGBACK_FLUSH_INTERVAL", &cfg.FlushInterval) },
		func() error { return intValue(lookup, "MINILOGBACK_MAX_EVENT_BYTES", &cfg.MaxEventBytes) },
		func() error { return durationValue(lookup, "MINILOGBACK_LEASE_TIMEOUT", &cfg.LeaseTimeout) },
		func() error { return durationValue(lookup, "MINILOGBACK_AUDIT_INTERVAL", &cfg.AuditInterval) },
		func() error {
			return durationValue(lookup, "MINILOGBACK_NETWORK_DIAL_TIMEOUT", &cfg.NetworkDialTimeout)
		},
		func() error { return durationValue(lookup, "MINILOGBACK_NETWORK_IO_TIMEOUT", &cfg.NetworkIOTimeout) },
		func() error {
			return durationValue(lookup, "MINILOGBACK_NETWORK_RETRY_INITIAL", &cfg.NetworkRetryInitial)
		},
		func() error {
			return durationValue(lookup, "MINILOGBACK_NETWORK_RETRY_MAXIMUM", &cfg.NetworkRetryMaximum)
		},
		func() error { return intValue(lookup, "MINILOGBACK_NETWORK_MAX_ATTEMPTS", &cfg.NetworkMaxAttempts) },
		func() error { return int64Value(lookup, "MINILOGBACK_ROTATE_BYTES", &cfg.RotateBytes) },
		func() error { return boolValue(lookup, "MINILOGBACK_SYNC_ON_FLUSH", &cfg.SyncOnFlush) },
		func() error { return boolValue(lookup, "MINILOGBACK_DEMO_MODE", &cfg.DemoMode) },
		func() error { return boolValue(lookup, "MINILOGBACK_CONTAINER_MODE", &cfg.ContainerMode) },
		func() error { return durationValue(lookup, "MINILOGBACK_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout) },
		func() error {
			return intValue(lookup, "MINILOGBACK_COLLECTOR_MAX_CONNECTIONS", &cfg.CollectorMaxConnections)
		},
		func() error {
			return durationValue(lookup, "MINILOGBACK_COLLECTOR_READ_TIMEOUT", &cfg.CollectorReadTimeout)
		},
		func() error {
			return intValue(lookup, "MINILOGBACK_COLLECTOR_DEDUPE_ENTRIES", &cfg.CollectorDedupeEntries)
		},
		func() error { return uint32Value(lookup, "MINILOGBACK_PROTOCOL_MAX_RECORDS", &cfg.ProtocolMaxRecords) },
		func() error { return uint32Value(lookup, "MINILOGBACK_PROTOCOL_MAX_PAYLOAD", &cfg.ProtocolMaxPayload) },
		func() error { return durationValue(lookup, "MINILOGBACK_TELEMETRY_INTERVAL", &cfg.TelemetryInterval) },
		func() error { return intValue(lookup, "MINILOGBACK_SSE_MAX_CLIENTS", &cfg.SSEMaxClients) },
	}
	for _, parse := range parsers {
		if err := parse(); err != nil {
			return Config{}, err
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	for name, addr := range map[string]string{"MINILOGBACK_HTTP_ADDR": c.HTTPAddr, "MINILOGBACK_INGEST_ADDR": c.IngestAddr} {
		if err := validateAddress(addr); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if c.RingCapacity < 2 || c.RingCapacity&(c.RingCapacity-1) != 0 {
		return fmt.Errorf("MINILOGBACK_RING_CAPACITY must be a power of two >= 2")
	}
	if c.BatchSize < 1 || uint64(c.BatchSize) > c.RingCapacity {
		return fmt.Errorf("MINILOGBACK_BATCH_SIZE must be between 1 and ring capacity")
	}
	if c.MaxEventBytes < 1 || uint64(c.MaxEventBytes) > uint64(c.ProtocolMaxPayload) {
		return fmt.Errorf("MINILOGBACK_MAX_EVENT_BYTES must fit protocol payload")
	}
	if c.ProtocolMaxRecords == 0 || c.ProtocolMaxPayload == 0 {
		return fmt.Errorf("protocol limits must be positive")
	}
	if c.ProtocolMaxRecords > 1<<16 || c.ProtocolMaxPayload > 64<<20 || c.MaxEventBytes > 16<<20 {
		return fmt.Errorf("protocol limits exceed safe implementation bounds")
	}
	if c.FlushInterval <= 0 || c.LeaseTimeout <= 0 || c.AuditInterval <= 0 || c.ShutdownTimeout <= 0 || c.TelemetryInterval <= 0 || c.CollectorReadTimeout <= 0 {
		return fmt.Errorf("duration settings must be positive")
	}
	if c.AuditMode != "off" && c.AuditMode != "sampled" && c.AuditMode != "full" {
		return fmt.Errorf("MINILOGBACK_AUDIT_MODE must be off, sampled, or full")
	}
	if c.Sink != "file" && c.Sink != "network" {
		return fmt.Errorf("MINILOGBACK_SINK must be file or network")
	}
	if c.Sink == "file" && strings.TrimSpace(c.LogPath) == "" {
		return fmt.Errorf("MINILOGBACK_LOG_PATH is required for file sink")
	}
	if c.Sink == "network" {
		if err := validateAddress(c.NetworkAddr); err != nil {
			return fmt.Errorf("MINILOGBACK_NETWORK_ADDR: %w", err)
		}
	}
	if c.NetworkDialTimeout <= 0 || c.NetworkIOTimeout <= 0 || c.NetworkRetryInitial <= 0 || c.NetworkRetryMaximum < c.NetworkRetryInitial || c.NetworkMaxAttempts <= 0 {
		return fmt.Errorf("network timeouts, retry bounds, and attempts must be positive and ordered")
	}
	if c.RotateBytes <= 0 || c.CollectorMaxConnections <= 0 || c.CollectorDedupeEntries <= 0 || c.SSEMaxClients <= 0 {
		return fmt.Errorf("capacity settings must be positive")
	}
	if strings.TrimSpace(c.WebDir) == "" {
		return fmt.Errorf("MINILOGBACK_WEB_DIR must not be empty")
	}
	return nil
}

func (c Config) DemoAllowed() bool {
	if !c.DemoMode {
		return false
	}
	host, _, err := net.SplitHostPort(c.HTTPAddr)
	if err != nil {
		return false
	}
	trimmed := strings.Trim(host, "[]")
	if strings.EqualFold(trimmed, "localhost") {
		return true
	}
	if ip := net.ParseIP(trimmed); ip != nil && ip.IsLoopback() {
		return true
	}
	return c.ContainerMode && (trimmed == "" || trimmed == "0.0.0.0" || trimmed == "::")
}

func (c Config) Effective() map[string]any {
	target := filepath.Base(c.LogPath)
	if c.Sink == "network" {
		target = c.NetworkAddr
	}
	return map[string]any{
		"ring_capacity":    c.RingCapacity,
		"batch_size":       c.BatchSize,
		"flush_interval":   c.FlushInterval.String(),
		"max_event_bytes":  c.MaxEventBytes,
		"audit_mode":       c.AuditMode,
		"lease_timeout":    c.LeaseTimeout.String(),
		"sink_type":        c.Sink,
		"sink_target":      target,
		"demo_mode":        c.DemoMode,
		"demo_allowed":     c.DemoAllowed(),
		"web_assets":       filepath.Base(filepath.Clean(c.WebDir)),
		"network_security": "trusted_network_plaintext",
		"capabilities": map[string]bool{
			"vectored_file": true, "vectored_network": true, "sse": true,
		},
	}
}

func validateAddress(value string) error {
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("invalid host:port %q", value)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port in %q", value)
	}
	return nil
}

func stringValue(lookup func(string) (string, bool), name string, target *string) {
	if value, ok := lookup(name); ok {
		*target = strings.TrimSpace(value)
	}
}

func intValue(lookup func(string) (string, bool), name string, target *int) error {
	if value, ok := lookup(name); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*target = parsed
	}
	return nil
}

func int64Value(lookup func(string) (string, bool), name string, target *int64) error {
	if value, ok := lookup(name); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*target = parsed
	}
	return nil
}

func uint64Value(lookup func(string) (string, bool), name string, target *uint64) error {
	if value, ok := lookup(name); ok {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*target = parsed
	}
	return nil
}

func uint32Value(lookup func(string) (string, bool), name string, target *uint32) error {
	var parsed uint64
	if err := uint64Value(lookup, name, &parsed); err != nil {
		return err
	}
	if _, ok := lookup(name); !ok {
		return nil
	}
	if parsed > uint64(^uint32(0)) {
		return fmt.Errorf("%s: value overflows uint32", name)
	}
	*target = uint32(parsed)
	return nil
}

func durationValue(lookup func(string) (string, bool), name string, target *time.Duration) error {
	if value, ok := lookup(name); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*target = parsed
	}
	return nil
}

func boolValue(lookup func(string) (string, bool), name string, target *bool) error {
	if value, ok := lookup(name); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*target = parsed
	}
	return nil
}
