package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavskye/minilogback/internal/collector"
	"github.com/xavskye/minilogback/internal/config"
	"github.com/xavskye/minilogback/internal/httpapi"
	"github.com/xavskye/minilogback/internal/protocol"
	"github.com/xavskye/minilogback/internal/sink"
	"github.com/xavskye/minilogback/internal/telemetry"
	"github.com/xavskye/minilogback/pkg/minilogback"
)

type runtimeState struct {
	config      config.Config
	logger      *slog.Logger
	started     time.Time
	appender    *minilogback.Appender
	collector   *collector.Server
	localFile   *sink.FileSink
	appSink     *observedSink
	closing     atomic.Bool
	demoMu      sync.Mutex
	demoCancel  context.CancelFunc
	demoLeases  map[uint64]*minilogback.Lease
	demoWG      sync.WaitGroup
	rootContext context.Context
}

func newRuntime(ctx context.Context, cfg config.Config, logger *slog.Logger) (*runtimeState, error) {
	localFile, err := sink.NewFile(sink.FileConfig{Path: cfg.LogPath, MaxBytes: cfg.RotateBytes, SyncPolicy: syncPolicy(cfg.SyncOnFlush)})
	if err != nil {
		return nil, err
	}
	var appenderSink minilogback.Sink = localFile
	mode := localFile.Capabilities().BatchMode
	if cfg.Sink == "network" {
		client, clientErr := protocol.NewClient(protocol.ClientConfig{
			Address: cfg.NetworkAddr, DialTimeout: cfg.NetworkDialTimeout, IOTimeout: cfg.NetworkIOTimeout,
			RetryInitial: cfg.NetworkRetryInitial, RetryMaximum: cfg.NetworkRetryMaximum, MaxAttempts: cfg.NetworkMaxAttempts,
			Limits: protocol.Limits{MaxRecords: cfg.ProtocolMaxRecords, MaxPayload: cfg.ProtocolMaxPayload, MaxEventBytes: uint32(cfg.MaxEventBytes)},
		})
		if clientErr != nil {
			_ = localFile.Close()
			return nil, clientErr
		}
		appenderSink = sink.NewNetworkAdapter(client)
		mode = "scatter_gather"
	}
	observed := newObservedSink(appenderSink, mode)
	appender, err := minilogback.New(minilogback.Config{
		Name: "minilogbackd", MinLevel: minilogback.DebugLevel, RingCapacity: cfg.RingCapacity,
		BatchSize: cfg.BatchSize, FlushInterval: cfg.FlushInterval, MaxEventBytes: cfg.MaxEventBytes,
		AuditMode: minilogback.AuditMode(strings.ToUpper(cfg.AuditMode)), LeaseTimeout: cfg.LeaseTimeout,
		AuditScanInterval: cfg.AuditInterval, ProjectRoot: cfg.ProjectRoot, Sink: observed,
	})
	if err != nil {
		_ = observed.Close()
		_ = localFile.Close()
		return nil, err
	}
	collectorServer, err := collector.New(collector.Config{
		Address: cfg.IngestAddr, MaxConnections: cfg.CollectorMaxConnections,
		ReadTimeout: cfg.CollectorReadTimeout, WriteTimeout: cfg.CollectorReadTimeout,
		SinkTimeout: cfg.CollectorReadTimeout, DedupeEntries: cfg.CollectorDedupeEntries,
		Limits: protocol.Limits{MaxRecords: cfg.ProtocolMaxRecords, MaxPayload: cfg.ProtocolMaxPayload, MaxEventBytes: uint32(cfg.MaxEventBytes)},
	}, localFile, logger)
	if err != nil {
		_ = appender.Close(context.Background())
		_ = localFile.Close()
		return nil, err
	}
	return &runtimeState{
		config: cfg, logger: logger, started: time.Now(), appender: appender, collector: collectorServer,
		localFile: localFile, appSink: observed, demoLeases: make(map[uint64]*minilogback.Lease), rootContext: ctx,
	}, nil
}

func syncPolicy(enabled bool) sink.SyncPolicy {
	if enabled {
		return sink.SyncEveryBatch
	}
	return sink.SyncManual
}

func (s *runtimeState) publish(level string, payloadBytes int) {
	lease, err := s.appender.Acquire(payloadBytes)
	if err != nil {
		return
	}
	buffer, err := lease.Buffer()
	if err != nil {
		_ = lease.Release()
		return
	}
	fillDemoPayload(buffer[:payloadBytes], level, lease.ID())
	if err := lease.Commit(payloadBytes); err != nil {
		_ = lease.Release()
		return
	}
	if s.appender.TryPublishLease(parseLevel(level), lease) != minilogback.Accepted {
		_ = lease.Release()
	}
}

func fillDemoPayload(target []byte, level string, id uint64) {
	prefix := []byte(strings.ToUpper(level) + " demo lease=" + strconv.FormatUint(id, 10) + " ")
	copy(target, prefix)
	for index := len(prefix); index < len(target)-1; index++ {
		target[index] = 'x'
	}
	if len(target) > 0 {
		target[len(target)-1] = '\n'
	}
}

func parseLevel(level string) minilogback.Level {
	switch level {
	case "debug":
		return minilogback.DebugLevel
	case "warn":
		return minilogback.WarnLevel
	case "error":
		return minilogback.ErrorLevel
	default:
		return minilogback.InfoLevel
	}
}

func (s *runtimeState) TelemetrySnapshot() telemetry.RawSnapshot {
	stats := s.appender.Stats()
	collectorStats := s.collector.Stats()
	droppedByLevel := map[string]uint64{"debug": 0, "info": 0, "warn": 0, "error": 0}
	for level, value := range stats.ByLevel {
		droppedByLevel[strings.ToLower(level)] = value.Dropped
	}
	classes := make([]telemetry.PoolClass, len(stats.Pool.Classes))
	for i, class := range stats.Pool.Classes {
		classes[i] = telemetry.PoolClass{Size: class.Size, InUse: class.InUse, Available: 0}
	}
	totalGets := stats.Pool.PoolHits + stats.Pool.PoolMisses
	hitPercent := float64(0)
	if totalGets > 0 {
		hitPercent = float64(stats.Pool.PoolHits) * 100 / float64(totalGets)
	}
	sinkStatus := "ok"
	if stats.Flusher.Degraded {
		sinkStatus = "degraded"
	}
	collectorStatus := "listening"
	if s.closing.Load() {
		collectorStatus = "closing"
	}
	return telemetry.RawSnapshot{
		Ring: telemetry.RingMetrics{
			Capacity: stats.Ring.Capacity, Depth: stats.Ring.Depth, HighWatermark: stats.Ring.HighWater,
			PublishAttempts: stats.PublishAttempts, Accepted: stats.Accepted, Consumed: stats.Ring.Consumed,
			DroppedTotal:   stats.RejectedFull + stats.RejectedClosed + stats.RejectedOversized + stats.RejectedInvalid,
			DroppedByLevel: droppedByLevel,
		},
		Flusher: telemetry.FlusherMetrics{
			Batches: stats.Flusher.Batches, Records: stats.Flusher.Records, Bytes: stats.Flusher.Bytes,
			InFlight: stats.Flusher.InFlight, LastBatchSize: int(stats.Flusher.LastBatchSize),
			FlushP95Micros: s.appSink.p95Micros(), Errors: stats.Flusher.Errors, Mode: s.appSink.mode,
		},
		Pool: telemetry.PoolMetrics{
			BorrowedTotal: stats.Pool.Borrowed, ReturnedTotal: stats.Pool.Returned, Outstanding: stats.Pool.Outstanding,
			Overdue: stats.Pool.Overdue, DoubleReturns: stats.Pool.DoubleReturns, InvalidReturns: stats.Pool.InvalidReturns,
			HitPercent: hitPercent, Classes: classes,
		},
		Collector: telemetry.CollectorMetrics{
			Connections: collectorStats.Connections, AcceptedBatches: collectorStats.AcceptedBatches,
			DuplicateBatches: collectorStats.DuplicateBatches, InvalidFrames: collectorStats.InvalidFrames,
			Overloaded: collectorStats.Overloaded, SinkErrors: collectorStats.SinkErrors,
		},
		DemoMode: s.config.DemoMode, AuditMode: s.config.AuditMode,
		Status: telemetry.StatusMetrics{Sink: sinkStatus, Collector: collectorStatus, Transport: "sse"},
	}
}

func (s *runtimeState) Health() httpapi.Health {
	stats := s.appender.Stats()
	status, appenderStatus := "ok", strings.ToLower(stats.State)
	if s.closing.Load() {
		status, appenderStatus = "closing", "closing"
	}
	sinkStatus := "ok"
	if stats.Flusher.Degraded {
		sinkStatus = "degraded"
	}
	collectorStatus := "listening"
	if s.closing.Load() {
		collectorStatus = "closing"
	}
	return httpapi.Health{
		Status: status, Appender: appenderStatus, Sink: sinkStatus, Collector: collectorStatus,
		Version: s.config.Version, UptimeSeconds: int64(time.Since(s.started).Seconds()),
	}
}

func (s *runtimeState) ListLeases(_ context.Context, query httpapi.LeaseQuery) (httpapi.LeasePage, error) {
	filter := ""
	if query.State != "all" {
		filter = strings.ToUpper(query.State)
	}
	snapshots := s.appender.SnapshotLeasesBefore(filter, query.Cursor, query.Limit+1)
	leases := make([]httpapi.Lease, 0, len(snapshots))
	for _, snapshot := range snapshots {
		leases = append(leases, convertLease(snapshot, false))
	}
	hasMore := len(leases) > query.Limit
	if hasMore {
		leases = leases[:query.Limit]
	}
	nextCursor := ""
	if hasMore && len(leases) > 0 {
		nextCursor = strconv.FormatUint(leases[len(leases)-1].ID, 10)
	}
	return httpapi.LeasePage{Leases: leases, NextCursor: nextCursor, HasMore: hasMore}, nil
}

func (s *runtimeState) LeaseByID(_ context.Context, id uint64) (httpapi.Lease, error) {
	snapshot, ok := s.appender.LeaseByID(id)
	if !ok {
		return httpapi.Lease{}, httpapi.ErrNotFound
	}
	return convertLease(snapshot, true), nil
}

func convertLease(snapshot minilogback.LeaseSnapshot, includeStack bool) httpapi.Lease {
	frames := make([]httpapi.StackFrame, 0, len(snapshot.Stack))
	if includeStack {
		for _, frame := range snapshot.Stack {
			frames = append(frames, httpapi.StackFrame{Function: frame.Function, Source: frame.File, Line: frame.Line})
		}
	}
	return httpapi.Lease{
		ID: snapshot.ID, State: strings.ToLower(snapshot.State), SizeClass: snapshot.ClassSize, Length: snapshot.Length,
		BorrowedAt: snapshot.BorrowedAt, Deadline: snapshot.Deadline, AgeMillis: snapshot.AgeNanos / int64(time.Millisecond),
		Level: strings.ToLower(snapshot.Level), Source: fmt.Sprintf("%s:%d", snapshot.File, snapshot.Line),
		Function: snapshot.Function, Stack: frames,
	}
}

func (s *runtimeState) Close(ctx context.Context) error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	s.demoMu.Lock()
	if s.demoCancel != nil {
		s.demoCancel()
	}
	for id, lease := range s.demoLeases {
		_ = lease.Release()
		delete(s.demoLeases, id)
	}
	s.demoMu.Unlock()
	s.demoWG.Wait()
	collectorErr := s.collector.Close()
	appenderErr := s.appender.Close(ctx)
	fileErr := s.localFile.Close()
	return errors.Join(collectorErr, appenderErr, fileErr)
}
