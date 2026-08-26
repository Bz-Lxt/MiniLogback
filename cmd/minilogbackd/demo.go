package main

import (
	"context"
	"errors"
	"time"

	"github.com/xavskye/minilogback/internal/httpapi"
)

func (s *runtimeState) StartTraffic(_ context.Context, request httpapi.DemoTrafficRequest) (httpapi.DemoTrafficStatus, error) {
	s.demoMu.Lock()
	if s.closing.Load() {
		s.demoMu.Unlock()
		return httpapi.DemoTrafficStatus{}, errors.New("service is closing")
	}
	// Cancel any in-flight traffic generator before starting the new one. A
	// new context is created unconditionally so the replacement cannot be
	// cancelled by the prior cancel function.
	ctx, cancel := context.WithTimeout(s.rootContext, time.Duration(request.DurationSeconds)*time.Second)
	previous := s.demoCancel
	s.demoCancel = cancel
	s.demoWG.Add(1)
	s.demoMu.Unlock()
	if previous != nil {
		previous()
	}
	go s.generateTraffic(ctx, request)
	return httpapi.DemoTrafficStatus{Status: "started", EventsPerSecond: request.EventsPerSecond, DurationSeconds: request.DurationSeconds}, nil
}

func (s *runtimeState) generateTraffic(ctx context.Context, request httpapi.DemoTrafficRequest) {
	defer s.demoWG.Done()
	rate := request.EventsPerSecond
	burst := 1
	interval := time.Second / time.Duration(rate)
	if rate >= 100 {
		interval = 10 * time.Millisecond
		burst = rate / 100
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for index := 0; index < burst; index++ {
				s.publish("info", request.PayloadBytes)
			}
		}
	}
}

func (s *runtimeState) RetainLease(_ context.Context, request httpapi.DemoLeaseRequest) (httpapi.Lease, error) {
	if s.closing.Load() {
		return httpapi.Lease{}, errors.New("service is closing")
	}
	lease, err := s.appender.AcquireFor(parseLevel(request.Level), request.SizeBytes)
	if err != nil {
		return httpapi.Lease{}, err
	}
	writable, err := lease.Buffer()
	if err != nil {
		_ = lease.Release()
		return httpapi.Lease{}, err
	}
	fillDemoPayload(writable[:request.SizeBytes], request.Level, lease.ID())
	if err := lease.Commit(request.SizeBytes); err != nil {
		_ = lease.Release()
		return httpapi.Lease{}, err
	}
	s.demoMu.Lock()
	if s.closing.Load() {
		s.demoMu.Unlock()
		_ = lease.Release()
		return httpapi.Lease{}, errors.New("service is closing")
	}
	s.demoLeases[lease.ID()] = lease
	s.demoMu.Unlock()
	if snapshot, ok := s.appender.LeaseByID(lease.ID()); ok {
		return convertLease(snapshot, false), nil
	}
	return httpapi.Lease{ID: lease.ID(), State: "borrowed", SizeClass: lease.ClassSize(), Length: lease.Len(), Level: request.Level}, nil
}

func (s *runtimeState) ReleaseLease(_ context.Context, id uint64) error {
	s.demoMu.Lock()
	lease := s.demoLeases[id]
	if lease != nil {
		delete(s.demoLeases, id)
	}
	s.demoMu.Unlock()
	if lease == nil {
		return httpapi.ErrNotFound
	}
	if err := lease.Release(); err != nil {
		return err
	}
	return nil
}
