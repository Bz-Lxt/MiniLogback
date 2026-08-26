package collector

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/protocol"
)

type testSink struct {
	calls   atomic.Uint64
	release <-chan struct{}
}

func (s *testSink) WriteBatch(context.Context, [][]byte) error {
	if s.release != nil {
		<-s.release
	}
	s.calls.Add(1)
	return nil
}

func TestCollectorACKAfterSinkAndDeduplicates(t *testing.T) {
	release := make(chan struct{})
	sink := &testSink{release: release}
	cfg := DefaultConfig()
	cfg.Address = "127.0.0.1:0"
	server, err := New(cfg, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	send := func(expect protocol.AckStatus, block bool) {
		client, service := net.Pipe()
		server.sema <- struct{}{}
		server.mu.Lock()
		server.active[service] = struct{}{}
		server.mu.Unlock()
		server.stats.connections.Add(1)
		server.wg.Add(1)
		go server.handle(service)
		defer client.Close()
		batch, _ := protocol.NewEncodedBatch(4, 8, time.Unix(1, 0), [][]byte{[]byte("record")}, protocol.DefaultLimits())
		writeDone := make(chan error, 1)
		go func() { _, err := batch.WriteTo(client); writeDone <- err }()
		if err := <-writeDone; err != nil {
			t.Fatal(err)
		}
		if block {
			_ = client.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
			if _, err := protocol.ReadAck(client); err == nil {
				t.Fatal("ack arrived before sink completion")
			}
			_ = client.SetReadDeadline(time.Time{})
			close(release)
		}
		ack, err := protocol.ReadAck(client)
		if err != nil {
			t.Fatal(err)
		}
		if ack.Status != expect {
			t.Fatalf("status=%d want=%d", ack.Status, expect)
		}
	}
	send(protocol.StatusAccepted, true)
	send(protocol.StatusDuplicate, false)
	if sink.calls.Load() != 1 {
		t.Fatalf("sink calls=%d", sink.calls.Load())
	}
}

func TestDedupeEvictsLeastRecent(t *testing.T) {
	dedupe := NewDedupe(1)
	work := 0
	for _, key := range []BatchKey{{1, 1}, {1, 2}, {1, 1}} {
		_, err := dedupe.Do(context.Background(), key, func() error { work++; return nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	if work != 3 || dedupe.Len() != 1 {
		t.Fatalf("work=%d len=%d", work, dedupe.Len())
	}
}

func TestCloseUnblocksIdleConnections(t *testing.T) {
	server, err := New(DefaultConfig(), &testSink{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, service := net.Pipe()
	defer client.Close()
	server.sema <- struct{}{}
	server.mu.Lock()
	server.active[service] = struct{}{}
	server.mu.Unlock()
	server.stats.connections.Add(1)
	server.wg.Add(1)
	go server.handle(service)
	done := make(chan error, 1)
	go func() { done <- server.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock idle connection")
	}
}
