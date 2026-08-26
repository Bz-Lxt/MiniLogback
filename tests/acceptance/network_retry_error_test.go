package acceptance_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/collector"
	"github.com/xavskye/minilogback/internal/protocol"
	"github.com/xavskye/minilogback/internal/sink"
)

type failOnceCollectorSink struct {
	mu      sync.Mutex
	calls   int
	records [][]byte
}

func (s *failOnceCollectorSink) WriteBatch(ctx context.Context, records [][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return errors.New("temporary storage failure")
	}
	for _, record := range records {
		s.records = append(s.records, append([]byte(nil), record...))
	}
	return nil
}

func (s *failOnceCollectorSink) snapshot() (int, [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([][]byte, len(s.records))
	for index, record := range s.records {
		records[index] = append([]byte(nil), record...)
	}
	return s.calls, records
}

type pipeCollectorListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

func newPipeCollectorListener() *pipeCollectorListener {
	return &pipeCollectorListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *pipeCollectorListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeCollectorListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (*pipeCollectorListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
}

func (l *pipeCollectorListener) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.connections <- server:
		return client, nil
	case <-l.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	case <-ctx.Done():
		_ = client.Close()
		_ = server.Close()
		return nil, ctx.Err()
	}
}

func TestNetworkSinkRetriesBatchAfterCollectorSinkError(t *testing.T) {
	listener := newPipeCollectorListener()
	storage := &failOnceCollectorSink{}
	config := collector.DefaultConfig()
	config.SinkTimeout = time.Second
	server, err := collector.New(config, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close collector: %v", err)
		}
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve collector: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("collector did not stop")
		}
	})

	client, err := protocol.NewClient(protocol.ClientConfig{
		Address:      "pipe-collector",
		ClientID:     91,
		DialTimeout:  time.Second,
		IOTimeout:    time.Second,
		RetryInitial: time.Millisecond,
		RetryMaximum: time.Millisecond,
		MaxAttempts:  2,
		DialContext:  listener.DialContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close network client: %v", err)
		}
	})
	networkSink := sink.NewNetworkAdapter(client)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := networkSink.WriteBatch(ctx, [][]byte{[]byte("persist after retry")}); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	calls, records := storage.snapshot()
	if calls != 2 {
		t.Errorf("storage calls=%d, want 2 so the failed attempt is retried", calls)
	}
	if len(records) != 1 || string(records[0]) != "persist after retry" {
		t.Errorf("stored records=%q, want the retried batch", records)
	}
	stats := server.Stats()
	if stats.SinkErrors != 1 || stats.AcceptedBatches != 1 || stats.DuplicateBatches != 0 {
		t.Errorf("collector stats=%+v, want one sink error followed by one accepted retry", stats)
	}
}
