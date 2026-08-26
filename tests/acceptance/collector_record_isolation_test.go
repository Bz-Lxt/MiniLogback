package acceptance_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/collector"
	"github.com/xavskye/minilogback/internal/protocol"
)

type decoratingCollectorSink struct {
	received chan [][]byte
}

func (s *decoratingCollectorSink) WriteBatch(_ context.Context, records [][]byte) error {
	records[0] = append(records[0], " [ok]"...)
	copied := make([][]byte, len(records))
	for index := range records {
		copied[index] = append([]byte(nil), records[index]...)
	}
	s.received <- copied
	return nil
}

type collectorPipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

func newCollectorPipeListener() *collectorPipeListener {
	return &collectorPipeListener{connections: make(chan net.Conn, 1), closed: make(chan struct{})}
}

func (l *collectorPipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *collectorPipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *collectorPipeListener) Addr() net.Addr { return collectorPipeAddress{} }

func (l *collectorPipeListener) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	client, service := net.Pipe()
	select {
	case l.connections <- service:
		return client, nil
	case <-ctx.Done():
		_ = client.Close()
		_ = service.Close()
		return nil, ctx.Err()
	case <-l.closed:
		_ = client.Close()
		_ = service.Close()
		return nil, net.ErrClosed
	}
}

type collectorPipeAddress struct{}

func (collectorPipeAddress) Network() string { return "pipe" }
func (collectorPipeAddress) String() string  { return "collector-pipe" }

func TestCollectorSinkAppendDoesNotCorruptFollowingRecord(t *testing.T) {
	listener := newCollectorPipeListener()
	sink := &decoratingCollectorSink{received: make(chan [][]byte, 1)}
	config := collector.DefaultConfig()
	server, err := collector.New(config, sink, nil)
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
		Address:     "collector-pipe",
		ClientID:    91,
		MaxAttempts: 1,
		DialContext: listener.DialContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Send(ctx, [][]byte{[]byte("alpha"), []byte("bravo")}); err != nil {
		t.Fatal(err)
	}
	records := <-sink.received
	if got, want := string(records[0]), "alpha [ok]"; got != want {
		t.Fatalf("decorated first record = %q; want %q", got, want)
	}
	if got, want := string(records[1]), "bravo"; got != want {
		t.Fatalf("second record changed while decorating first: got %q, want %q", got, want)
	}
}
