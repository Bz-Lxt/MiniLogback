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

type contextCapturingSink struct {
	contexts chan context.Context
}

func (s *contextCapturingSink) WriteBatch(ctx context.Context, _ [][]byte) error {
	s.contexts <- ctx
	return nil
}

type pipeListener struct {
	connection net.Conn
	closed     chan struct{}
	closeOnce  sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	if l.connection != nil {
		connection := l.connection
		l.connection = nil
		return connection, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (*pipeListener) Addr() net.Addr { return pipeAddress{} }

type pipeAddress struct{}

func (pipeAddress) Network() string { return "pipe" }
func (pipeAddress) String() string  { return "pipe" }

func TestCollectorReleasesSuccessfulBatchContextBeforeNextRequest(t *testing.T) {
	connection, service := net.Pipe()
	listener := &pipeListener{connection: service, closed: make(chan struct{})}
	sink := &contextCapturingSink{contexts: make(chan context.Context, 4)}
	config := collector.DefaultConfig()
	config.SinkTimeout = time.Minute
	server, err := collector.New(config, sink, nil)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	t.Cleanup(func() {
		_ = connection.Close()
		_ = listener.Close()
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
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	for batchID := uint64(1); batchID <= 3; batchID++ {
		batch, err := protocol.NewEncodedBatch(41, batchID, time.Now(), [][]byte{[]byte("record")}, protocol.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.WriteTo(connection); err != nil {
			t.Fatal(err)
		}
		ack, err := protocol.ReadAck(connection)
		if err != nil {
			t.Fatal(err)
		}
		if ack.Status != protocol.StatusAccepted {
			t.Fatalf("batch %d ACK status = %d", batchID, ack.Status)
		}
		var batchContext context.Context
		select {
		case batchContext = <-sink.contexts:
		case <-time.After(time.Second):
			t.Fatalf("batch %d did not reach sink", batchID)
		}
		select {
		case <-batchContext.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("batch %d was acknowledged but its sink context is still active", batchID)
		}
	}
}
