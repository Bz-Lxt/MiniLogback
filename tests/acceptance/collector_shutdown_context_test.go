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
)

type shutdownContextSink struct {
	entered     chan struct{}
	release     chan struct{}
	result      chan error
	enteredOnce sync.Once
	releaseOnce sync.Once
}

type singleConnectionListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

func newSingleConnectionListener(connection net.Conn) *singleConnectionListener {
	connections := make(chan net.Conn, 1)
	connections <- connection
	return &singleConnectionListener{connections: connections, closed: make(chan struct{})}
}

func (l *singleConnectionListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *singleConnectionListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (*singleConnectionListener) Addr() net.Addr { return pipeAddress("collector") }

type pipeAddress string

func (a pipeAddress) Network() string { return "pipe" }
func (a pipeAddress) String() string  { return string(a) }

func newShutdownContextSink() *shutdownContextSink {
	return &shutdownContextSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		result:  make(chan error, 1),
	}
}

func (s *shutdownContextSink) WriteBatch(ctx context.Context, _ [][]byte) error {
	s.enteredOnce.Do(func() { close(s.entered) })
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-s.release:
		err = errors.New("sink released without context cancellation")
	}
	s.result <- err
	return err
}

func (s *shutdownContextSink) unblock() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func TestServerCloseCancelsInFlightSinkWrite(t *testing.T) {
	sink := newShutdownContextSink()
	config := collector.DefaultConfig()
	config.SinkTimeout = time.Minute
	server, err := collector.New(config, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		sink.unblock()
		_ = server.Close()
	}()

	connection, service := net.Pipe()
	listener := newSingleConnectionListener(service)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	defer connection.Close()
	batch, err := protocol.NewEncodedBatch(19, 23, time.Now(), [][]byte{[]byte("shutdown")}, protocol.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.WriteTo(connection); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("collector did not start the sink write")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- server.Close() }()

	select {
	case sinkErr := <-sink.result:
		if !errors.Is(sinkErr, context.Canceled) {
			t.Fatalf("sink write error = %v; want context.Canceled", sinkErr)
		}
	case <-time.After(500 * time.Millisecond):
		sink.unblock()
		sinkErr := <-sink.result
		<-closeDone
		t.Fatalf("Server.Close did not cancel the in-flight sink write; sink returned only after test release: %v", sinkErr)
	}

	select {
	case closeErr := <-closeDone:
		if closeErr != nil {
			t.Fatalf("Server.Close error = %v", closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not finish after the sink observed cancellation")
	}
	select {
	case serveErr := <-serveDone:
		if serveErr != nil {
			t.Fatalf("Server.Serve error = %v", serveErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Server.Serve did not return after close")
	}
}
