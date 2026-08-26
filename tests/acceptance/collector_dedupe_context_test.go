package acceptance

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/collector"
	"github.com/xavskye/minilogback/internal/protocol"
)

type heldBatchSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

type pipeAddress struct{}

func newPipeListener() *pipeListener {
	return &pipeListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (*pipeListener) Addr() net.Addr { return pipeAddress{} }
func (pipeAddress) Network() string  { return "pipe" }
func (pipeAddress) String() string   { return "pipe" }
func (l *pipeListener) Dial(ctx context.Context, _, _ string) (net.Conn, error) {
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

func newHeldBatchSink() *heldBatchSink {
	return &heldBatchSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *heldBatchSink) WriteBatch(context.Context, [][]byte) error {
	if s.calls.Add(1) == 1 {
		close(s.entered)
		<-s.release
	}
	return nil
}

func (s *heldBatchSink) unblock() {
	s.once.Do(func() { close(s.release) })
}

func TestDuplicateBatchWaitHonorsSinkTimeout(t *testing.T) {
	listener := newPipeListener()
	sink := newHeldBatchSink()
	config := collector.DefaultConfig()
	config.SinkTimeout = 20 * time.Millisecond
	server, err := collector.New(config, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()

	clientConfig := protocol.ClientConfig{
		Address:      "pipe",
		ClientID:     42,
		IOTimeout:    time.Second,
		RetryInitial: time.Millisecond,
		RetryMaximum: time.Millisecond,
		MaxAttempts:  1,
		DialContext:  listener.Dial,
	}
	first, err := protocol.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	second, err := protocol.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		sink.unblock()
		_ = first.Close()
		_ = second.Close()
		_ = server.Close()
		if serveErr := <-serveResult; serveErr != nil {
			t.Errorf("Serve error = %v", serveErr)
		}
	}()

	firstResult := make(chan error, 1)
	go func() { firstResult <- first.Send(context.Background(), [][]byte{[]byte("same-batch")}) }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("first batch did not reach sink")
	}

	secondResult := make(chan error, 1)
	go func() { secondResult <- second.Send(context.Background(), [][]byte{[]byte("same-batch")}) }()
	select {
	case sendErr := <-secondResult:
		var remoteErr *protocol.RemoteError
		if !errors.As(sendErr, &remoteErr) || remoteErr.Status != protocol.StatusSinkError {
			t.Fatalf("duplicate Send error = %v; want sink-error ACK after SinkTimeout", sendErr)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("duplicate Send remained blocked after SinkTimeout")
	}

	sink.unblock()
	select {
	case sendErr := <-firstResult:
		if sendErr != nil {
			t.Fatalf("first Send error = %v", sendErr)
		}
	case <-time.After(time.Second):
		t.Fatal("first Send did not finish after sink was released")
	}
}
