package collector_test

import (
	"context"
	"net"
	"testing"

	"github.com/xavskye/minilogback/internal/collector"
)

type acceptingSink struct{}

func (acceptingSink) WriteBatch(context.Context, [][]byte) error { return nil }

type closedListener struct {
	address net.Addr
}

func (l *closedListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l *closedListener) Close() error              { return nil }
func (l *closedListener) Addr() net.Addr            { return l.address }

func TestCollectorDefaultLoggerServesNonLoopbackListener(t *testing.T) {
	server, err := collector.New(collector.DefaultConfig(), acceptingSink{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener := &closedListener{address: &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 28641}}
	if err := server.Serve(listener); err != nil {
		t.Fatalf("Serve returned %v", err)
	}
}
