package sink

import (
	"context"
	"errors"
	"testing"
)

type fakeTransport struct {
	records int
	closed  int
	err     error
}

func (t *fakeTransport) Send(_ context.Context, records [][]byte) error {
	t.records += len(records)
	return t.err
}

func (t *fakeTransport) Close() error { t.closed++; return t.err }

func TestNetworkAdapterDelegatesToAcknowledgedTransport(t *testing.T) {
	transport := &fakeTransport{}
	s := NewNetworkAdapter(transport)
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if transport.records != 2 || transport.closed != 1 {
		t.Fatalf("transport calls records=%d closed=%d", transport.records, transport.closed)
	}
	capability := s.Capabilities()
	if !capability.CopyAvoiding || !capability.Acknowledged || capability.BatchMode != "scatter_gather" {
		t.Fatalf("capability = %+v", capability)
	}
}

func TestNetworkAdapterValidatesTransportAndPropagatesErrors(t *testing.T) {
	nilAdapter := NewNetworkAdapter(nil)
	if err := nilAdapter.WriteBatch(context.Background(), nil); err == nil {
		t.Fatal("nil transport was accepted")
	}
	if err := nilAdapter.Close(); err != nil {
		t.Fatal(err)
	}
	want := errors.New("transport failed")
	s := NewNetworkAdapter(&fakeTransport{err: want})
	if err := s.WriteBatch(context.Background(), nil); !errors.Is(err, want) {
		t.Fatalf("WriteBatch error = %v", err)
	}
	if err := s.Close(); !errors.Is(err, want) {
		t.Fatalf("Close error = %v", err)
	}
}
