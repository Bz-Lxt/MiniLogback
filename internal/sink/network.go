package sink

import (
	"context"
	"errors"
)

// AcknowledgingTransport is implemented by the versioned protocol transport.
// Send returns nil only after a matching downstream ACK, allowing this package
// to remain independent from the wire codec.
type AcknowledgingTransport interface {
	Send(context.Context, [][]byte) error
	Close() error
}

type NetworkAdapter struct {
	transport AcknowledgingTransport
}

func NewNetworkAdapter(transport AcknowledgingTransport) *NetworkAdapter {
	return &NetworkAdapter{transport: transport}
}

func (s *NetworkAdapter) WriteBatch(ctx context.Context, records [][]byte) error {
	if s == nil || s.transport == nil {
		return errors.New("network transport is nil")
	}
	return s.transport.Send(ctx, records)
}

func (s *NetworkAdapter) Flush(context.Context) error { return nil }

func (s *NetworkAdapter) Close() error {
	if s == nil || s.transport == nil {
		return nil
	}
	return s.transport.Close()
}

func (s *NetworkAdapter) Capabilities() Capabilities {
	return Capabilities{BatchMode: "scatter_gather", CopyAvoiding: true, Acknowledged: true}
}
