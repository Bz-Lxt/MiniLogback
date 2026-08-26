package sink_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/protocol"
	"github.com/xavskye/minilogback/internal/sink"
)

func TestNetworkAdapterDoesNotRetryInvalidAcknowledgement(t *testing.T) {
	const maxAttempts = 3
	var attempts atomic.Int32
	var servers sync.WaitGroup
	serverErrors := make(chan error, maxAttempts)
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		servers.Add(1)
		go func() {
			defer servers.Done()
			defer server.Close()
			batch, err := protocol.ReadBatch(server, protocol.DefaultLimits())
			if err != nil {
				serverErrors <- fmt.Errorf("read batch: %w", err)
				return
			}
			attempts.Add(1)
			if err := protocol.WriteAck(server, protocol.Ack{
				Status:   protocol.StatusInvalid,
				ClientID: batch.Header.ClientID,
				BatchID:  batch.Header.BatchID,
			}); err != nil {
				serverErrors <- fmt.Errorf("write acknowledgement: %w", err)
			}
		}()
		return client, nil
	}

	client, err := protocol.NewClient(protocol.ClientConfig{
		Address:      "collector",
		ClientID:     71,
		DialTimeout:  time.Second,
		IOTimeout:    time.Second,
		RetryInitial: time.Microsecond,
		RetryMaximum: time.Microsecond,
		MaxAttempts:  maxAttempts,
		DialContext:  dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter := sink.NewNetworkAdapter(client)

	writeErr := adapter.WriteBatch(context.Background(), [][]byte{[]byte("malformed upstream event")})
	if writeErr == nil {
		t.Fatal("expected the collector rejection to be returned")
	}
	var remoteErr *protocol.RemoteError
	if !errors.As(writeErr, &remoteErr) || remoteErr.Status != protocol.StatusInvalid {
		t.Errorf("WriteBatch error = %v; want invalid remote acknowledgement", writeErr)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("invalid acknowledgement was sent %d times; want 1", got)
	}
	_ = adapter.Close()
	servers.Wait()
	close(serverErrors)
	for serverErr := range serverErrors {
		t.Error(serverErr)
	}
}
