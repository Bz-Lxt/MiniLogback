package protocol

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientRetriesRetryableACKWithSameBatchID(t *testing.T) {
	var attempts atomic.Int32
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			batch, err := ReadBatch(server, DefaultLimits())
			if err != nil {
				return
			}
			status := StatusSinkError
			if attempts.Add(1) == 2 {
				status = StatusAccepted
			}
			_ = WriteAck(server, Ack{Status: status, ClientID: batch.Header.ClientID, BatchID: batch.Header.BatchID})
		}()
		return client, nil
	}
	client, err := NewClient(ClientConfig{Address: "pipe", ClientID: 5, MaxAttempts: 2, RetryInitial: time.Microsecond, RetryMaximum: time.Microsecond, DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(context.Background(), [][]byte{[]byte("record")}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}
