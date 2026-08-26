package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/xavskye/minilogback/internal/protocol"
)

type options struct {
	address      string
	rate         int
	duration     time.Duration
	payloadBytes int
	batchSize    int
	timeout      time.Duration
}

func main() {
	options := parseFlags()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, options); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var value options
	flag.StringVar(&value.address, "addr", "127.0.0.1:28641", "collector host:port")
	flag.IntVar(&value.rate, "rate", 10000, "target records per second")
	flag.DurationVar(&value.duration, "duration", 10*time.Second, "generation duration")
	flag.IntVar(&value.payloadBytes, "payload-bytes", 256, "encoded record size")
	flag.IntVar(&value.batchSize, "batch-size", 1024, "records per batch")
	flag.DurationVar(&value.timeout, "timeout", 5*time.Second, "dial/write/ack timeout")
	flag.Parse()
	return value
}

func run(ctx context.Context, options options) error {
	if options.rate < 1 || options.duration <= 0 || options.payloadBytes < 32 || options.payloadBytes > 1<<20 || options.batchSize < 1 || options.batchSize > 1024 || options.timeout <= 0 {
		return errors.New("invalid rate, duration, payload size, batch size, or timeout")
	}
	clientID, err := randomID()
	if err != nil {
		return err
	}
	started := time.Now()
	deadline := started.Add(options.duration)
	interval := time.Duration(float64(time.Second) * float64(options.batchSize) / float64(options.rate))
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var connection net.Conn
	defer func() {
		if connection != nil {
			_ = connection.Close()
		}
	}()
	var batchID, sent, bytesSent uint64
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if connection == nil {
			connection, err = net.DialTimeout("tcp", options.address, options.timeout)
			if err != nil {
				return fmt.Errorf("dial collector: %w", err)
			}
		}
		batchID++
		records := makeRecords(options.batchSize, options.payloadBytes, sent)
		encoded, err := protocol.NewEncodedBatch(clientID, batchID, time.Now(), records, protocol.DefaultLimits())
		if err != nil {
			return err
		}
		_ = connection.SetDeadline(time.Now().Add(options.timeout))
		written, err := encoded.WriteTo(connection)
		if err != nil {
			_ = connection.Close()
			connection = nil
			return fmt.Errorf("write batch %d: %w", batchID, err)
		}
		ack, err := protocol.ReadAck(connection)
		if err != nil {
			_ = connection.Close()
			connection = nil
			return fmt.Errorf("read ack %d: %w", batchID, err)
		}
		if ack.ClientID != clientID || ack.BatchID != batchID || (ack.Status != protocol.StatusAccepted && ack.Status != protocol.StatusDuplicate) {
			return fmt.Errorf("unexpected ack for batch %d: %+v", batchID, ack)
		}
		sent += uint64(len(records))
		bytesSent += uint64(written)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"client_id": clientID, "batches": batchID, "records": sent, "wire_bytes": bytesSent,
		"elapsed": time.Since(started).String(), "target_rate": options.rate,
	})
}

func randomID() (uint64, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, fmt.Errorf("generate client id: %w", err)
	}
	id := binary.BigEndian.Uint64(raw[:])
	if id == 0 {
		id = 1
	}
	return id, nil
}

func makeRecords(count, size int, sequence uint64) [][]byte {
	records := make([][]byte, count)
	for index := range records {
		prefix := []byte("INFO sequence=" + strconv.FormatUint(sequence+uint64(index)+1, 10) + " ")
		record := make([]byte, size)
		copy(record, prefix)
		for cursor := len(prefix); cursor < len(record)-1; cursor++ {
			record[cursor] = 'x'
		}
		record[len(record)-1] = '\n'
		records[index] = record
	}
	return records
}
