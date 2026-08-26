package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func TestBatchRoundTrip(t *testing.T) {
	records := [][]byte{[]byte("first\n"), []byte("second\n")}
	encoded, err := NewEncodedBatch(7, 11, time.Unix(1, 2), records, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if _, err := encoded.WriteTo(&wire); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadBatch(&wire, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header.ClientID != 7 || decoded.Header.BatchID != 11 || len(decoded.Records) != 2 {
		t.Fatalf("unexpected header or count: %+v", decoded.Header)
	}
	for i := range records {
		if !bytes.Equal(decoded.Records[i], records[i]) {
			t.Fatalf("record %d mismatch", i)
		}
	}
}

func TestReadBatchRejectsBoundariesAndCRC(t *testing.T) {
	valid, err := NewEncodedBatch(1, 1, time.Unix(1, 0), [][]byte{[]byte("x")}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	_, _ = valid.WriteTo(&buf)
	wire := buf.Bytes()

	tests := map[string]func([]byte){
		"magic":   func(b []byte) { b[0] = 0 },
		"version": func(b []byte) { b[4] = 2 },
		"flags":   func(b []byte) { b[5] = 1 },
		"length":  func(b []byte) { binary.BigEndian.PutUint16(b[6:8], 47) },
		"count":   func(b []byte) { binary.BigEndian.PutUint32(b[24:28], 0) },
		"crc":     func(b []byte) { b[len(b)-1] ^= 0xff },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := append([]byte(nil), wire...)
			mutate(candidate)
			if _, err := ReadBatch(bytes.NewReader(candidate), DefaultLimits()); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("expected invalid frame, got %v", err)
			}
		})
	}

	if _, err := ReadBatch(bytes.NewReader(wire[:len(wire)-1]), DefaultLimits()); !errors.Is(err, ErrTruncated) {
		t.Fatalf("expected truncated frame, got %v", err)
	}
	if _, err := ReadBatch(bytes.NewReader(nil), DefaultLimits()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean EOF, got %v", err)
	}
}

func TestAckRoundTripAndMismatchFields(t *testing.T) {
	wire, err := (Ack{Status: StatusDuplicate, ClientID: 9, BatchID: 10}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	ack, err := ReadAck(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != StatusDuplicate || ack.ClientID != 9 || ack.BatchID != 10 {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	wire[5] = 99
	if _, err := ReadAck(bytes.NewReader(wire)); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("expected invalid ack, got %v", err)
	}
}

type oneByteWriter struct{ bytes.Buffer }

func (w *oneByteWriter) Write(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return w.Buffer.Write(p)
}

func TestPartialWrites(t *testing.T) {
	encoded, err := NewEncodedBatch(1, 2, time.Unix(1, 0), [][]byte{[]byte("abcdef")}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var writer oneByteWriter
	if _, err := encoded.WriteTo(&writer); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBatch(bytes.NewReader(writer.Bytes()), DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	writer.Reset()
	if err := WriteAck(&writer, Ack{Status: StatusAccepted, ClientID: 1, BatchID: 2}); err != nil {
		t.Fatal(err)
	}
	if writer.Len() != AckSize {
		t.Fatalf("ack size=%d", writer.Len())
	}
}

func FuzzReadBatch(f *testing.F) {
	encoded, _ := NewEncodedBatch(1, 1, time.Unix(1, 0), [][]byte{[]byte("seed")}, DefaultLimits())
	var seed bytes.Buffer
	_, _ = encoded.WriteTo(&seed)
	f.Add(seed.Bytes())
	f.Add([]byte("MLBK"))
	f.Fuzz(func(t *testing.T, data []byte) {
		limits := Limits{MaxRecords: 16, MaxPayload: 4096, MaxEventBytes: 1024}
		_, _ = ReadBatch(bytes.NewReader(data), limits)
	})
}
