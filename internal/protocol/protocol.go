package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"
)

const (
	Version         = 1
	BatchHeaderSize = 48
	AckSize         = 24

	StatusAccepted   AckStatus = 0
	StatusDuplicate  AckStatus = 1
	StatusInvalid    AckStatus = 2
	StatusOverloaded AckStatus = 3
	StatusSinkError  AckStatus = 4
)

var (
	batchMagic = [4]byte{'M', 'L', 'B', 'K'}
	ackMagic   = [4]byte{'M', 'L', 'A', 'K'}

	ErrInvalidFrame = errors.New("invalid protocol frame")
	ErrTruncated    = errors.New("truncated protocol frame")
)

type AckStatus uint8

func (s AckStatus) Valid() bool { return s <= StatusSinkError }

type Limits struct {
	MaxRecords    uint32
	MaxPayload    uint32
	MaxEventBytes uint32
}

func DefaultLimits() Limits {
	return Limits{MaxRecords: 1024, MaxPayload: 16 << 20, MaxEventBytes: 1 << 20}
}

func (l Limits) Validate() error {
	if l.MaxRecords == 0 || l.MaxPayload == 0 || l.MaxEventBytes == 0 {
		return fmt.Errorf("%w: protocol limits must be positive", ErrInvalidFrame)
	}
	if l.MaxRecords > 1<<16 || l.MaxPayload > 64<<20 || l.MaxEventBytes > 16<<20 || l.MaxEventBytes > l.MaxPayload {
		return fmt.Errorf("%w: protocol limits exceed safe implementation bounds", ErrInvalidFrame)
	}
	return nil
}

type BatchHeader struct {
	ClientID     uint64
	BatchID      uint64
	RecordCount  uint32
	PayloadBytes uint32
	PayloadCRC32 uint32
	SentUnixNano uint64
}

type Batch struct {
	Header  BatchHeader
	Records [][]byte
	payload []byte
}

type EncodedBatch struct {
	Header   [BatchHeaderSize]byte
	Prefixes [][]byte
	Records  [][]byte
}

func (b *EncodedBatch) ClientID() uint64 { return binary.BigEndian.Uint64(b.Header[8:16]) }
func (b *EncodedBatch) BatchID() uint64  { return binary.BigEndian.Uint64(b.Header[16:24]) }

// Matches verifies that an outer retry still represents the exact logical
// payload captured by this batch. It deliberately recomputes the checksum so a
// retained backing slice modified after the first attempt cannot inherit the
// old identity and checksum.
func (b *EncodedBatch) Matches(records [][]byte) bool {
	if b == nil || len(records) != len(b.Records) {
		return false
	}
	hash := crc32.NewIEEE()
	var payloadBytes uint64
	var prefix [4]byte
	for index, record := range records {
		if len(record) != len(b.Records[index]) {
			return false
		}
		binary.BigEndian.PutUint32(prefix[:], uint32(len(record)))
		payloadBytes += 4 + uint64(len(record))
		_, _ = hash.Write(prefix[:])
		_, _ = hash.Write(record)
	}
	return payloadBytes == uint64(binary.BigEndian.Uint32(b.Header[28:32])) &&
		hash.Sum32() == binary.BigEndian.Uint32(b.Header[32:36])
}

func NewEncodedBatch(clientID, batchID uint64, sentAt time.Time, records [][]byte, limits Limits) (*EncodedBatch, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if clientID == 0 || batchID == 0 {
		return nil, fmt.Errorf("%w: client_id and batch_id must be non-zero", ErrInvalidFrame)
	}
	if len(records) == 0 || uint64(len(records)) > uint64(limits.MaxRecords) {
		return nil, fmt.Errorf("%w: record_count out of range", ErrInvalidFrame)
	}
	if sentAt.IsZero() || sentAt.UnixNano() <= 0 {
		return nil, fmt.Errorf("%w: sent_at must be positive", ErrInvalidFrame)
	}

	encoded := &EncodedBatch{Records: append([][]byte(nil), records...), Prefixes: make([][]byte, len(records))}
	hash := crc32.NewIEEE()
	var payloadBytes uint64
	for i, record := range records {
		if len(record) == 0 || uint64(len(record)) > uint64(limits.MaxEventBytes) {
			return nil, fmt.Errorf("%w: record %d length out of range", ErrInvalidFrame, i)
		}
		payloadBytes += 4 + uint64(len(record))
		if payloadBytes > uint64(limits.MaxPayload) {
			return nil, fmt.Errorf("%w: payload exceeds configured maximum", ErrInvalidFrame)
		}
		prefix := make([]byte, 4)
		binary.BigEndian.PutUint32(prefix, uint32(len(record)))
		encoded.Prefixes[i] = prefix
		_, _ = hash.Write(prefix)
		_, _ = hash.Write(record)
	}

	copy(encoded.Header[0:4], batchMagic[:])
	encoded.Header[4] = Version
	encoded.Header[5] = 0
	binary.BigEndian.PutUint16(encoded.Header[6:8], BatchHeaderSize)
	binary.BigEndian.PutUint64(encoded.Header[8:16], clientID)
	binary.BigEndian.PutUint64(encoded.Header[16:24], batchID)
	binary.BigEndian.PutUint32(encoded.Header[24:28], uint32(len(records)))
	binary.BigEndian.PutUint32(encoded.Header[28:32], uint32(payloadBytes))
	binary.BigEndian.PutUint32(encoded.Header[32:36], hash.Sum32())
	binary.BigEndian.PutUint32(encoded.Header[36:40], 0)
	binary.BigEndian.PutUint64(encoded.Header[40:48], uint64(sentAt.UnixNano()))
	return encoded, nil
}

func (b *EncodedBatch) Buffers() net.Buffers {
	buffers := make(net.Buffers, 0, 1+2*len(b.Records))
	buffers = append(buffers, b.Header[:])
	for i := range b.Records {
		buffers = append(buffers, b.Prefixes[i], b.Records[i])
	}
	return buffers
}

func (b *EncodedBatch) WriteTo(w io.Writer) (int64, error) {
	buffers := b.Buffers()
	var total int64
	for _, buffer := range buffers {
		for len(buffer) > 0 {
			n, err := w.Write(buffer)
			if n < 0 || n > len(buffer) {
				return total, fmt.Errorf("invalid writer count %d", n)
			}
			total += int64(n)
			buffer = buffer[n:]
			if err != nil {
				return total, err
			}
			if n == 0 {
				return total, io.ErrShortWrite
			}
		}
	}
	return total, nil
}

// WriteVectoredTo uses net.Buffers so TCP connections can issue scatter/gather
// writes. Callers that need generic short-writer tolerance should use WriteTo.
func (b *EncodedBatch) WriteVectoredTo(w io.Writer) (int64, error) {
	buffers := b.Buffers()
	written, err := buffers.WriteTo(w)
	if err == nil && written != b.Size() {
		return written, io.ErrShortWrite
	}
	return written, err
}

func (b *EncodedBatch) Size() int64 {
	size := int64(BatchHeaderSize)
	for _, record := range b.Records {
		size += 4 + int64(len(record))
	}
	return size
}

func ReadBatch(r io.Reader, limits Limits) (*Batch, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	var raw [BatchHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: batch header: %v", ErrTruncated, err)
	}
	header, err := parseBatchHeader(raw[:], limits)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, int(header.PayloadBytes))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("%w: batch payload: %v", ErrTruncated, err)
	}
	if crc32.ChecksumIEEE(payload) != header.PayloadCRC32 {
		return nil, fmt.Errorf("%w: payload crc mismatch", ErrInvalidFrame)
	}

	records := make([][]byte, 0, header.RecordCount)
	offset := 0
	for i := uint32(0); i < header.RecordCount; i++ {
		if len(payload)-offset < 4 {
			return nil, fmt.Errorf("%w: missing record %d length", ErrInvalidFrame, i)
		}
		length := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		if length == 0 || length > limits.MaxEventBytes || uint64(length) > uint64(len(payload)-offset) {
			return nil, fmt.Errorf("%w: record %d length out of range", ErrInvalidFrame, i)
		}
		next := offset + int(length)
		records = append(records, payload[offset:next])
		offset = next
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("%w: record count and payload length disagree", ErrInvalidFrame)
	}
	return &Batch{Header: header, Records: records, payload: payload}, nil
}

func parseBatchHeader(raw []byte, limits Limits) (BatchHeader, error) {
	if len(raw) != BatchHeaderSize || string(raw[0:4]) != string(batchMagic[:]) {
		return BatchHeader{}, fmt.Errorf("%w: bad batch magic", ErrInvalidFrame)
	}
	if raw[4] != Version || raw[5] != 0 || binary.BigEndian.Uint16(raw[6:8]) != BatchHeaderSize {
		return BatchHeader{}, fmt.Errorf("%w: unsupported batch header", ErrInvalidFrame)
	}
	header := BatchHeader{
		ClientID:     binary.BigEndian.Uint64(raw[8:16]),
		BatchID:      binary.BigEndian.Uint64(raw[16:24]),
		RecordCount:  binary.BigEndian.Uint32(raw[24:28]),
		PayloadBytes: binary.BigEndian.Uint32(raw[28:32]),
		PayloadCRC32: binary.BigEndian.Uint32(raw[32:36]),
		SentUnixNano: binary.BigEndian.Uint64(raw[40:48]),
	}
	if header.ClientID == 0 || header.BatchID == 0 || header.SentUnixNano == 0 || binary.BigEndian.Uint32(raw[36:40]) != 0 {
		return BatchHeader{}, fmt.Errorf("%w: invalid required or reserved header field", ErrInvalidFrame)
	}
	if header.RecordCount == 0 || header.RecordCount > limits.MaxRecords {
		return BatchHeader{}, fmt.Errorf("%w: record_count out of range", ErrInvalidFrame)
	}
	if header.PayloadBytes > limits.MaxPayload || uint64(header.PayloadBytes) < uint64(header.RecordCount)*4 {
		return BatchHeader{}, fmt.Errorf("%w: payload_bytes out of range", ErrInvalidFrame)
	}
	return header, nil
}

type Ack struct {
	Status   AckStatus
	ClientID uint64
	BatchID  uint64
}

func (a Ack) MarshalBinary() ([]byte, error) {
	if !a.Status.Valid() || a.ClientID == 0 || a.BatchID == 0 {
		return nil, fmt.Errorf("%w: invalid ack fields", ErrInvalidFrame)
	}
	raw := make([]byte, AckSize)
	copy(raw[0:4], ackMagic[:])
	raw[4] = Version
	raw[5] = byte(a.Status)
	binary.BigEndian.PutUint16(raw[6:8], AckSize)
	binary.BigEndian.PutUint64(raw[8:16], a.ClientID)
	binary.BigEndian.PutUint64(raw[16:24], a.BatchID)
	return raw, nil
}

func ReadAck(r io.Reader) (Ack, error) {
	var raw [AckSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return Ack{}, fmt.Errorf("%w: ack: %v", ErrTruncated, err)
	}
	if string(raw[0:4]) != string(ackMagic[:]) || raw[4] != Version || binary.BigEndian.Uint16(raw[6:8]) != AckSize {
		return Ack{}, fmt.Errorf("%w: unsupported ack header", ErrInvalidFrame)
	}
	ack := Ack{Status: AckStatus(raw[5]), ClientID: binary.BigEndian.Uint64(raw[8:16]), BatchID: binary.BigEndian.Uint64(raw[16:24])}
	if !ack.Status.Valid() || ack.ClientID == 0 || ack.BatchID == 0 {
		return Ack{}, fmt.Errorf("%w: invalid ack fields", ErrInvalidFrame)
	}
	return ack, nil
}

func WriteAck(w io.Writer, ack Ack) error {
	raw, err := ack.MarshalBinary()
	if err != nil {
		return err
	}
	for len(raw) > 0 {
		n, writeErr := w.Write(raw)
		if n < 0 || n > len(raw) {
			return fmt.Errorf("invalid writer count %d", n)
		}
		raw = raw[n:]
		if writeErr != nil {
			return writeErr
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
