package sink

import (
	"bytes"
	"context"
	"errors"
	"io"
	"syscall"
)

var ErrBatchChanged = errors.New("sink retry batch changed after partial commit")

// batchCursor retains the original zero-copy views after a partial commit. The
// BatchSink contract requires the caller to retry that logical batch until it
// succeeds; retaining only slice headers lets built-in sinks continue at the
// exact byte offset without copying or replaying an already written prefix.
type batchCursor struct {
	records [][]byte
	record  int
	offset  int
	active  bool
}

func (c *batchCursor) begin(records [][]byte) error {
	if !c.active {
		c.records = records[:len(records):len(records)]
		c.active = true
	} else {
		if len(c.records) != len(records) {
			return ErrBatchChanged
		}
		for index := range records {
			if !bytes.Equal(c.records[index], records[index]) {
				return ErrBatchChanged
			}
		}
	}
	return c.advance(0)
}

func (c *batchCursor) remaining() [][]byte {
	if c.record >= len(c.records) {
		return nil
	}
	remaining := append([][]byte(nil), c.records[c.record:]...)
	if c.offset > 0 {
		remaining[0] = remaining[0][c.offset:]
	}
	return remaining
}

func (c *batchCursor) advance(written int64) error {
	if written < 0 {
		return io.ErrShortWrite
	}
	for c.record < len(c.records) && c.offset == len(c.records[c.record]) {
		c.record++
		c.offset = 0
	}
	for written > 0 && c.record < len(c.records) {
		available := len(c.records[c.record]) - c.offset
		if written < int64(available) {
			c.offset += int(written)
			return nil
		}
		written -= int64(available)
		c.record++
		c.offset = 0
	}
	for c.record < len(c.records) && c.offset == len(c.records[c.record]) {
		c.record++
		c.offset = 0
	}
	if written != 0 {
		return io.ErrShortWrite
	}
	return nil
}

func (c *batchCursor) done() bool { return c.record == len(c.records) }

func (c *batchCursor) reset() {
	// Drop the cursor's reference to the batch without mutating it: c.records
	// aliases the caller's [][]byte backing array, so clear() would nil out
	// every record visible to a fan-out downstream sink.
	c.records = nil
	c.record = 0
	c.offset = 0
	c.active = false
}

func writeSequential(ctx context.Context, writer io.Writer, records [][]byte) (written int64, shortWrites uint64, err error) {
	for _, record := range records {
		for len(record) > 0 {
			if err := ctx.Err(); err != nil {
				return written, shortWrites, err
			}
			count, writeErr := writer.Write(record)
			if count < 0 || count > len(record) {
				return written, shortWrites, io.ErrShortWrite
			}
			if count > 0 {
				written += int64(count)
				record = record[count:]
				if len(record) > 0 {
					shortWrites++
				}
			}
			if writeErr != nil {
				if errors.Is(writeErr, syscall.EINTR) {
					continue
				}
				return written, shortWrites, writeErr
			}
			if count == 0 {
				return written, shortWrites, io.ErrNoProgress
			}
		}
	}
	return written, shortWrites, nil
}
