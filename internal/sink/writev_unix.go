//go:build darwin || linux

package sink

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	fileBatchMode = "writev"
	maxIOVectors  = 1024
)

func writeFileBatch(ctx context.Context, file *os.File, records [][]byte) (written int64, shortWrites uint64, err error) {
	remaining := append([][]byte(nil), records...)
	for len(remaining) > 0 {
		for len(remaining) > 0 && len(remaining[0]) == 0 {
			remaining = remaining[1:]
		}
		if len(remaining) == 0 {
			break
		}
		if err := ctx.Err(); err != nil {
			return written, shortWrites, err
		}
		limit := len(remaining)
		if limit > maxIOVectors {
			limit = maxIOVectors
		}
		iovecs := make([]syscall.Iovec, 0, limit)
		requested := 0
		for _, record := range remaining[:limit] {
			if len(record) == 0 {
				continue
			}
			iovec := syscall.Iovec{Base: &record[0]}
			iovec.SetLen(len(record))
			iovecs = append(iovecs, iovec)
			requested += len(record)
		}
		count, _, errno := syscall.RawSyscall(syscall.SYS_WRITEV, file.Fd(), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)))
		runtime.KeepAlive(remaining)
		if errno != 0 {
			if errors.Is(errno, syscall.EINTR) {
				continue
			}
			return written, shortWrites, errno
		}
		if count == 0 {
			return written, shortWrites, io.ErrNoProgress
		}
		if int(count) < requested {
			shortWrites++
		}
		written += int64(count)
		remaining = advanceRecords(remaining, int(count))
	}
	return written, shortWrites, nil
}

func advanceRecords(records [][]byte, written int) [][]byte {
	for len(records) > 0 && written >= len(records[0]) {
		written -= len(records[0])
		records = records[1:]
	}
	if len(records) > 0 && written > 0 {
		records[0] = records[0][written:]
	}
	return records
}
