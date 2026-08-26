//go:build !darwin && !linux

package sink

import (
	"context"
	"os"
)

const fileBatchMode = "sequential"

func writeFileBatch(ctx context.Context, file *os.File, records [][]byte) (int64, uint64, error) {
	return writeSequential(ctx, file, records)
}
