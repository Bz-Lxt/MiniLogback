package sink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileSinkAppendsAndFlushes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "app.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := NewFile(FileConfig{Path: path, SyncPolicy: SyncEveryBatch})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("-new"), []byte("\n")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "old-new\n" {
		t.Fatalf("file contents = %q", got)
	}
	if capability := s.Capabilities(); capability.BatchMode == "" {
		t.Fatalf("missing capability: %+v", capability)
	}
}

func TestFileSinkRotatesAtRecordBoundary(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "rotate.log")
	s, err := NewFile(FileConfig{Path: path, MaxBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("aa"), []byte("bb")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rotated, err := filepath.Glob(path + ".*")
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated) != 1 {
		t.Fatalf("rotated files = %v", rotated)
	}
	first, _ := os.ReadFile(rotated[0])
	second, _ := os.ReadFile(path)
	if string(first) != "aa" || string(second) != "bb" {
		t.Fatalf("rotation contents = %q / %q", first, second)
	}
	if stats := s.Stats(); stats.Rotations != 1 || stats.Records != 2 || stats.Bytes != 4 {
		t.Fatalf("rotation stats = %+v", stats)
	}
}

func TestFileSinkResumesPartialWriteAcrossRotationWithoutReplay(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "retry.log")
	s, err := NewFile(FileConfig{Path: path, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.writeFn = func(ctx context.Context, file *os.File, records [][]byte) (int64, uint64, error) {
		calls++
		if calls == 2 {
			written, writeErr := file.Write(records[0][:1])
			if writeErr != nil {
				return int64(written), 1, writeErr
			}
			return int64(written), 1, errors.New("injected partial file failure")
		}
		return writeFileBatch(ctx, file, records)
	}
	records := [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")}
	if err := s.WriteBatch(context.Background(), records); err == nil {
		t.Fatal("partial file write unexpectedly succeeded")
	}
	if err := s.WriteBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rotated, _ := filepath.Glob(path + ".*")
	if len(rotated) != 1 {
		t.Fatalf("rotated files = %v", rotated)
	}
	first, _ := os.ReadFile(rotated[0])
	second, _ := os.ReadFile(path)
	if string(first)+string(second) != "aabbcc" {
		t.Fatalf("resumed rotation contents = %q / %q", first, second)
	}
}

func TestFileSinkRetriesSyncWithoutRewritingBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync-retry.log")
	s, err := NewFile(FileConfig{Path: path, SyncPolicy: SyncEveryBatch})
	if err != nil {
		t.Fatal(err)
	}
	syncCalls := 0
	s.syncFn = func(file *os.File) error {
		syncCalls++
		if syncCalls == 1 {
			return errors.New("injected sync failure")
		}
		return file.Sync()
	}
	records := [][]byte{[]byte("once")}
	if err := s.WriteBatch(context.Background(), records); err == nil {
		t.Fatal("first sync unexpectedly succeeded")
	}
	if err := s.WriteBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "once" {
		t.Fatalf("sync retry rewrote data: %q", data)
	}
}

func TestNewFileRejectsMissingPath(t *testing.T) {
	if _, err := NewFile(FileConfig{}); err == nil {
		t.Fatal("NewFile accepted an empty path")
	}
}

func TestNewFileValidatesLimitsAndClosedOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if _, err := NewFile(FileConfig{Path: path, MaxBytes: -1}); err == nil {
		t.Fatal("negative max bytes was accepted")
	}
	if _, err := NewFile(FileConfig{Path: path, SyncPolicy: "UNKNOWN"}); err == nil {
		t.Fatal("unknown sync policy was accepted")
	}
	s, err := NewFile(FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Flush(ctx); err != context.Canceled {
		t.Fatalf("canceled Flush error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(context.Background(), [][]byte{[]byte("x")}); err == nil {
		t.Fatal("closed file accepted a batch")
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after close = %v", err)
	}
}
