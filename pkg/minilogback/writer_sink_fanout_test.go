package minilogback_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

func TestWriterSinkPreservesBatchForFanout(t *testing.T) {
	records := [][]byte{[]byte("alpha\n"), []byte("bravo\n")}
	var primary bytes.Buffer
	if err := minilogback.NewWriterSink(&primary).WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("primary WriteBatch: %v", err)
	}

	var archive bytes.Buffer
	if err := minilogback.NewWriterSink(&archive).WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("archive WriteBatch: %v", err)
	}

	const want = "alpha\nbravo\n"
	if got := primary.String(); got != want {
		t.Fatalf("primary output = %q, want %q", got, want)
	}
	if got := archive.String(); got != want {
		t.Fatalf("archive output = %q, want %q", got, want)
	}
}
