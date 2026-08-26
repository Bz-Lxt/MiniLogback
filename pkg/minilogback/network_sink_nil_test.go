package minilogback_test

import (
	"testing"
	"time"

	"github.com/xavskye/minilogback/pkg/minilogback"
)

func TestNewNetworkSinkConstructionFailureIsSafeToCleanUp(t *testing.T) {
	sink, err := minilogback.NewNetworkSink(minilogback.NetworkSinkConfig{
		Address:      "127.0.0.1:28641",
		ClientID:     1,
		RetryInitial: 2 * time.Second,
		RetryMaximum: time.Second,
	})
	if err == nil {
		t.Fatal("expected an invalid retry window to be rejected")
	}
	if sink == nil {
		return
	}
	if closeErr := sink.Close(); closeErr != nil {
		t.Fatalf("clean up sink returned with construction error: %v", closeErr)
	}
	t.Fatal("NewNetworkSink returned a usable-looking sink together with a construction error")
}
