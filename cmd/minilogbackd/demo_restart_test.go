package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/config"
	"github.com/xavskye/minilogback/internal/httpapi"
	"github.com/xavskye/minilogback/internal/telemetry"
)

func TestDemoTrafficRestartAppliesLatestRequest(t *testing.T) {
	rootCtx, stop := context.WithCancel(context.Background())
	cfg := config.Defaults()
	cfg.DemoMode = true
	cfg.LogPath = filepath.Join(t.TempDir(), "demo-restart.log")
	runtime, err := newRuntime(rootCtx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		stop()
		t.Fatal(err)
	}
	sampler, err := telemetry.NewSampler(runtime, 10*time.Millisecond, cfg.SSEMaxClients)
	if err != nil {
		stop()
		_ = runtime.Close(context.Background())
		t.Fatal(err)
	}
	handler, err := httpapi.New(httpapi.Dependencies{
		Metrics: sampler, Health: runtime, Leases: runtime, Effective: cfg.Effective,
		Demo: runtime, DemoAllowed: cfg.DemoAllowed(), Keepalive: time.Second,
	})
	if err != nil {
		stop()
		_ = runtime.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(shutdownCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	startTraffic := func(eventsPerSecond int) {
		t.Helper()
		body := fmt.Sprintf(`{"events_per_second":%d,"duration_seconds":5,"payload_bytes":64}`, eventsPerSecond)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/demo/traffic", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("start traffic at %d events/s: status=%d body=%s", eventsPerSecond, response.Code, response.Body.String())
		}
	}

	startTraffic(1000)

	readAccepted := func() uint64 {
		t.Helper()
		sampler.SampleNow()
		request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/current", nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("read metrics: status=%d body=%s", response.Code, response.Body.String())
		}
		var envelope struct {
			Data struct {
				Ring struct {
					Accepted uint64 `json:"accepted"`
				} `json:"ring"`
			} `json:"data"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.Data.Ring.Accepted
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for readAccepted() < 20 {
		if time.Now().After(deadline) {
			t.Fatal("initial 1000 events/s request did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	startTraffic(1)
	before := readAccepted()
	time.Sleep(300 * time.Millisecond)
	after := readAccepted()
	if after-before > 50 {
		t.Fatalf("latest 1 event/s request was accepted, but the previous generator produced %d more events in 300ms", after-before)
	}
}
