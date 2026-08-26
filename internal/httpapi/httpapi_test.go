package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xavskye/minilogback/internal/telemetry"
)

type fakeMetrics struct{ snapshot telemetry.Snapshot }

func (f fakeMetrics) Current() telemetry.Snapshot { return f.snapshot }
func (f fakeMetrics) Subscribe(context.Context) (<-chan telemetry.Snapshot, error) {
	updates := make(chan telemetry.Snapshot, 1)
	updates <- f.snapshot
	return updates, nil
}

type fakeHealth struct{}

func (fakeHealth) Health() Health { return Health{Status: "ok", Version: "test"} }

type fakeLeases struct{}

func (fakeLeases) ListLeases(context.Context, LeaseQuery) (LeasePage, error) {
	return LeasePage{Leases: nil}, nil
}
func (fakeLeases) LeaseByID(context.Context, uint64) (Lease, error) { return Lease{}, ErrNotFound }

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	handler, err := New(Dependencies{Metrics: fakeMetrics{snapshot: telemetry.Snapshot{Sequence: 9, SampledAt: time.Unix(1, 0)}}, Health: fakeHealth{}, Leases: fakeLeases{}, Effective: func() map[string]any { return map[string]any{"demo_mode": false} }})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestMetricsAndEmptyLeases(t *testing.T) {
	for _, path := range []string{"/api/v1/metrics/current", "/api/v1/leases"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		testHandler(t).ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDemoRequiresBothGates(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/demo/traffic", nil)
	response := httptest.NewRecorder()
	testHandler(t).ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestLeaseQueryValidation(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/leases?limit=201", nil)
	response := httptest.NewRecorder()
	testHandler(t).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestMethodAndRouteErrorsUseJSONEnvelope(t *testing.T) {
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/healthz", nil),
		httptest.NewRequest(http.MethodGet, "/missing", nil),
	} {
		response := httptest.NewRecorder()
		testHandler(t).ServeHTTP(response, request)
		if response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
			t.Fatalf("content type=%q", response.Header().Get("Content-Type"))
		}
		var body struct {
			Error map[string]any `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.Error == nil {
			t.Fatalf("invalid error envelope: %s (%v)", response.Body.String(), err)
		}
	}
}
