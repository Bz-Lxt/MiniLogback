package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xavskye/minilogback/internal/telemetry"
)

var ErrNotFound = errors.New("not found")

type MetricsProvider interface {
	Current() telemetry.Snapshot
	Subscribe(context.Context) (<-chan telemetry.Snapshot, error)
}

type Health struct {
	Status        string `json:"status"`
	Appender      string `json:"appender"`
	Sink          string `json:"sink"`
	Collector     string `json:"collector"`
	Version       string `json:"version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

type HealthProvider interface{ Health() Health }

type StackFrame struct {
	Function string `json:"function"`
	Source   string `json:"source"`
	Line     int    `json:"line"`
}

type Lease struct {
	ID         uint64       `json:"id"`
	State      string       `json:"state"`
	SizeClass  int          `json:"size_class"`
	Length     int          `json:"length"`
	BorrowedAt time.Time    `json:"borrowed_at"`
	Deadline   time.Time    `json:"deadline"`
	AgeMillis  int64        `json:"age_millis"`
	Level      string       `json:"level"`
	Source     string       `json:"source"`
	Function   string       `json:"function"`
	Stack      []StackFrame `json:"stack,omitempty"`
}

type LeaseQuery struct {
	State  string
	Cursor uint64
	Limit  int
}

type LeasePage struct {
	Leases     []Lease
	NextCursor string
	HasMore    bool
}

type LeaseProvider interface {
	ListLeases(context.Context, LeaseQuery) (LeasePage, error)
	LeaseByID(context.Context, uint64) (Lease, error)
}

type DemoTrafficRequest struct {
	EventsPerSecond int `json:"events_per_second"`
	DurationSeconds int `json:"duration_seconds"`
	PayloadBytes    int `json:"payload_bytes"`
}

type DemoTrafficStatus struct {
	Status          string `json:"status"`
	EventsPerSecond int    `json:"events_per_second"`
	DurationSeconds int    `json:"duration_seconds"`
}

type DemoLeaseRequest struct {
	SizeBytes int    `json:"size_bytes"`
	Level     string `json:"level"`
}

type DemoController interface {
	StartTraffic(context.Context, DemoTrafficRequest) (DemoTrafficStatus, error)
	RetainLease(context.Context, DemoLeaseRequest) (Lease, error)
	ReleaseLease(context.Context, uint64) error
}

type Dependencies struct {
	Metrics     MetricsProvider
	Health      HealthProvider
	Leases      LeaseProvider
	Effective   func() map[string]any
	Demo        DemoController
	DemoAllowed bool
	Keepalive   time.Duration
}

func New(dependencies Dependencies) (http.Handler, error) {
	if dependencies.Metrics == nil || dependencies.Health == nil || dependencies.Leases == nil || dependencies.Effective == nil {
		return nil, errors.New("metrics, health, leases, and effective config providers are required")
	}
	if dependencies.Keepalive <= 0 {
		dependencies.Keepalive = 15 * time.Second
	}
	api := &API{dependencies: dependencies}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /api/v1/metrics/current", api.metricsCurrent)
	mux.HandleFunc("GET /api/v1/metrics/stream", api.metricsStream)
	mux.HandleFunc("GET /api/v1/leases", api.leases)
	mux.HandleFunc("GET /api/v1/leases/{id}", api.lease)
	mux.HandleFunc("GET /api/v1/config/effective", api.effective)
	mux.HandleFunc("POST /api/v1/demo/traffic", api.demoTraffic)
	mux.HandleFunc("POST /api/v1/demo/leases", api.demoLease)
	mux.HandleFunc("DELETE /api/v1/demo/leases/{id}", api.demoRelease)
	for _, pattern := range []string{
		"/healthz", "/api/v1/metrics/current", "/api/v1/metrics/stream", "/api/v1/leases",
		"/api/v1/leases/{id}", "/api/v1/config/effective", "/api/v1/demo/traffic",
		"/api/v1/demo/leases", "/api/v1/demo/leases/{id}",
	} {
		mux.HandleFunc(pattern, api.methodNotAllowed)
	}
	mux.HandleFunc("/", api.notFound)
	return securityHeaders(mux), nil
}

type API struct{ dependencies Dependencies }

func (a *API) methodNotAllowed(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Allow", "GET, POST, DELETE")
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed for this resource")
}

func (a *API) notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not_found", "resource not found")
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	health := a.dependencies.Health.Health()
	status := http.StatusOK
	if health.Status == "unhealthy" || health.Status == "closing" {
		status = http.StatusServiceUnavailable
	}
	writeData(w, status, health)
}

func (a *API) metricsCurrent(w http.ResponseWriter, _ *http.Request) {
	writeData(w, http.StatusOK, a.dependencies.Metrics.Current())
}

func (a *API) metricsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal_error", "streaming is unavailable")
		return
	}
	updates, err := a.dependencies.Metrics.Subscribe(r.Context())
	if err != nil {
		if errors.Is(err, telemetry.ErrTooManySubscribers) {
			writeError(w, http.StatusTooManyRequests, "rate_limited", "too many metric streams")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "unable to subscribe")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	_, _ = fmt.Fprint(w, "retry: 2000\n\n")
	flusher.Flush()
	keepalive := time.NewTicker(a.dependencies.Keepalive)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case snapshot := <-updates:
			payload, marshalErr := json.Marshal(map[string]any{"data": snapshot})
			if marshalErr != nil {
				return
			}
			_, _ = fmt.Fprintf(w, "id: %d\nevent: metrics\ndata: %s\n\n", snapshot.Sequence, payload)
			flusher.Flush()
		}
	}
}

func (a *API) leases(w http.ResponseWriter, r *http.Request) {
	query := LeaseQuery{State: r.URL.Query().Get("state"), Limit: 50}
	if query.State == "" {
		query.State = "all"
	}
	if !validLeaseState(query.State) {
		writeError(w, http.StatusBadRequest, "invalid_request", "unsupported lease state")
		return
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 200 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 200")
			return
		}
		query.Limit = value
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || value == 0 {
			writeError(w, http.StatusBadRequest, "invalid_request", "cursor must be a positive unsigned integer")
			return
		}
		query.Cursor = value
	}
	page, err := a.dependencies.Leases.ListLeases(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "unable to list leases")
		return
	}
	if page.Leases == nil {
		page.Leases = []Lease{}
	}
	for i := range page.Leases {
		sanitizeLease(&page.Leases[i], false)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": page.Leases, "meta": map[string]any{"next_cursor": page.NextCursor, "has_more": page.HasMore, "limit": query.Limit}})
}

func (a *API) lease(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	lease, err := a.dependencies.Leases.LeaseByID(r.Context(), id)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "lease not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "unable to read lease")
		return
	}
	sanitizeLease(&lease, true)
	writeData(w, http.StatusOK, lease)
}

func (a *API) effective(w http.ResponseWriter, _ *http.Request) {
	writeData(w, http.StatusOK, a.dependencies.Effective())
}

func (a *API) demoTraffic(w http.ResponseWriter, r *http.Request) {
	if !a.demoReady(w) {
		return
	}
	var request DemoTrafficRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.EventsPerSecond < 1 || request.EventsPerSecond > 1_000_000 || request.DurationSeconds < 1 || request.DurationSeconds > 60 || request.PayloadBytes < 32 || request.PayloadBytes > 65536 {
		writeError(w, http.StatusBadRequest, "invalid_request", "demo traffic values are out of range")
		return
	}
	status, err := a.dependencies.Demo.StartTraffic(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "unable to start demo traffic")
		return
	}
	writeData(w, http.StatusAccepted, status)
}

func (a *API) demoLease(w http.ResponseWriter, r *http.Request) {
	if !a.demoReady(w) {
		return
	}
	var request DemoLeaseRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.SizeBytes < 1 || request.SizeBytes > 65536 || !validLevel(request.Level) {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid demo lease size or level")
		return
	}
	lease, err := a.dependencies.Demo.RetainLease(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "unable to retain demo lease")
		return
	}
	sanitizeLease(&lease, false)
	w.Header().Set("Location", fmt.Sprintf("/api/v1/leases/%d", lease.ID))
	writeData(w, http.StatusCreated, lease)
}

func (a *API) demoRelease(w http.ResponseWriter, r *http.Request) {
	if !a.demoReady(w) {
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := a.dependencies.Demo.ReleaseLease(r.Context(), id); err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "demo lease not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "unable to release demo lease")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) demoReady(w http.ResponseWriter) bool {
	if !a.dependencies.DemoAllowed || a.dependencies.Demo == nil {
		writeError(w, http.StatusForbidden, "demo_disabled", "demo actions require DEMO_MODE and a safe listener")
		return false
	}
	return true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain one JSON value")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "id must be a positive unsigned integer")
		return 0, false
	}
	return id, true
}

func sanitizeLease(lease *Lease, includeStack bool) {
	lease.Source = sanitizePath(lease.Source)
	if !includeStack {
		lease.Stack = nil
		return
	}
	if lease.Stack == nil {
		lease.Stack = []StackFrame{}
	}
	for i := range lease.Stack {
		lease.Stack[i].Source = sanitizePath(lease.Stack[i].Source)
	}
}

func sanitizePath(value string) string {
	if filepath.IsAbs(value) {
		return filepath.Base(value)
	}
	clean := filepath.Clean(value)
	if clean == "." || strings.HasPrefix(clean, "..") {
		return filepath.Base(clean)
	}
	return filepath.ToSlash(clean)
}

func validLeaseState(state string) bool {
	switch state {
	case "all", "borrowed", "queued", "in_flight", "overdue", "returned":
		return true
	default:
		return false
	}
}

func validLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}

func writeData(w http.ResponseWriter, status int, data any) {
	writeJSON(w, status, map[string]any{"data": data})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": code, "message": message, "details": []any{}}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
