package httpapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSPAStaticAndReservedRoutes(t *testing.T) {
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "index.html"), []byte("dashboard-shell"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "assets", "app.js"), []byte("asset-body"), 0o600); err != nil {
		t.Fatal(err)
	}
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("api-response")) })
	handler := WithSPA(api, directory)
	tests := []struct {
		method   string
		path     string
		expected string
	}{
		{http.MethodGet, "/", "dashboard-shell"},
		{http.MethodGet, "/leases/42", "dashboard-shell"},
		{http.MethodGet, "/assets/app.js", "asset-body"},
		{http.MethodGet, "/api", "api-response"},
		{http.MethodGet, "/api/v1/metrics/current", "api-response"},
		{http.MethodGet, "/healthz", "api-response"},
		{http.MethodPost, "/unknown", "api-response"},
	}
	for _, test := range tests {
		t.Run(test.method+"_"+test.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Body.String() != test.expected {
				t.Fatalf("body=%q want=%q", response.Body.String(), test.expected)
			}
		})
	}
}
