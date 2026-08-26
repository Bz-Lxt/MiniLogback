package config

import "testing"

func TestDefaultsValidate(t *testing.T) {
	cfg, err := Parse(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:28640" || cfg.IngestAddr != "127.0.0.1:28641" {
		t.Fatalf("unexpected addresses: %q %q", cfg.HTTPAddr, cfg.IngestAddr)
	}
	if cfg.NetworkMaxAttempts != 8 || cfg.NetworkRetryMaximum < cfg.NetworkRetryInitial {
		t.Fatalf("unexpected network retry defaults: %+v", cfg)
	}
}

func TestParseRejectsInvalidValues(t *testing.T) {
	values := map[string]string{"MINILOGBACK_RING_CAPACITY": "1000"}
	_, err := Parse(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err == nil {
		t.Fatal("expected validation error")
	}
	values = map[string]string{"MINILOGBACK_NETWORK_RETRY_MAXIMUM": "1ms"}
	_, err = Parse(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err == nil {
		t.Fatal("expected retry ordering error")
	}
}

func TestDemoDoubleGate(t *testing.T) {
	cfg := Defaults()
	cfg.DemoMode = true
	if !cfg.DemoAllowed() {
		t.Fatal("loopback demo should be allowed")
	}
	cfg.HTTPAddr = "0.0.0.0:8080"
	if cfg.DemoAllowed() {
		t.Fatal("wildcard bind requires container gate")
	}
	cfg.ContainerMode = true
	if !cfg.DemoAllowed() {
		t.Fatal("explicit container gate should allow wildcard bind")
	}
}
