package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xavskye/minilogback/internal/config"
	"github.com/xavskye/minilogback/internal/httpapi"
	"github.com/xavskye/minilogback/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger); err != nil {
		logger.Error("minilogbackd stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	runtime, err := newRuntime(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("initialize runtime: %w", err)
	}
	sampler, err := telemetry.NewSampler(runtime, cfg.TelemetryInterval, cfg.SSEMaxClients)
	if err != nil {
		_ = runtime.Close(context.Background())
		return err
	}
	handler, err := httpapi.New(httpapi.Dependencies{
		Metrics: sampler, Health: runtime, Leases: runtime, Effective: cfg.Effective,
		Demo: runtime, DemoAllowed: cfg.DemoAllowed(), Keepalive: 15 * time.Second,
	})
	if err != nil {
		_ = runtime.Close(context.Background())
		return err
	}
	handler = httpapi.WithSPA(handler, cfg.WebDir)
	httpListener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = runtime.Close(context.Background())
		return fmt.Errorf("listen HTTP: %w", err)
	}
	ingestListener, err := net.Listen("tcp", cfg.IngestAddr)
	if err != nil {
		_ = httpListener.Close()
		_ = runtime.Close(context.Background())
		return fmt.Errorf("listen collector: %w", err)
	}

	serviceCtx, serviceCancel := context.WithCancel(ctx)
	defer serviceCancel()
	go func() { _ = sampler.Run(serviceCtx) }()
	httpServer := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20,
	}
	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- normalizeServeError(httpServer.Serve(httpListener)) }()
	go func() { errorsChannel <- runtime.collector.Serve(ingestListener) }()
	logger.Info("minilogbackd ready", "http_addr", httpListener.Addr().String(), "ingest_addr", ingestListener.Addr().String(), "demo_allowed", cfg.DemoAllowed())

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errorsChannel:
	}
	serviceCancel()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	httpErr := httpServer.Shutdown(shutdownCtx)
	runtimeErr := runtime.Close(shutdownCtx)
	return errors.Join(serveErr, httpErr, runtimeErr)
}

func normalizeServeError(err error) error {
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
