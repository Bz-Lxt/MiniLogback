# MiniLogback

MiniLogback is a bounded, fully asynchronous Go log appender with an atomic
MPSC ring, pooled byte leases, copy-avoiding batch sinks, a distributed TCP
collector, and a React operations dashboard for queue water level and overdue
lease stacks.

## 1. What it guarantees

- Producer publication performs no disk/network I/O. The ring event path uses
  typed atomics and per-slot sequences, not channels or mutexes.
- A successful publication transfers lease ownership to the appender. The
  backing bytes are returned only after file completion or a matching network
  ACK.
- Batch flush occurs at 1,024 records or 50ms after the first pending record,
  whichever happens first.
- Pool leases that exceed the configured deadline are observable with a
  sanitized allocation stack.

The queue is finite. Its default overload policy is `DROP_NEWEST`, so it cannot
promise both immediate producer return and lossless logging. A process crash,
SIGKILL, disk failure, or prolonged sink outage can lose unflushed records.

“Zero-copy” in this project means no aggregate application buffer is built from
the leased payloads. Vectored I/O is used where supported; kernel, filesystem,
TCP and Collector-boundary copies still exist.

## 2. Quick start

Requirements: Docker with Compose. The images are multi-stage and build both Go
and React inside Docker.

```bash
cp .env.example .env
docker compose up --build -d
./scripts/smoke_test.sh
```

Open <http://localhost:28640>. The trusted-network TCP ingest listener is
available on `127.0.0.1:28641`. Override stable host ports with
`MINILOGBACK_HTTP_PORT` and `MINILOGBACK_INGEST_PORT`.

Stop without deleting the persistent log volume:

```bash
docker compose down
```

## 3. Go SDK

The public package is `github.com/xavskye/minilogback/pkg/minilogback`.
Construct an Appender with a bounded ring, pool and sink, then use
`Debug/Info/Warn/Error` for convenience. For the measured low-allocation path,
acquire a lease, encode into its buffer and publish it explicitly. Rejected
explicit publication retains caller ownership; convenience methods clean up
automatically.

It can replace the destination of an existing standard-library logger:

```go
sink, err := minilogback.NewFileSink(minilogback.FileSinkConfig{Path: "./data/app.log", MaxBytes: 64 << 20})
if err != nil { log.Fatal(err) }
appender, err := minilogback.New(minilogback.Config{RingCapacity: 65536, BatchSize: 1024, FlushInterval: 50 * time.Millisecond, Sink: sink})
if err != nil { log.Fatal(err) }
standard := log.New(appender.Writer(minilogback.InfoLevel), "", 0)
standard.Print("request completed")
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := appender.Close(ctx); err != nil { log.Printf("log drain failed: %v", err) }
```

The snippet uses `context`, `log`, `time`, and the public `minilogback` package.
For distributed delivery, construct `NewNetworkSink(NetworkSinkConfig{Address:
"collector:9010"})` and pass it as `Config.Sink`; the public adapter returns
success only after the versioned Collector ACK and applies the configured
bounded retry policy.

Custom `Sink` implementations receive the same logical batch again after an
error. If an implementation can commit only a prefix before returning that
error, it must retain its record/byte offset and resume without replaying the
prefix. The built-in file and writer sinks already implement this contract and
keep the zero-copy views pinned until completion.

## 4. Architecture and protocols

- [Requirements](docs/Requirements.md)
- [Roadmap](docs/Roadmap.md)
- [Architecture](docs/Architecture.md)
- [Management API](docs/API.md)
- [TCP protocol](docs/Protocol.md)
- [UI design](docs/DesignSpec.md)

Network delivery is ACK-after-sink with bounded in-memory deduplication. It is
at-least-once leaning during a Collector lifetime; Collector restart can admit
a duplicate. Exactly-once, persistent WAL/dedupe, and cross-region consensus
are outside MVP.

## 5. Configuration

All settings are environment variables and fail fast on invalid values. Copy
`.env.example` for the complete list. Key defaults are ring capacity 65,536,
batch size 1,024, flush interval 50ms, maximum event 1MiB, lease deadline 2s,
and file sink `/data/minilogback.log`.

`MINILOGBACK_AUDIT_MODE` accepts `off`, `sampled`, or `full`. The SDK-oriented
default is off for hot-path performance; the Docker diagnostic experience uses
full. Benchmark reports must keep these modes separate.

## 6. Development and verification

```bash
go test ./...
go test -race ./...
go vet ./...
make bench
make perf-publish

cd frontend
npm ci
npm test -- --run
npm run build
npx playwright test
```

With the Compose service healthy, run the non-mocked browser contract with:

```bash
cd frontend
E2E_LIVE=1 E2E_BASE_URL=http://127.0.0.1:28640 PLAYWRIGHT_CHANNEL=chrome npm run test:e2e -- tests/live_flow.spec.ts --project=mobile-480 --workers=1
```

The required benchmark protocol records Go/OS/arch/CPU/GOMAXPROCS and reports
the explicit lease fast path independently from convenience encoding and FULL
audit. Actual runs are recorded in `docs/QA_Record.md`; no performance claim is
made from design alone.

`perfcheck` publishes pre-encoded 256B leases through a 65,536-slot ring with
32 producers. Latency covers one `TryPublishLease` call and reports warmed-up
p50/p95/p99/p999/max samples. Run audit-off and FULL together to obtain the
relative throughput and p99 cost:

```bash
go run ./cmd/perfcheck -mode publish -audit both -warmup 3s -duration 10s > publish.json
docker compose exec -T minilogback perfcheck -mode publish -audit both -warmup 3s -duration 10s > publish-container.json
```

The release soak command is intentionally 30 minutes. It drains after warmup,
runs explicit GC for the live-heap baseline, samples heap/goroutines, drains and
GCs again, and requires heap drift ≤10%, no final goroutine growth, zero queue
overflow and zero outstanding leases:

```bash
go run ./cmd/perfcheck -mode soak -audit off -warmup 30s -duration 30m -rate 100000 > soak-30m.json
docker compose exec -T minilogback perfcheck -mode soak -audit off -warmup 30s -duration 30m -rate 100000 > soak-container-30m.json
```

`make perf-short` exercises the same path in five seconds, but its report sets
`thirty_minute_evidence=false`; it is a diagnostic and must not be presented as
AC-12/NFR-PERF-04 release evidence. Absolute throughput/latency gates apply only
to audit-off runs on machines with at least eight logical CPUs. FULL mode is a
separate overhead report, not an audit-off performance claim.

## 7. Real, demo, and external-provider modes

There is no external or metered provider. File and TCP sinks are real local
implementations; expected QA/CI provider cost is **¥0**. Tests replace sinks and
network peers with deterministic in-process fakes, never a fake external API.

`MINILOGBACK_DEMO_MODE=true` enables bounded traffic generation and a deliberate
retained-lease diagnostic endpoint. The dashboard displays a permanent demo
badge while enabled. These mutation endpoints are disabled by default in the
library/production configuration and require the trusted local listener gate.
Use `./scripts/seed_demo.sh` only against an explicit demo instance.

## 8. Security boundary

MVP TCP ingest is plaintext and intended for loopback or a trusted Docker/private
network. Do not expose port 9010 to the public internet. TLS/mTLS and identity
are V1 work pending a deployment trust model. The HTTP management API is
read-only except for demo-gated endpoints; it never returns payload bytes,
secrets, or untrimmed host paths.

Containers run as UID 10001, drop Linux capabilities, set no-new-privileges,
use a read-only root filesystem and write logs only to the named `/data` volume.

## 9. Failure and shutdown behavior

On SIGTERM the server stops accepting work, rejects new publication, drains the
ring, flushes the sink and waits up to the configured shutdown timeout. Repeated
Close is idempotent. A timeout reports remaining and in-flight work; it is not
reported as successful persistence.

Network sink retains one unacknowledged batch and retries transient disconnect,
overload or sink errors with capped backoff. While a sink is unavailable,
producers remain non-blocking and queue-full rejections become visible in the
dashboard.

## 10. License

This repository is delivered as a project implementation. Add the intended
distribution license before publishing it as a public module.
