import type { LeaseDetail, LeaseSummary, MetricsSnapshot } from '../types';

export const metricsFixture: MetricsSnapshot = {
  sequence: 802,
  sampled_at: '2026-08-26T08:00:00.223456Z',
  ring: {
    capacity: 65_536,
    depth: 9_216,
    watermark_percent: 14.06,
    high_watermark: 18_432,
    publish_attempts: 3_000_000,
    accepted: 2_999_988,
    consumed: 2_990_772,
    dropped_total: 12,
    dropped_by_level: { debug: 10, info: 2, warn: 0, error: 0 },
    publish_rate: 428_000,
    consume_rate: 426_900,
  },
  flusher: {
    batches: 2_921,
    records: 2_990_772,
    bytes: 765_242_112,
    in_flight: 0,
    last_batch_size: 1_024,
    flush_p95_micros: 740,
    errors: 0,
    mode: 'vectored',
  },
  pool: {
    borrowed_total: 3_000_010,
    returned_total: 2_990_794,
    outstanding: 9_216,
    overdue: 1,
    double_returns: 0,
    invalid_returns: 0,
    hit_percent: 99.8,
    classes: [{ size: 256, in_use: 8_000, available: 4_096 }],
  },
  collector: { connections: 1, accepted_batches: 20, duplicate_batches: 0, invalid_frames: 0 },
  runtime: { goroutines: 12, heap_bytes: 16_777_216, demo_mode: true, audit_mode: 'full' },
  status: { sink: 'ok', collector: 'listening', transport: 'sse' },
};

export const overdueLease: LeaseSummary = {
  id: 91,
  state: 'overdue',
  size_class: 1_024,
  length: 318,
  borrowed_at: '2026-08-26T07:59:57Z',
  deadline: '2026-08-26T07:59:59Z',
  age_millis: 3_123,
  level: 'error',
  source: 'cmd/minilogbackd/demo.go:74',
  function: 'main.startDemoLeak',
};

export const leaseDetail: LeaseDetail = {
  ...overdueLease,
  stack: [{ function: 'main.startDemoLeak', source: 'cmd/minilogbackd/demo.go', line: 74 }],
};
