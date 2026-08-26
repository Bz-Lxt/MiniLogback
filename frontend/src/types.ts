export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type LeaseState = 'borrowed' | 'queued' | 'in_flight' | 'overdue' | 'returned';
export type LeaseFilter = 'all' | LeaseState;

export interface RingMetrics {
  capacity: number;
  depth: number;
  watermark_percent: number;
  high_watermark: number;
  publish_attempts: number;
  accepted: number;
  consumed: number;
  dropped_total: number;
  dropped_by_level: Record<LogLevel, number>;
  publish_rate: number;
  consume_rate: number;
}

export interface FlusherMetrics {
  batches: number;
  records: number;
  bytes: number;
  in_flight: number;
  last_batch_size: number;
  flush_p95_micros: number;
  errors: number;
  mode: string;
}

export interface PoolClass {
  size: number;
  in_use: number;
  available: number;
}

export interface PoolMetrics {
  borrowed_total: number;
  returned_total: number;
  outstanding: number;
  overdue: number;
  double_returns: number;
  invalid_returns: number;
  hit_percent: number;
  classes: PoolClass[];
}

export interface MetricsSnapshot {
  sequence: number;
  sampled_at: string;
  ring: RingMetrics;
  flusher: FlusherMetrics;
  pool: PoolMetrics;
  collector: {
    connections: number;
    accepted_batches: number;
    duplicate_batches: number;
    invalid_frames: number;
  };
  runtime: {
    goroutines: number;
    heap_bytes: number;
    demo_mode: boolean;
    audit_mode: string;
  };
  status: {
    sink: string;
    collector: string;
    transport: string;
  };
}

export interface LeaseSummary {
  id: number;
  state: LeaseState;
  size_class: number;
  length: number;
  borrowed_at: string;
  deadline: string;
  age_millis: number;
  level: LogLevel;
  source: string;
  function: string;
}

export interface StackFrame {
  function: string;
  source: string;
  line: number;
}

export interface LeaseDetail extends LeaseSummary {
  stack: StackFrame[];
}

export interface LeasePage {
  data: LeaseSummary[];
  meta: {
    next_cursor: string;
    has_more: boolean;
    limit: number;
  };
}

export interface EffectiveConfig {
  ring_capacity: number;
  batch_size: number;
  flush_interval: string;
  max_event_bytes: number;
  audit_mode: string;
  lease_timeout: string;
  sink_type: string;
  sink_target: string;
  demo_mode: boolean;
  demo_allowed: boolean;
  network_security: string;
  capabilities: {
    vectored_file: boolean;
    vectored_network: boolean;
    sse: boolean;
  };
}

export interface APIErrorShape {
  error?: {
    code?: string;
    message?: string;
    details?: unknown[];
  };
}

export type ConnectionMode = 'loading' | 'live' | 'degraded' | 'error';

export interface TrafficRequest {
  events_per_second: number;
  duration_seconds: number;
  payload_bytes: number;
}

export interface DemoLeaseRequest {
  size_bytes: number;
  level: LogLevel;
}
