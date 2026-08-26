import { formatBytes, formatCompact, formatNumber } from '../lib/format';
import type { MetricsSnapshot } from '../types';
import { MetricCard } from './MetricCard';

interface Props {
  snapshot: MetricsSnapshot | null;
}

export function MetricGrid({ snapshot }: Props) {
  const loading = snapshot === null;
  const ring = snapshot?.ring;
  const flusher = snapshot?.flusher;
  const pool = snapshot?.pool;
  const dropTone = (ring?.dropped_total ?? 0) > 0 ? 'danger' : 'normal';
  const overdueTone = (pool?.overdue ?? 0) > 0 ? 'danger' : 'signal';

  return (
    <div className="metric-grid" aria-label="核心遥测指标">
      <MetricCard label="PUBLISH RATE" value={formatCompact(ring?.publish_rate ?? 0)} unit="evt/s" detail="producer ingress" tone="signal" loading={loading} />
      <MetricCard label="CONSUME RATE" value={formatCompact(ring?.consume_rate ?? 0)} unit="evt/s" detail="single consumer" tone="signal" loading={loading} />
      <MetricCard label="HIGH WATERMARK" value={formatNumber(ring?.high_watermark ?? 0)} detail={`${ring?.capacity ? ((ring.high_watermark / ring.capacity) * 100).toFixed(1) : '0.0'}% of capacity`} loading={loading} />
      <MetricCard label="DROPPED TOTAL" value={formatNumber(ring?.dropped_total ?? 0)} detail="DROP_NEWEST policy" tone={dropTone} loading={loading} />
      <MetricCard label="LAST BATCH" value={formatNumber(flusher?.last_batch_size ?? 0)} unit="records" detail={`${formatBytes(flusher?.bytes ?? 0)} total`} loading={loading} />
      <MetricCard label="FLUSH P95" value={formatNumber(flusher?.flush_p95_micros ?? 0)} unit="µs" detail={`${formatNumber(flusher?.errors ?? 0)} sink errors`} tone={(flusher?.errors ?? 0) > 0 ? 'danger' : 'normal'} loading={loading} />
      <MetricCard label="POOL OUTSTANDING" value={formatNumber(pool?.outstanding ?? 0)} unit="leases" detail={`${pool?.hit_percent?.toFixed(1) ?? '0.0'}% pool hit`} loading={loading} />
      <MetricCard label="OVERDUE LEASES" value={formatNumber(pool?.overdue ?? 0)} detail={`${formatNumber(pool?.double_returns ?? 0)} double returns`} tone={overdueTone} loading={loading} />
      <MetricCard label="HEAP LIVE" value={formatBytes(snapshot?.runtime.heap_bytes ?? 0)} detail={`${formatNumber(snapshot?.runtime.goroutines ?? 0)} goroutines`} loading={loading} />
    </div>
  );
}
