import type { MetricsSnapshot } from '../types';

interface Props {
  snapshot: MetricsSnapshot | null;
}

function RailNode({ label, value, state = 'normal' }: { label: string; value: string; state?: 'normal' | 'good' | 'danger' }) {
  return (
    <div className={`rail-node rail-node--${state}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function PipelineRail({ snapshot }: Props) {
  const sinkOK = snapshot?.status.sink === 'ok';
  const collectorOK = snapshot?.status.collector === 'listening';
  return (
    <section className="pipeline-rail" aria-label="Pipeline runtime status">
      <span className="pipeline-rail__label">PIPELINE</span>
      <RailNode label="APPENDER" value="ASYNC" state="good" />
      <span className="rail-link" aria-hidden="true" />
      <RailNode label="BATCH I/O" value={snapshot?.flusher.mode?.toUpperCase() ?? '—'} state={snapshot ? 'good' : 'normal'} />
      <span className="rail-link" aria-hidden="true" />
      <RailNode label="SINK" value={snapshot?.status.sink?.toUpperCase() ?? '—'} state={!snapshot ? 'normal' : sinkOK ? 'good' : 'danger'} />
      <span className="rail-link" aria-hidden="true" />
      <RailNode label="COLLECTOR" value={snapshot?.status.collector?.toUpperCase() ?? '—'} state={!snapshot ? 'normal' : collectorOK ? 'good' : 'danger'} />
      <div className="pipeline-rail__runtime">
        <span>AUDIT <b>{snapshot?.runtime.audit_mode?.toUpperCase() ?? '—'}</b></span>
        <span>CONN <b>{snapshot?.collector.connections ?? '—'}</b></span>
      </div>
    </section>
  );
}
