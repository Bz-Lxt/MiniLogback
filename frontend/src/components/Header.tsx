import { formatTimestamp } from '../lib/format';
import type { ConnectionMode } from '../types';
import { ConnectionPill } from './ConnectionPill';

interface Props {
  mode: ConnectionMode;
  stale: boolean;
  sequence?: number;
  sampledAt?: string;
  demoMode: boolean;
}

export function Header({ mode, stale, sequence, sampledAt, demoMode }: Props) {
  return (
    <header className="masthead">
      <a className="brand" href="#main-content" aria-label="MiniLogback dashboard home">
        <span className="brand__mark" aria-hidden="true"><i /><i /><i /></span>
        <span>
          <strong>MINILOGBACK</strong>
          <small>ASYNC PIPELINE / TELEMETRY CORE</small>
        </span>
      </a>
      <div className="masthead__meta">
        {demoMode && <span className="demo-badge">DEMO MODE</span>}
        <span className="sample-meta">
          <small>SEQ</small>
          <b>{sequence === undefined ? '—' : sequence.toLocaleString('en-US')}</b>
        </span>
        <span className="sample-meta sample-meta--time">
          <small>LAST SAMPLE</small>
          <time dateTime={sampledAt}>{formatTimestamp(sampledAt)}</time>
        </span>
        <ConnectionPill mode={mode} stale={stale} />
      </div>
    </header>
  );
}
