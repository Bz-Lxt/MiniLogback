import type { ConnectionMode } from '../types';

interface Props {
  mode: ConnectionMode;
  stale: boolean;
}

const labels: Record<ConnectionMode, string> = {
  loading: 'CONNECTING',
  live: 'SSE LIVE',
  degraded: 'POLLING 2S',
  error: 'OFFLINE',
};

export function ConnectionPill({ mode, stale }: Props) {
  const state = stale ? 'stale' : mode;
  return (
    <span className={`connection-pill connection-pill--${state}`} role="status">
      <span className="connection-pill__dot" aria-hidden="true" />
      {stale ? 'STALE DATA' : labels[mode]}
    </span>
  );
}
