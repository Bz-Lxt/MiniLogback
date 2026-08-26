import { useEffect, useState } from 'react';
import { api } from './api/client';
import { DemoControls } from './components/DemoControls';
import { Header } from './components/Header';
import { LeaseAudit } from './components/LeaseAudit';
import { MetricGrid } from './components/MetricGrid';
import { PanelHeading } from './components/PanelHeading';
import { PipelineRail } from './components/PipelineRail';
import { RingGauge } from './components/RingGauge';
import { useLeases } from './hooks/useLeases';
import { useTelemetry } from './hooks/useTelemetry';
import { logger } from './lib/logger';
import type { EffectiveConfig, LeaseFilter } from './types';

export default function App() {
  const telemetry = useTelemetry();
  const [filter, setFilter] = useState<LeaseFilter>('all');
  const leaseFeed = useLeases(filter);
  const [config, setConfig] = useState<EffectiveConfig | null>(null);

  useEffect(() => {
    let active = true;
    api.getConfig()
      .then((value) => active && setConfig(value))
      .catch((reason) => logger.warn('Effective config is unavailable; demo controls remain hidden', reason));
    return () => { active = false; };
  }, []);

  const snapshot = telemetry.snapshot;
  const demoMode = config?.demo_allowed === true;
  const isInitialError = telemetry.mode === 'error' && snapshot === null;

  return (
    <div className="app-shell">
      <div className="ambient-grid" aria-hidden="true" />
      <Header
        mode={telemetry.mode}
        stale={telemetry.stale}
        sequence={snapshot?.sequence}
        sampledAt={snapshot?.sampled_at}
        demoMode={demoMode}
      />

      <main id="main-content">
        {telemetry.error && (
          <div className={isInitialError ? 'system-banner system-banner--error' : 'system-banner'} role="alert">
            <span><strong>{isInitialError ? 'TELEMETRY OFFLINE' : 'STREAM DEGRADED'}</strong>{telemetry.error}</span>
            <button className="text-button" type="button" onClick={telemetry.retry}>RECONNECT</button>
          </div>
        )}

        <section className="telemetry-panel overview-panel" aria-labelledby="overview-title">
          <PanelHeading index="01" eyebrow="REALTIME / MPSC CORE" title="Ring Buffer 流量水位" titleId="overview-title">
            <div className="heading-legend" aria-label="水位阈值">
              <span><i className="legend-dot legend-dot--healthy" />&lt;60</span>
              <span><i className="legend-dot legend-dot--warning" />60–85</span>
              <span><i className="legend-dot legend-dot--danger" />&gt;85</span>
            </div>
          </PanelHeading>
          <div className="overview-grid">
            <RingGauge
              percent={snapshot?.ring.watermark_percent ?? 0}
              depth={snapshot?.ring.depth ?? 0}
              capacity={snapshot?.ring.capacity ?? config?.ring_capacity ?? 0}
            />
            <MetricGrid snapshot={snapshot} />
          </div>
        </section>

        <PipelineRail snapshot={snapshot} />

        <LeaseAudit
          leases={leaseFeed.leases}
          loading={leaseFeed.loading}
          refreshing={leaseFeed.refreshing}
          error={leaseFeed.error}
          filter={filter}
          overdueCount={snapshot?.pool.overdue ?? leaseFeed.leases.filter((lease) => lease.state === 'overdue').length}
          onFilterChange={setFilter}
          onRefresh={() => void leaseFeed.refresh()}
        />

        {demoMode && <DemoControls onMutated={() => void leaseFeed.refresh()} />}
      </main>

      <footer className="app-footer">
        <span>MINILOGBACK / COPY-AVOIDING ASYNC APPENDER</span>
        <span>{config ? `${config.sink_type.toUpperCase()} SINK · ${config.batch_size} BATCH · ${config.flush_interval}` : 'READING EFFECTIVE CONFIG'}</span>
      </footer>
    </div>
  );
}
