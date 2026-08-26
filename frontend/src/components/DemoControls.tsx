import { useState } from 'react';
import { api } from '../api/client';
import type { LogLevel, TrafficRequest } from '../types';
import { ArrowIcon, DatabaseIcon, PulseIcon } from './Icons';
import { PanelHeading } from './PanelHeading';

interface Props {
  onMutated: () => void;
}

export function DemoControls({ onMutated }: Props) {
  const [traffic, setTraffic] = useState<TrafficRequest>({ events_per_second: 25_000, duration_seconds: 10, payload_bytes: 256 });
  const [leaseSize, setLeaseSize] = useState(512);
  const [leaseLevel, setLeaseLevel] = useState<LogLevel>('error');
  const [heldLeaseID, setHeldLeaseID] = useState<number | null>(null);
  const [busy, setBusy] = useState<'traffic' | 'lease' | 'release' | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const run = async (kind: typeof busy, action: () => Promise<void>) => {
    setBusy(kind);
    setError(null);
    setMessage(null);
    try {
      await action();
      onMutated();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '诊断动作失败');
    } finally {
      setBusy(null);
    }
  };

  const startTraffic = (event: React.FormEvent) => {
    event.preventDefault();
    void run('traffic', async () => {
      await api.startTraffic(traffic);
      setMessage(`已启动 ${traffic.events_per_second.toLocaleString('en-US')} evt/s，持续 ${traffic.duration_seconds}s`);
    });
  };

  const createLease = (event: React.FormEvent) => {
    event.preventDefault();
    void run('lease', async () => {
      const lease = await api.createDemoLease({ size_bytes: leaseSize, level: leaseLevel });
      setHeldLeaseID(lease.id);
      setMessage(`诊断 lease #${lease.id} 已暂留，超过阈值后将触发红色告警`);
    });
  };

  const releaseLease = () => {
    if (heldLeaseID === null) return;
    const id = heldLeaseID;
    void run('release', async () => {
      await api.releaseDemoLease(id);
      setHeldLeaseID(null);
      setMessage(`诊断 lease #${id} 已归还 BytePool`);
    });
  };

  return (
    <section className="demo-panel telemetry-panel" aria-labelledby="demo-title">
      <PanelHeading index="03" eyebrow="AUTHORIZED DIAGNOSTICS" title="演示流量与故障注入" titleId="demo-title">
        <span className="demo-panel__warning">LOCAL / CONTAINER ONLY</span>
      </PanelHeading>
      <div className="demo-panel__grid">
        <form className="demo-tool" onSubmit={startTraffic}>
          <div className="demo-tool__heading"><PulseIcon /><div><strong>TRAFFIC GENERATOR</strong><small>有界日志流量发生器</small></div></div>
          <div className="field-grid">
            <label><span>EVENTS / SEC</span><input type="number" min="1" max="1000000" value={traffic.events_per_second} onChange={(e) => setTraffic({ ...traffic, events_per_second: Number(e.target.value) })} /></label>
            <label><span>DURATION</span><span className="input-unit"><input type="number" min="1" max="60" value={traffic.duration_seconds} onChange={(e) => setTraffic({ ...traffic, duration_seconds: Number(e.target.value) })} /><i>s</i></span></label>
            <label><span>PAYLOAD</span><span className="input-unit"><input type="number" min="32" max="65536" value={traffic.payload_bytes} onChange={(e) => setTraffic({ ...traffic, payload_bytes: Number(e.target.value) })} /><i>B</i></span></label>
          </div>
          <button className="button button--signal" type="submit" disabled={busy !== null}>
            {busy === 'traffic' ? 'STARTING…' : 'INJECT TRAFFIC'}<ArrowIcon />
          </button>
        </form>

        <form className="demo-tool" onSubmit={createLease}>
          <div className="demo-tool__heading"><DatabaseIcon /><div><strong>LEASE RETENTION PROBE</strong><small>故意暂留池化字节块</small></div></div>
          <div className="field-grid field-grid--lease">
            <label><span>SIZE</span><span className="input-unit"><input type="number" min="32" max="1048576" value={leaseSize} onChange={(e) => setLeaseSize(Number(e.target.value))} /><i>B</i></span></label>
            <label><span>LEVEL</span><select value={leaseLevel} onChange={(e) => setLeaseLevel(e.target.value as LogLevel)}><option value="debug">DEBUG</option><option value="info">INFO</option><option value="warn">WARN</option><option value="error">ERROR</option></select></label>
          </div>
          {heldLeaseID === null ? (
            <button className="button button--danger" type="submit" disabled={busy !== null}>{busy === 'lease' ? 'BORROWING…' : 'RETAIN ONE LEASE'}<ArrowIcon /></button>
          ) : (
            <button className="button" type="button" onClick={releaseLease} disabled={busy !== null}>{busy === 'release' ? 'RELEASING…' : `RELEASE LEASE #${heldLeaseID}`}<ArrowIcon /></button>
          )}
        </form>
      </div>
      {(message || error) && <p className={error ? 'action-result action-result--error' : 'action-result'} role={error ? 'alert' : 'status'}>{error ?? message}</p>}
    </section>
  );
}
