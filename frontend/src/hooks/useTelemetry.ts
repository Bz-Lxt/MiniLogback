import { useCallback, useEffect, useRef, useState } from 'react';
import { api } from '../api/client';
import { logger } from '../lib/logger';
import type { ConnectionMode, MetricsSnapshot } from '../types';

const POLL_INTERVAL_MS = 2_000;
const RECONNECT_INTERVAL_MS = 5_000;
const STALE_AFTER_MS = 5_000;

interface TelemetryState {
  snapshot: MetricsSnapshot | null;
  mode: ConnectionMode;
  stale: boolean;
  error: string | null;
  retry: () => void;
}

function isSnapshotEnvelope(value: unknown): value is { data: MetricsSnapshot } {
  if (!value || typeof value !== 'object' || !('data' in value)) return false;
  const data = (value as { data?: Partial<MetricsSnapshot> }).data;
  return Boolean(data && typeof data.sequence === 'number' && typeof data.sampled_at === 'string');
}

export function useTelemetry(): TelemetryState {
  const [snapshot, setSnapshot] = useState<MetricsSnapshot | null>(null);
  const [mode, setMode] = useState<ConnectionMode>('loading');
  const [stale, setStale] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [generation, setGeneration] = useState(0);
  const lastReceivedAt = useRef(0);
  const highestSequence = useRef(-1);

  const acceptSnapshot = useCallback((next: MetricsSnapshot) => {
    if (next.sequence < highestSequence.current) return;
    highestSequence.current = next.sequence;
    lastReceivedAt.current = Date.now();
    setSnapshot(next);
    setStale(false);
    setError(null);
  }, []);

  useEffect(() => {
    let disposed = false;
    let eventSource: EventSource | null = null;
    let pollTimer: number | null = null;
    let reconnectTimer: number | null = null;
    let staleTimer: number | null = null;
    let polling = false;

    const stopPolling = () => {
      if (pollTimer !== null) window.clearInterval(pollTimer);
      pollTimer = null;
      polling = false;
    };

    const poll = async () => {
      if (disposed || polling) return;
      polling = true;
      try {
        acceptSnapshot(await api.getMetrics());
        if (!eventSource || eventSource.readyState !== EventSource.OPEN) setMode('degraded');
      } catch (reason) {
        const message = reason instanceof Error ? reason.message : '遥测轮询失败';
        setError(message);
        if (highestSequence.current < 0) setMode('error');
        logger.warn('Metrics poll failed', reason);
      } finally {
        polling = false;
      }
    };

    const startPolling = () => {
      if (pollTimer !== null || disposed) return;
      setMode(highestSequence.current < 0 ? 'loading' : 'degraded');
      void poll();
      pollTimer = window.setInterval(() => void poll(), POLL_INTERVAL_MS);
    };

    const scheduleReconnect = () => {
      if (disposed || reconnectTimer !== null || typeof EventSource === 'undefined') return;
      reconnectTimer = window.setTimeout(() => {
        reconnectTimer = null;
        openSSE();
      }, RECONNECT_INTERVAL_MS);
    };

    const openSSE = () => {
      if (disposed || typeof EventSource === 'undefined') {
        startPolling();
        return;
      }
      eventSource?.close();
      eventSource = new EventSource('/api/v1/metrics/stream');

      eventSource.addEventListener('metrics', (event) => {
        try {
          const parsed: unknown = JSON.parse((event as MessageEvent<string>).data);
          if (!isSnapshotEnvelope(parsed)) throw new Error('invalid metrics envelope');
          acceptSnapshot(parsed.data);
          setMode('live');
          stopPolling();
        } catch (reason) {
          logger.warn('Discarded malformed SSE metrics event', reason);
        }
      });

      eventSource.onerror = () => {
        if (disposed) return;
        eventSource?.close();
        eventSource = null;
        setMode(highestSequence.current < 0 ? 'loading' : 'degraded');
        startPolling();
        scheduleReconnect();
      };
    };

    const boot = async () => {
      setMode('loading');
      try {
        acceptSnapshot(await api.getMetrics());
      } catch (reason) {
        const message = reason instanceof Error ? reason.message : '无法加载遥测数据';
        setError(message);
        logger.warn('Initial metrics request failed', reason);
      }
      if (!disposed) openSSE();
    };

    staleTimer = window.setInterval(() => {
      const age = Date.now() - lastReceivedAt.current;
      setStale(lastReceivedAt.current > 0 && age > STALE_AFTER_MS);
    }, 1_000);

    void boot();

    return () => {
      disposed = true;
      eventSource?.close();
      stopPolling();
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
      if (staleTimer !== null) window.clearInterval(staleTimer);
    };
  }, [acceptSnapshot, generation]);

  const retry = useCallback(() => {
    highestSequence.current = -1;
    lastReceivedAt.current = 0;
    setError(null);
    setStale(false);
    setGeneration((value) => value + 1);
  }, []);

  return { snapshot, mode, stale, error, retry };
}
