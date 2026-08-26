import { useCallback, useEffect, useRef, useState } from 'react';
import { api } from '../api/client';
import { logger } from '../lib/logger';
import type { LeaseFilter, LeaseSummary } from '../types';

interface LeaseStateValue {
  leases: LeaseSummary[];
  loading: boolean;
  refreshing: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useLeases(filter: LeaseFilter): LeaseStateValue {
  const [leases, setLeases] = useState<LeaseSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const mounted = useRef(true);
  const controller = useRef<AbortController | null>(null);

  const load = useCallback(async (initial = false) => {
    controller.current?.abort();
    const nextController = new AbortController();
    controller.current = nextController;
    if (initial) setLoading(true);
    else setRefreshing(true);
    try {
      const page = await api.getLeases(filter, nextController.signal);
      if (!mounted.current || nextController.signal.aborted) return;
      setLeases(page.data ?? []);
      setError(null);
    } catch (reason) {
      if (nextController.signal.aborted || !mounted.current) return;
      setError(reason instanceof Error ? reason.message : '无法加载 lease 数据');
      logger.warn('Lease request failed', reason);
    } finally {
      if (mounted.current && !nextController.signal.aborted) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [filter]);

  useEffect(() => {
    mounted.current = true;
    void load(true);
    const timer = window.setInterval(() => void load(), 2_000);
    return () => {
      mounted.current = false;
      controller.current?.abort();
      window.clearInterval(timer);
    };
  }, [load]);

  return { leases, loading, refreshing, error, refresh: () => load(false) };
}
