import { useCallback, useState } from 'react';
import { api } from '../api/client';
import { formatBytes, formatDuration, formatTimestamp } from '../lib/format';
import type { LeaseDetail, LeaseFilter, LeaseSummary } from '../types';
import { AlertIcon, RefreshIcon } from './Icons';
import { LeaseDrawer } from './LeaseDrawer';
import { PanelHeading } from './PanelHeading';

interface Props {
  leases: LeaseSummary[];
  loading: boolean;
  refreshing: boolean;
  error: string | null;
  filter: LeaseFilter;
  overdueCount: number;
  onFilterChange: (filter: LeaseFilter) => void;
  onRefresh: () => void;
}

const FILTERS: { value: LeaseFilter; label: string }[] = [
  { value: 'all', label: 'ALL' },
  { value: 'overdue', label: 'OVERDUE' },
  { value: 'borrowed', label: 'BORROWED' },
  { value: 'queued', label: 'QUEUED' },
  { value: 'in_flight', label: 'IN FLIGHT' },
  { value: 'returned', label: 'RETURNED' },
];

export function LeaseAudit({ leases, loading, refreshing, error, filter, overdueCount, onFilterChange, onRefresh }: Props) {
  const [selectedID, setSelectedID] = useState<number | null>(null);
  const [detail, setDetail] = useState<LeaseDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  const loadDetail = useCallback(async (id: number) => {
    setSelectedID(id);
    setDetailLoading(true);
    setDetailError(null);
    try {
      setDetail(await api.getLease(id));
    } catch (reason) {
      setDetail(null);
      setDetailError(reason instanceof Error ? reason.message : '无法读取 lease 详情');
    } finally {
      setDetailLoading(false);
    }
  }, []);

  const closeDrawer = useCallback(() => {
    setSelectedID(null);
    setDetail(null);
    setDetailError(null);
  }, []);

  const onRowKeyDown = (event: React.KeyboardEvent, id: number) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      void loadDetail(id);
    }
  };

  return (
    <section className="audit-panel telemetry-panel" aria-labelledby="audit-title">
      <PanelHeading index="02" eyebrow="SLAB / BYTEPOOL FORENSICS" title="字节流内存泄漏审计中心" titleId="audit-title">
        <div className={overdueCount > 0 ? 'alarm-beacon alarm-beacon--active' : 'alarm-beacon'} aria-live="polite">
          <AlertIcon />
          <span><small>OVERDUE</small><strong>{overdueCount}</strong></span>
        </div>
      </PanelHeading>

      <div className="audit-toolbar">
        <div className="filter-tabs" role="group" aria-label="按 lease 状态筛选">
          {FILTERS.map((item) => (
            <button
              type="button"
              key={item.value}
              className={filter === item.value ? 'filter-tab is-active' : 'filter-tab'}
              aria-pressed={filter === item.value}
              onClick={() => onFilterChange(item.value)}
            >
              {item.label}
            </button>
          ))}
        </div>
        <button className="icon-button" type="button" aria-label="刷新 lease 列表" onClick={onRefresh} disabled={refreshing}>
          <RefreshIcon className={refreshing ? 'is-spinning' : ''} />
        </button>
      </div>

      {error && (
        <div className="inline-error" role="alert">
          <span><strong>LEASE FEED ERROR</strong>{error}</span>
          <button type="button" className="text-button" onClick={onRefresh}>RETRY</button>
        </div>
      )}

      <div className="table-scroll" aria-busy={loading}>
        <table className="lease-table">
          <thead>
            <tr>
              <th scope="col">LEASE</th>
              <th scope="col">STATE</th>
              <th scope="col">CLASS / LEN</th>
              <th scope="col">AGE</th>
              <th scope="col">BORROWED AT</th>
              <th scope="col">ALLOCATION SITE</th>
              <th scope="col">LEVEL</th>
            </tr>
          </thead>
          <tbody>
            {loading && Array.from({ length: 4 }, (_, index) => (
              <tr className="skeleton-row" key={index} aria-hidden="true">
                <td colSpan={7}><span /></td>
              </tr>
            ))}
            {!loading && leases.map((lease) => (
              <tr
                key={lease.id}
                className={lease.state === 'overdue' ? 'lease-row lease-row--overdue' : 'lease-row'}
                tabIndex={0}
                role="button"
                aria-label={`查看 lease ${lease.id} 的申请堆栈`}
                onClick={() => void loadDetail(lease.id)}
                onKeyDown={(event) => onRowKeyDown(event, lease.id)}
              >
                <td><b className="lease-id">#{lease.id}</b></td>
                <td><span className={`state-badge state-badge--${lease.state}`}><i aria-hidden="true" />{lease.state.replace('_', ' ').toUpperCase()}</span></td>
                <td><span className="data-pair"><b>{formatBytes(lease.size_class)}</b><small>{formatBytes(lease.length)}</small></span></td>
                <td className={lease.state === 'overdue' ? 'danger-text' : ''}>{formatDuration(lease.age_millis)}</td>
                <td><time dateTime={lease.borrowed_at}>{formatTimestamp(lease.borrowed_at)}</time></td>
                <td><span className="source-cell"><code>{lease.source}</code><small>{lease.function}</small></span></td>
                <td><span className={`level level--${lease.level}`}>{lease.level.toUpperCase()}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
        {!loading && leases.length === 0 && !error && (
          <div className="empty-state">
            <span className="empty-state__radar" aria-hidden="true"><i /></span>
            <strong>NO MATCHING LEASES</strong>
            <p>{filter === 'all' ? '审计器当前没有可展示的租约记录。' : `没有处于 ${filter.replace('_', ' ')} 状态的租约。`}</p>
          </div>
        )}
      </div>

      {selectedID !== null && (
        <LeaseDrawer
          lease={detail}
          loading={detailLoading}
          error={detailError}
          onClose={closeDrawer}
          onRetry={() => void loadDetail(selectedID)}
        />
      )}
    </section>
  );
}
