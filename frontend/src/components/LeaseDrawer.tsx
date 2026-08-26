import { useEffect, useRef } from 'react';
import { formatBytes, formatDuration, formatTimestamp } from '../lib/format';
import type { LeaseDetail } from '../types';
import { CloseIcon } from './Icons';

interface Props {
  lease: LeaseDetail | null;
  loading: boolean;
  error: string | null;
  onClose: () => void;
  onRetry: () => void;
}

export function LeaseDrawer({ lease, loading, error, onClose, onRetry }: Props) {
  const panelRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    closeRef.current?.focus();

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
        return;
      }
      if (event.key !== 'Tab' || !panelRef.current) return;
      const focusable = Array.from(
        panelRef.current.querySelectorAll<HTMLElement>('button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])'),
      );
      if (focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener('keydown', onKeyDown);
      previous?.focus();
    };
  }, [onClose]);

  return (
    <div className="drawer-layer" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <div
        className="lease-drawer"
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="lease-drawer-title"
        aria-describedby="lease-drawer-description"
      >
        <header className="lease-drawer__header">
          <div>
            <span className="eyebrow">BYTE STREAM / OWNERSHIP TRACE</span>
            <h2 id="lease-drawer-title">LEASE #{lease?.id ?? '—'}</h2>
          </div>
          <button className="icon-button" ref={closeRef} type="button" onClick={onClose} aria-label="关闭堆栈面板">
            <CloseIcon />
          </button>
        </header>

        <div className="lease-drawer__body" id="lease-drawer-description">
          {loading && <div className="drawer-state" role="status"><span className="spinner" />正在解析申请堆栈…</div>}
          {error && (
            <div className="drawer-state drawer-state--error" role="alert">
              <strong>堆栈读取失败</strong>
              <p>{error}</p>
              <button className="button button--small" type="button" onClick={onRetry}>重试</button>
            </div>
          )}
          {lease && !loading && !error && (
            <>
              <div className={`lease-signal lease-signal--${lease.state}`}>
                <span className="lease-signal__beacon" aria-hidden="true" />
                <div><small>OWNERSHIP STATE</small><strong>{lease.state.toUpperCase()}</strong></div>
                <div><small>LEASE AGE</small><strong>{formatDuration(lease.age_millis)}</strong></div>
              </div>
              <dl className="lease-facts">
                <div><dt>Borrowed</dt><dd>{formatTimestamp(lease.borrowed_at)}</dd></div>
                <div><dt>Deadline</dt><dd>{formatTimestamp(lease.deadline)}</dd></div>
                <div><dt>Size class</dt><dd>{formatBytes(lease.size_class)}</dd></div>
                <div><dt>Payload length</dt><dd>{formatBytes(lease.length)}</dd></div>
                <div className="lease-facts__wide"><dt>Allocation site</dt><dd>{lease.source}</dd></div>
                <div className="lease-facts__wide"><dt>Function</dt><dd>{lease.function}</dd></div>
              </dl>
              <div className="stack-heading">
                <span>CAPTURED STACK</span>
                <small>{lease.stack.length} FRAMES</small>
              </div>
              {lease.stack.length === 0 ? (
                <p className="drawer-empty">此审计模式未保留堆栈帧。</p>
              ) : (
                <ol className="stack-list">
                  {lease.stack.map((frame, index) => (
                    <li key={`${frame.source}:${frame.line}:${index}`}>
                      <span className="stack-list__index">{String(index).padStart(2, '0')}</span>
                      <div><strong>{frame.function}</strong><code>{frame.source}:{frame.line}</code></div>
                    </li>
                  ))}
                </ol>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
