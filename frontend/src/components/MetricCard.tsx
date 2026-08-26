import type { ReactNode } from 'react';

interface Props {
  label: string;
  value: string;
  unit?: string;
  detail?: ReactNode;
  tone?: 'normal' | 'signal' | 'warning' | 'danger';
  loading?: boolean;
}

export function MetricCard({ label, value, unit, detail, tone = 'normal', loading = false }: Props) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <span className="metric-card__label">{label}</span>
      <div className={loading ? 'metric-card__value is-loading' : 'metric-card__value'}>
        {loading ? '····' : value}
        {!loading && unit && <small>{unit}</small>}
      </div>
      <div className="metric-card__detail">{loading ? 'ACQUIRING SIGNAL' : detail}</div>
    </article>
  );
}
