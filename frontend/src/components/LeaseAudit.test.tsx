import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { api } from '../api/client';
import { leaseDetail, overdueLease } from '../test/fixtures';
import { LeaseAudit } from './LeaseAudit';

const baseProps = {
  loading: false,
  refreshing: false,
  error: null,
  filter: 'all' as const,
  overdueCount: 0,
  onFilterChange: vi.fn(),
  onRefresh: vi.fn(),
};

describe('LeaseAudit', () => {
  it('renders a clear empty state', () => {
    render(<LeaseAudit {...baseProps} leases={[]} />);
    expect(screen.getByText('NO MATCHING LEASES')).toBeInTheDocument();
  });

  it('marks overdue leases and opens a stack drawer from the row', async () => {
    vi.spyOn(api, 'getLease').mockResolvedValue(leaseDetail);
    render(<LeaseAudit {...baseProps} leases={[overdueLease]} overdueCount={1} />);

    const row = screen.getByRole('button', { name: /lease 91/i });
    expect(row).toHaveClass('lease-row--overdue');
    await userEvent.click(row);

    expect(await screen.findByRole('dialog')).toBeInTheDocument();
    expect(screen.getAllByText('main.startDemoLeak').length).toBeGreaterThan(0);
    expect(screen.getAllByText('cmd/minilogbackd/demo.go:74').length).toBeGreaterThan(0);
    await userEvent.click(screen.getByRole('button', { name: '关闭堆栈面板' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});
