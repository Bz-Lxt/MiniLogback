import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { api } from '../api/client';
import { overdueLease } from '../test/fixtures';
import { DemoControls } from './DemoControls';

describe('DemoControls', () => {
  it('starts bounded traffic with the configured values', async () => {
    const request = vi.spyOn(api, 'startTraffic').mockResolvedValue();
    render(<DemoControls onMutated={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: /inject traffic/i }));
    expect(request).toHaveBeenCalledWith({ events_per_second: 25_000, duration_seconds: 10, payload_bytes: 256 });
    expect(await screen.findByRole('status')).toHaveTextContent('25,000 evt/s');
  });

  it('creates and releases a retained lease', async () => {
    vi.spyOn(api, 'createDemoLease').mockResolvedValue(overdueLease);
    const release = vi.spyOn(api, 'releaseDemoLease').mockResolvedValue();
    render(<DemoControls onMutated={vi.fn()} />);
    await userEvent.click(screen.getByRole('button', { name: /retain one lease/i }));
    expect(await screen.findByRole('button', { name: /release lease #91/i })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /release lease #91/i }));
    expect(release).toHaveBeenCalledWith(91);
  });
});
