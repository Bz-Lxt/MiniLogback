import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { RingGauge } from './RingGauge';

describe('RingGauge', () => {
  it('exposes the real numeric waterline as accessible text', () => {
    render(<RingGauge percent={91.25} depth={59802} capacity={65536} />);
    expect(screen.getByRole('status')).toHaveTextContent('91.3%');
    expect(screen.getByRole('status')).toHaveTextContent('danger');
    expect(screen.getByText('DANGER')).toBeInTheDocument();
  });

  it('never displays values outside zero to one hundred percent', () => {
    render(<RingGauge percent={120} depth={70000} capacity={65536} />);
    expect(screen.getByText('100.0')).toBeInTheDocument();
  });
});
