import { describe, expect, it } from 'vitest';
import { clampPercent, formatBytes, formatDuration, formatTimestamp, waterlineSeverity } from './format';

describe('telemetry formatting', () => {
  it('uses the required timestamp shape', () => {
    expect(formatTimestamp('2026-08-26T08:00:00Z')).toMatch(/^2026-08-26 \d{2}:00:00$/);
    expect(formatTimestamp('bad input')).toBe('—');
  });

  it('formats operational units', () => {
    expect(formatBytes(1024)).toBe('1.0 KiB');
    expect(formatDuration(3_123)).toBe('3.1 s');
  });

  it('clamps waterline values and applies frozen thresholds', () => {
    expect(clampPercent(102)).toBe(100);
    expect(clampPercent(-5)).toBe(0);
    expect(waterlineSeverity(59.9)).toBe('healthy');
    expect(waterlineSeverity(60)).toBe('warning');
    expect(waterlineSeverity(85)).toBe('warning');
    expect(waterlineSeverity(85.1)).toBe('danger');
  });
});
