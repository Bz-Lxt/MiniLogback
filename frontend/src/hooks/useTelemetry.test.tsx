import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api/client';
import { metricsFixture } from '../test/fixtures';
import { useTelemetry } from './useTelemetry';

class EventSourceStub {
  static readonly OPEN = 1;
  static latest: EventSourceStub;
  readyState = 0;
  onerror: ((event: Event) => void) | null = null;
  close = vi.fn();

  constructor() {
    EventSourceStub.latest = this;
  }

  addEventListener() {}
}

describe('useTelemetry', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('falls back to snapshot polling when SSE fails', async () => {
    vi.stubGlobal('EventSource', EventSourceStub);
    const metrics = vi.spyOn(api, 'getMetrics').mockResolvedValue(metricsFixture);
    const { result, unmount } = renderHook(() => useTelemetry());

    await waitFor(() => expect(result.current.snapshot?.sequence).toBe(802));
    act(() => EventSourceStub.latest.onerror?.(new Event('error')));
    await waitFor(() => expect(result.current.mode).toBe('degraded'));
    expect(metrics.mock.calls.length).toBeGreaterThanOrEqual(2);
    unmount();
  });
});
