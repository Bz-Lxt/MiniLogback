import { expect, test, type Page, type Route } from '@playwright/test';

const metrics = {
  sequence: 802,
  sampled_at: new Date().toISOString(),
  ring: {
    capacity: 65536,
    depth: 9216,
    watermark_percent: 14.06,
    high_watermark: 18432,
    publish_attempts: 3000000,
    accepted: 2999988,
    consumed: 2990772,
    dropped_total: 12,
    dropped_by_level: { debug: 10, info: 2, warn: 0, error: 0 },
    publish_rate: 428000,
    consume_rate: 426900,
  },
  flusher: { batches: 2921, records: 2990772, bytes: 765242112, in_flight: 0, last_batch_size: 1024, flush_p95_micros: 740, errors: 0, mode: 'vectored' },
  pool: { borrowed_total: 3000010, returned_total: 2990794, outstanding: 9216, overdue: 1, double_returns: 0, invalid_returns: 0, hit_percent: 99.8, classes: [{ size: 256, in_use: 8000, available: 4096 }] },
  collector: { connections: 1, accepted_batches: 20, duplicate_batches: 0, invalid_frames: 0 },
  runtime: { goroutines: 12, heap_bytes: 16777216, demo_mode: true, audit_mode: 'full' },
  status: { sink: 'ok', collector: 'listening', transport: 'sse' },
};

const lease = {
  id: 91,
  state: 'overdue',
  size_class: 1024,
  length: 318,
  borrowed_at: '2026-08-26T07:59:57Z',
  deadline: '2026-08-26T07:59:59Z',
  age_millis: 3123,
  level: 'error',
  source: 'cmd/minilogbackd/demo.go:74',
  function: 'main.startDemoLeak',
};

const config = {
  ring_capacity: 65536,
  batch_size: 1024,
  flush_interval: '50ms',
  max_event_bytes: 1048576,
  audit_mode: 'full',
  lease_timeout: '2s',
  sink_type: 'file',
  sink_target: 'minilogback.log',
  demo_mode: true,
  demo_allowed: true,
  network_security: 'trusted_network_plaintext',
  capabilities: { vectored_file: true, vectored_network: true, sse: true },
};

async function installEventSource(page: Page, fail: boolean) {
  await page.addInitScript(({ shouldFail, snapshot }) => {
    class SyntheticEventSource {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSED = 2;
      readonly CONNECTING = 0;
      readonly OPEN = 1;
      readonly CLOSED = 2;
      readyState = 0;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      listeners = new Map<string, EventListener[]>();

      constructor(public url: string) {
        window.setTimeout(() => {
          if (shouldFail) {
            this.readyState = 2;
            this.onerror?.(new Event('error'));
            return;
          }
          this.readyState = 1;
          const event = new MessageEvent('metrics', { data: JSON.stringify({ data: snapshot }) });
          for (const listener of this.listeners.get('metrics') ?? []) listener(event);
        }, 40);
      }

      addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
        const callback: EventListener = typeof listener === 'function' ? listener : (event) => listener.handleEvent(event);
        this.listeners.set(type, [...(this.listeners.get(type) ?? []), callback]);
      }
      removeEventListener() {}
      dispatchEvent() { return true; }
      close() { this.readyState = 2; }
      withCredentials = false;
    }
    Object.defineProperty(window, 'EventSource', { configurable: true, value: SyntheticEventSource });
  }, { shouldFail: fail, snapshot: metrics });
}

async function installAPI(page: Page, options: { empty?: boolean; demo?: boolean } = {}) {
  let metricRequests = 0;
  let trafficPayload: unknown = null;
  let releaseCount = 0;

  await page.route('**/api/v1/**', async (route: Route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    const json = (body: unknown, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });

    if (path === '/api/v1/metrics/current') {
      metricRequests += 1;
      await json({ data: { ...metrics, sequence: 801 + metricRequests, sampled_at: new Date().toISOString() } });
      return;
    }
    if (path === '/api/v1/config/effective') {
      await json({ data: { ...config, demo_mode: options.demo ?? true, demo_allowed: options.demo ?? true } });
      return;
    }
    if (path === '/api/v1/leases' && method === 'GET') {
      await json({ data: options.empty ? [] : [lease], meta: { next_cursor: '91', has_more: false, limit: 100 } });
      return;
    }
    if (path === '/api/v1/leases/91') {
      await json({ data: { ...lease, stack: [{ function: lease.function, source: 'cmd/minilogbackd/demo.go', line: 74 }] } });
      return;
    }
    if (path === '/api/v1/demo/traffic' && method === 'POST') {
      trafficPayload = request.postDataJSON();
      await json({ data: { status: 'started', events_per_second: 25000, duration_seconds: 10 } }, 202);
      return;
    }
    if (path === '/api/v1/demo/leases' && method === 'POST') {
      await json({ data: lease }, 201);
      return;
    }
    if (path === '/api/v1/demo/leases/91' && method === 'DELETE') {
      releaseCount += 1;
      await route.fulfill({ status: 204, body: '' });
      return;
    }
    await json({ error: { code: 'not_found', message: 'not found', details: [] } }, 404);
  });

  return {
    metricRequests: () => metricRequests,
    trafficPayload: () => trafficPayload,
    releaseCount: () => releaseCount,
  };
}

test('renders live waterline, overdue trace, and functional demo actions', async ({ page }) => {
  await installEventSource(page, false);
  const calls = await installAPI(page);
  await page.goto('/');

  await expect(page.getByText('SSE LIVE')).toBeVisible();
  await expect(page.locator('.ring-gauge__readout strong')).toContainText('14.1');
  await expect(page.getByRole('cell', { name: 'OVERDUE', exact: true })).toBeVisible();

  await page.getByRole('button', { name: /查看 lease 91/ }).click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await expect(page.locator('.stack-list code')).toHaveText('cmd/minilogbackd/demo.go:74');
  await page.getByRole('button', { name: '关闭堆栈面板' }).click();

  await page.getByRole('button', { name: /inject traffic/i }).click();
  await expect.poll(calls.trafficPayload).toEqual({ events_per_second: 25000, duration_seconds: 10, payload_bytes: 256 });

  await page.getByRole('button', { name: /retain one lease/i }).click();
  await page.getByRole('button', { name: /release lease #91/i }).click();
  await expect.poll(calls.releaseCount).toBe(1);
});

test('falls back from SSE to the two-second polling transport', async ({ page }) => {
  await installEventSource(page, true);
  const calls = await installAPI(page);
  await page.goto('/');
  await expect(page.getByText('POLLING 2S')).toBeVisible();
  await expect.poll(calls.metricRequests, { timeout: 3_500 }).toBeGreaterThanOrEqual(2);
});

test('shows the empty audit state and stays within narrow viewport', async ({ page }) => {
  await installEventSource(page, false);
  await installAPI(page, { empty: true, demo: false });
  await page.goto('/');
  await expect(page.getByText('NO MATCHING LEASES')).toBeVisible();
  await expect(page.getByText('DEMO MODE')).toHaveCount(0);
  const pageOverflows = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth);
  expect(pageOverflows).toBe(false);
});
