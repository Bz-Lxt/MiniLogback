import { expect, test } from '@playwright/test';

test.use({ viewport: { width: 480, height: 900 } });
test.skip(process.env.E2E_LIVE !== '1', 'Set E2E_LIVE=1 to exercise a running Compose stack without mocks.');

test('validates the live same-origin telemetry and overdue lease lifecycle', async ({ page }) => {
  const browserErrors: string[] = [];
  let retainedLeaseID: number | null = null;

  page.on('pageerror', (error) => browserErrors.push(`pageerror: ${error.message}`));
  page.on('console', (message) => {
    if (message.type() === 'error') browserErrors.push(`console: ${message.text()}`);
  });

  try {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    await expect(page).toHaveTitle(/MiniLogback/i);
    await expect(page.getByRole('heading', { name: 'Ring Buffer 流量水位' })).toBeVisible();
    await expect(page.locator('.ring-gauge canvas')).toBeVisible();
    await expect(page.locator('.ring-gauge .sr-only[role="status"]')).toContainText('Ring Buffer 当前水位');
    await expect(page.getByText('SSE LIVE', { exact: true })).toBeVisible({ timeout: 15_000 });

    const sequence = page.locator('.sample-meta b').first();
    const initialSequence = await sequence.textContent();
    expect(initialSequence?.trim()).toMatch(/^\d[\d,]*$/);

    await expect(page.getByText('DEMO MODE', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: /inject traffic/i }).click();
    await expect(page.getByRole('status').filter({ hasText: /已启动 .* evt\/s/ })).toBeVisible();
    await expect.poll(async () => (await sequence.textContent())?.trim(), { timeout: 8_000 })
      .not.toBe(initialSequence?.trim());

    await page.getByRole('button', { name: /retain one lease/i }).click();
    const releaseButton = page.getByRole('button', { name: /release lease #\d+/i });
    await expect(releaseButton).toBeVisible();
    const releaseLabel = (await releaseButton.textContent()) ?? '';
    const leaseMatch = releaseLabel.match(/#(\d+)/);
    expect(leaseMatch, `Expected a lease ID in button label: ${releaseLabel}`).not.toBeNull();
    retainedLeaseID = Number(leaseMatch![1]);

    // The frozen live contract marks a retained lease overdue after the 2s safety window.
    await page.waitForTimeout(2_300);
    await page.getByRole('button', { name: 'OVERDUE', exact: true }).click();
    await page.getByRole('button', { name: '刷新 lease 列表' }).click();

    const leaseRow = page.getByRole('button', { name: `查看 lease ${retainedLeaseID} 的申请堆栈` });
    await expect(leaseRow).toBeVisible({ timeout: 8_000 });
    await expect(leaseRow).toHaveClass(/lease-row--overdue/);
    await expect(leaseRow.locator('.state-badge')).toHaveClass(/state-badge--overdue/);
    await expect(leaseRow.locator('.state-badge')).toContainText('OVERDUE');

    await leaseRow.click();
    const drawer = page.getByRole('dialog');
    await expect(drawer).toBeVisible();
    await expect(drawer.locator('.lease-signal')).toHaveClass(/lease-signal--overdue/);
    await expect(drawer.locator('.lease-signal')).toContainText('OVERDUE');

    const allocationSource = drawer.locator('.lease-facts__wide dd').first();
    await expect(allocationSource).not.toHaveText('');
    await expect(allocationSource).not.toHaveText('—');
    const stackFrames = drawer.locator('.stack-list li');
    await expect(stackFrames.first()).toBeVisible();
    expect(await stackFrames.count()).toBeGreaterThan(0);
    await expect(stackFrames.first().locator('code')).toContainText(':');

    await page.getByRole('button', { name: '关闭堆栈面板' }).click();
    await releaseButton.click();
    await expect(page.getByRole('status').filter({ hasText: `诊断 lease #${retainedLeaseID} 已归还` })).toBeVisible();
    retainedLeaseID = null;

    const viewportFits = await page.evaluate(
      () => document.documentElement.scrollWidth === document.documentElement.clientWidth,
    );
    expect(viewportFits, '480px viewport must not introduce page-level horizontal overflow').toBe(true);
    expect(browserErrors, 'Live page emitted browser errors').toEqual([]);
  } finally {
    if (retainedLeaseID !== null) {
      const releaseURL = new URL(`/api/v1/demo/leases/${retainedLeaseID}`, page.url()).toString();
      await page.request.delete(releaseURL).catch(() => undefined);
    }
  }
});

test('keeps 100 live telemetry samples within the local display latency budget', async ({ page }) => {
  test.setTimeout(30_000);
  await page.goto('/', { waitUntil: 'domcontentloaded' });
  await expect(page.getByText('SSE LIVE', { exact: true })).toBeVisible({ timeout: 15_000 });

  const latency = await page.evaluate(async () => {
    const sequence = document.querySelector<HTMLElement>('.sample-meta b');
    const sampledAt = document.querySelector<HTMLTimeElement>('.sample-meta time');
    if (!sequence || !sampledAt) throw new Error('telemetry sample metadata is unavailable');

    return new Promise<{ samples: number; p95_ms: number; max_ms: number }>((resolve, reject) => {
      const values: number[] = [];
      let previous = '';
      const timeout = window.setTimeout(() => {
        observer.disconnect();
        reject(new Error(`collected only ${values.length}/100 telemetry samples`));
      }, 15_000);
      const collect = () => {
        const current = sequence.textContent?.trim() ?? '';
        if (!current || current === previous) return;
        previous = current;
        const serverTime = Date.parse(sampledAt.dateTime);
        if (Number.isFinite(serverTime)) values.push(Math.max(0, Date.now() - serverTime));
        if (values.length < 100) return;
        window.clearTimeout(timeout);
        observer.disconnect();
        values.sort((left, right) => left - right);
        resolve({
          samples: values.length,
          p95_ms: values[Math.ceil(values.length * 0.95) - 1],
          max_ms: values[values.length - 1],
        });
      };
      const observer = new MutationObserver(collect);
      observer.observe(sequence, { childList: true, characterData: true, subtree: true });
      collect();
    });
  });

  expect(latency.samples).toBeGreaterThanOrEqual(100);
  expect(latency.p95_ms).toBeLessThanOrEqual(500);
  expect(latency.max_ms).toBeLessThanOrEqual(1_500);
});
