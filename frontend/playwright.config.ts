import { defineConfig, devices } from '@playwright/test';

const localTestURL = 'http://127.0.0.1:28642';

export default defineConfig({
  testDir: './tests',
  testMatch: /(e2e_flow|live_flow)\.spec\.ts/,
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: 'list',
  use: {
    baseURL: process.env.E2E_BASE_URL ?? localTestURL,
    channel: process.env.PLAYWRIGHT_CHANNEL || undefined,
    trace: 'retain-on-failure',
  },
  webServer: process.env.E2E_BASE_URL
    ? undefined
    : {
        command: 'vite --host 127.0.0.1 --port 28642 --strictPort',
        url: localTestURL,
        reuseExistingServer: false,
      },
  projects: [
    { name: 'desktop', use: { ...devices['Desktop Chrome'] } },
    { name: 'tablet-768', use: { viewport: { width: 768, height: 900 } } },
    { name: 'mobile-480', use: { viewport: { width: 480, height: 900 } } },
  ],
});
