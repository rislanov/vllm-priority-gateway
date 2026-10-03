import {defineConfig} from '@playwright/test';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

export default defineConfig({
  testDir: '.',
  testMatch: '*.spec.mjs',
  workers: 1,
  retries: 0,
  reporter: 'list',
  outputDir: join(tmpdir(), `llmgw-browser-results-${process.pid}`),
  use: {
    browserName: 'chromium',
    baseURL: 'http://127.0.0.1:18081',
    httpCredentials: {username: 'browser-test', password: 'browser-test-password-local'},
    launchOptions: {args: ['--host-resolver-rules=MAP gateway.test 127.0.0.1', '--no-proxy-server']},
    // Secret-bearing pages must not be retained as automatic screenshots/traces.
    screenshot: 'off', trace: 'off', video: 'off',
  },
  webServer: {
    command: 'node start-gateway.mjs',
    url: 'http://127.0.0.1:18081/healthz',
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
