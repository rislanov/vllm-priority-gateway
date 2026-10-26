import {test, expect} from '@playwright/test';
import {readFileSync} from 'node:fs';
import {analyticsFixtures} from './analytics-fixtures.mjs';

let fixtures;
const errors = new WeakMap();
const blockedHelper = new WeakSet();
const normalFrom = '2026-01-15T23:30:00.123456789Z';
const normalTo = '2026-01-16T02:30:00.987654321Z';
const preciseFrom = '2026-01-15T12:00:00.123456789Z';
const preciseTo = '2026-01-15T12:00:00.12345679Z';
test.beforeAll(() => { fixtures = analyticsFixtures(); });
test.afterAll(() => fixtures?.close());
test.beforeEach(({page}) => {
  const messages = [];
  page.on('pageerror', (error) => messages.push(error.message));
  page.on('console', (message) => { if (['error', 'warning'].includes(message.type())) messages.push(message.text()); });
  errors.set(page, messages);
});
test.afterEach(({page}) => {
  const messages = errors.get(page);
  expect(blockedHelper.has(page) ? messages.filter((message) => message !== 'Failed to load resource: net::ERR_FAILED') : messages).toEqual([]);
});

async function open(page, name = 'normal', transform) {
  await fixtures.route(page, name, transform);
  await page.goto(`/admin/analytics?fixture=${name}`);
  await expect(page).toHaveTitle('Usage analytics · vLLM Priority Gateway');
  await expect(page.getByRole('heading', {name: 'Usage analytics', exact: true})).toBeVisible();
}
async function apply(page) {
  const [request] = await Promise.all([
    page.waitForRequest((request) => request.isNavigationRequest() && new URL(request.url()).pathname === '/admin/analytics'),
    page.getByRole('button', {name: 'Apply', exact: true}).click(),
  ]);
  const query = new URL(request.url()).searchParams;
  expect(request.method()).toBe('GET');
  for (const name of ['from_local', 'to_local', 'range', 'offset']) expect(query.has(name)).toBe(false);
  for (const name of ['from', 'to', 'client_id', 'model_pool_id', 'usage_available', 'limit']) expect(query.getAll(name).length).toBeLessThanOrEqual(1);
  return query;
}
const zones = [
  {id: 'UTC', zone: /^UTC$/, prefix: '2026-01-15', bucket: '23:30', request: '23:45', offset: 'UTC+00:00', lastPrefix: '2026-01-16', lastBucket: '00:30'},
  {id: 'Europe/Berlin', zone: /^Europe\/Berlin$/, prefix: '2026-01-16', bucket: '00:30', request: '00:45', offset: 'UTC+01:00', lastPrefix: '2026-01-16', lastBucket: '01:30'},
  {id: 'America/New_York', zone: /^America\/New_York$/, prefix: '2026-01-15', bucket: '18:30', request: '18:45', offset: 'UTC-05:00', lastPrefix: '2026-01-15', lastBucket: '19:30'},
  {id: 'Asia/Kathmandu', zone: /^Asia\/Kat(h)?mandu$/, prefix: '2026-01-16', bucket: '05:15', request: '05:30', offset: 'UTC+05:45', lastPrefix: '2026-01-16', lastBucket: '06:15'},
];
for (const zone of zones) {
  test.describe(zone.id, () => {
    test.use({timezoneId: zone.id});
    for (const width of [375, 1440]) {
      test(`localizes request cells, all tables, axes and controls at ${width}px`, async ({page}) => {
        await page.setViewportSize({width, height: 1000});
        await open(page);
        await expect(page.locator('.request-table time')).toHaveText(`${zone.prefix} ${zone.request}:00.123 ${zone.offset}`);
        await expect(page.locator('.request-table time')).toHaveAttribute('datetime', '2026-01-15T23:45:00.123456789Z');
        await expect(page.locator('#analytics-from')).toHaveValue(`${zone.prefix}T${zone.bucket}`);
        await expect(page.locator('[data-analytics-offset="from"]')).toHaveText(zone.offset);
        for (const text of await page.locator('[data-analytics-timezone]').allTextContents()) expect(text).toMatch(zone.zone);
        await expect(page.locator('footer')).toContainText(zone.id === 'Asia/Kathmandu' ? /Asia\/Kat(h)?mandu/ : zone.id);
        await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
        const firstBucket = `${zone.prefix} ${zone.bucket}:00.123 ${zone.offset}`;
        const lastBucket = `${zone.lastPrefix} ${zone.lastBucket}:00.123 ${zone.offset}`;
        expect(await page.locator('.chart-fallback time').allTextContents()).toEqual([firstBucket, lastBucket, firstBucket, lastBucket, firstBucket, lastBucket, firstBucket, lastBucket]);
        for (const svg of await page.locator('svg.analytics-chart').all()) {
          await expect(svg).toContainText(`${zone.prefix} ${zone.bucket}`);
          await expect(svg.locator('[aria-label]').first()).toHaveAttribute('aria-label', `${zone.prefix} ${zone.bucket} ${zone.offset}`);
          await expect(svg.locator('[aria-label]').last()).toHaveAttribute('aria-label', `${zone.lastPrefix} ${zone.lastBucket} ${zone.offset}`);
        }
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
        if (width === 375) {
          const label = await page.locator('svg.analytics-chart').first().locator('[aria-label]').first().boundingBox();
          expect(label.height).toBeGreaterThanOrEqual(10);
        } else {
          const tops = await page.locator('.analytics-filter-grid input[type="datetime-local"], .analytics-filter-grid select').evaluateAll((fields) => fields.map((field) => field.getBoundingClientRect().top));
          expect(Math.max(...tops) - Math.min(...tops)).toBeLessThanOrEqual(1);
        }
        const links = await page.locator('.preset-list a, .pagination a, a:has-text("Download CSV")').evaluateAll((links) => links.map((link) => link.href));
        await page.locator('#analytics-client').selectOption('');
        expect(await page.locator('.preset-list a, .pagination a, a:has-text("Download CSV")').evaluateAll((links) => links.map((link) => link.href))).toEqual(links);
        const query = await apply(page);
        expect(query.get('from')).toBe(normalFrom);
        expect(query.get('to')).toBe(normalTo);
        expect(query.has('client_id')).toBe(false);
        expect(query.get('model_pool_id')).toBe('1');
        expect(query.get('usage_available')).toBe('true');
        expect(query.get('limit')).toBe('1');
      });
    }
    test('localizes empty analytics without relying on chart points', async ({page}) => {
      await open(page, 'empty');
      await expect(page.locator('#analytics-from')).toHaveValue(`${zone.prefix}T${zone.bucket}`);
      await expect(page.locator('svg.analytics-chart')).toHaveCount(0);
      await expect(page.locator('[data-analytics-chart]').first()).toHaveText('No series data is available for this range.');
      const query = await apply(page);
      expect(query.get('from')).toBe(normalFrom);
      expect(query.get('to')).toBe(normalTo);
    });
    test('unchanged Apply retains a one-nanosecond interval', async ({page}) => {
      await open(page, 'precise');
      const query = await apply(page);
      expect(query.get('from')).toBe(preciseFrom);
      expect(query.get('to')).toBe(preciseTo);
      expect(query.get('client_id')).toBe('1');
    });
  });
}
test.describe('range editing', () => {
  test.use({timezoneId: 'America/New_York'});
  test('editing one boundary preserves the other exact boundary and resets pagination', async ({page}) => {
    await open(page, 'precise');
    await expect(page.locator('#analytics-from')).toHaveValue('2026-01-15T07:00');
    await expect(page.locator('#analytics-to')).toHaveValue('2026-01-15T07:01');
    await page.locator('#analytics-to').fill('2026-01-15T07:02');
    const query = await apply(page);
    expect(query.get('from')).toBe(preciseFrom);
    expect(query.get('to')).toBe('2026-01-15T12:02:00.000Z');
  });
  test('rejects spring-forward gaps without navigation then accepts corrected input', async ({page}) => {
    await open(page);
    const before = page.url();
    await page.locator('#analytics-from').fill('2026-03-08T02:30');
    await page.getByRole('button', {name: 'Apply', exact: true}).click();
    await expect(page.locator('[data-analytics-range-error]')).toBeVisible();
    await expect(page.locator('#analytics-from')).toBeFocused();
    expect(await page.locator('#analytics-from').evaluate((input) => input.validity.customError)).toBe(true);
    expect(page.url()).toBe(before);
    await page.locator('#analytics-from').fill('2026-03-08T03:30');
    await page.locator('#analytics-to').fill('2026-03-08T04:30');
    const query = await apply(page);
    expect(query.get('from')).toBe('2026-03-08T07:30:00.000Z');
    expect(query.get('to')).toBe('2026-03-08T08:30:00.000Z');
  });
  test('retains later fold occurrence unchanged and chooses earlier occurrence for edits', async ({page}) => {
    await open(page, 'fold');
    await expect(page.locator('#analytics-from')).toHaveValue('2026-11-01T01:30');
    await expect(page.locator('[data-analytics-offset="from"]')).toHaveText('UTC-05:00');
    await expect(page.locator('.request-table time')).toHaveText('2026-11-01 01:30:00.123 UTC-05:00');
    let query = await apply(page);
    expect(query.get('from')).toBe('2026-11-01T06:30:00.123456789Z');
    await page.waitForLoadState('load');
    await page.locator('#analytics-from').fill('2026-11-01T01:31');
    await expect(page.locator('[data-analytics-offset="from"]')).toHaveText('UTC-04:00');
    query = await apply(page);
    expect(query.get('from')).toBe('2026-11-01T05:31:00.000Z');
    expect(query.get('to')).toBe('2026-11-01T07:30:00.987654321Z');
  });
  test('canonical pagination and CSV retain precision and presets retain durations', async ({page}) => {
    await open(page, 'precise');
    const next = new URL(await page.getByRole('link', {name: 'Next', exact: true}).getAttribute('href'), page.url());
    expect(next.searchParams.get('from')).toBe(preciseFrom);
    expect(next.searchParams.get('to')).toBe(preciseTo);
    expect(next.searchParams.get('offset')).toBe('2');
    const csv = new URL(await page.getByRole('link', {name: 'Download CSV', exact: true}).getAttribute('href'), page.url());
    expect(csv.searchParams.get('from')).toBe(preciseFrom);
    expect(csv.searchParams.get('to')).toBe(preciseTo);
    for (const preset of await page.locator('.preset-list a').all()) {
      const url = new URL(await preset.getAttribute('href'), page.url());
      expect(url.searchParams.has('from')).toBe(false);
      expect(url.searchParams.has('to')).toBe(false);
      expect(['1h', '24h', '7d', '30d', '90d']).toContain(url.searchParams.get('range'));
    }
    await page.getByRole('link', {name: 'Next', exact: true}).click();
    await expect(page).toHaveURL(next.href);
  });
});
test('the same shared UTC URL displays differently in two zones with the same interval', async ({browser}) => {
  const texts = [];
  for (const timezoneId of ['UTC', 'Asia/Kathmandu']) {
    const context = await browser.newContext({timezoneId, baseURL: 'http://127.0.0.1:18081', httpCredentials: {username: 'browser-test', password: 'browser-test-password-local'}});
    try {
      const page = await context.newPage();
      const pageErrors = [];
      page.on('pageerror', (error) => pageErrors.push(error.message));
      await fixtures.route(page);
      await page.goto(`/admin/analytics?from=${encodeURIComponent(normalFrom)}&to=${encodeURIComponent(normalTo)}`);
      texts.push(await page.locator('.request-table time').textContent());
      const query = await apply(page);
      expect(query.get('from')).toBe(normalFrom);
      expect(query.get('to')).toBe(normalTo);
      expect(pageErrors).toEqual([]);
    } finally { await context.close(); }
  }
  expect(texts).toEqual(['2026-01-15 23:45:00.123 UTC+00:00', '2026-01-16 05:30:00.123 UTC+05:45']);
});
test('localizes times even when the page has no chart containers', async ({page}) => {
  await open(page, 'normal', (html) => html.replaceAll('data-analytics-chart=', 'data-disabled-chart='));
  await expect(page.locator('.request-table time')).toContainText('UTC+');
  const query = await apply(page);
  expect(query.get('from')).toBe(normalFrom);
});
for (const mode of ['no JavaScript', 'missing helper', 'blocked helper', 'formatter failure']) {
  test.describe(mode, () => {
    test.use({timezoneId: 'Asia/Kathmandu', javaScriptEnabled: mode !== 'no JavaScript'});
    test('keeps usable UTC fallback including native form submission', async ({page}) => {
      if (mode === 'missing helper') await page.route('**/admin/static/analytics-time.js', (route) => route.fulfill({contentType: 'application/javascript', body: ''}));
      if (mode === 'blocked helper') {
        blockedHelper.add(page);
        await page.route('**/admin/static/analytics-time.js', (route) => route.abort());
      }
      if (mode === 'formatter failure') {
        const source = readFileSync(new URL('../../internal/web/static/analytics-time.js', import.meta.url), 'utf8');
        await page.route('**/admin/static/analytics-time.js', (route) => route.fulfill({contentType: 'application/javascript', body: source + '\nwindow.LLMGWAnalyticsTime = {...window.LLMGWAnalyticsTime, formatTimestamp() { throw new Error("Formatter unavailable"); }};'}));
      }
      await open(page);
      await expect(page.locator('.request-table time')).toHaveText('2026-01-15 23:45:00.123 UTC');
      await expect(page.locator('#analytics-from')).toHaveValue('2026-01-15T23:30');
      await expect(page.getByLabel('From (UTC)', {exact: true})).toBeVisible();
      await expect(page.locator('footer')).toContainText('Times are shown in UTC');
      if (mode !== 'no JavaScript') {
        await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
        await expect(page.locator('svg').first()).toContainText('2026-01-15 23:30 UTC');
      }
      await page.unroute('**/admin/analytics?**');
      await page.getByRole('button', {name: 'Apply', exact: true}).click();
      await page.waitForURL((url) => url.searchParams.has('from'));
      const query = new URL(page.url()).searchParams;
      expect(query.get('from')).toBe('2026-01-15T23:30:00Z');
      expect(query.get('to')).toBe('2026-01-16T02:30:00Z');
      expect(query.has('from_local')).toBe(false);
      await expect(page.getByRole('heading', {name: 'Usage analytics', exact: true})).toBeVisible();
    });
  });
}
