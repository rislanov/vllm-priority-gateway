import {test, expect} from '@playwright/test';
import {analyticsFixtures} from './analytics-fixtures.mjs';

let fixtures;
const errors = new WeakMap();
const chart = (page, name) => page.locator(`[data-analytics-chart="${name}"]`);
test.beforeAll(() => { fixtures = analyticsFixtures(); });
test.afterAll(() => fixtures?.close());
test.beforeEach(({page}) => {
  const messages = [];
  page.on('pageerror', (error) => messages.push(error.message));
  page.on('console', (message) => { if (['warning', 'error'].includes(message.type())) messages.push(message.text()); });
  errors.set(page, messages);
});
test.afterEach(({page}) => { expect(errors.get(page)).toEqual([]); });

async function open(page, name = 'imbalanced') {
  await fixtures.route(page, name);
  await page.goto(`/admin/analytics?fixture=${name}`);
  await expect(page).toHaveTitle('Usage analytics · vLLM Priority Gateway');
  await expect(page.getByRole('heading', {name: 'Usage analytics', exact: true})).toBeVisible();
}
async function coordinates(page, name, color) {
  return chart(page, name).locator(`[fill="${color}"]`).evaluateAll((markers) => markers.map((m) => ({
    x: Number(m.getAttribute(m.tagName === 'circle' ? 'cx' : 'x')) + (m.tagName === 'rect' ? 4 : 0),
    y: Number(m.getAttribute(m.tagName === 'circle' ? 'cy' : 'y')) + (m.tagName === 'rect' ? 4 : 0),
  })).slice(0, -1)); // The last marker belongs to the legend.
}

test('independent token scales make a 1000x smaller output equally readable', async ({page}) => {
  await open(page);
  await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
  const input = await coordinates(page, 'input-tokens', '#38d6a2');
  const output = await coordinates(page, 'output-tokens', '#b883f4');
  expect(input).toHaveLength(2);
  expect(output).toEqual(input);
  expect(input.map((p) => p.y)).toEqual([48, 129]);
  for (const [name, title, legend] of [['input-tokens', 'Input token chart', 'Input'], ['output-tokens', 'Output token chart', 'Output']]) {
    const svg = chart(page, name).locator('svg');
    await expect(svg).toHaveAccessibleName(new RegExp(title));
    await expect(svg.locator('.chart-legend')).toHaveText(legend);
    await expect(svg.locator('.chart-axis-label-y').first()).toHaveText('0');
    await expect(svg.locator('.chart-baseline')).toHaveAttribute('y1', '210');
  }
  await expect(chart(page, 'output-tokens').locator('path')).toHaveAttribute('stroke-dasharray', '8 7');
  await expect(chart(page, 'input-tokens').locator('path')).not.toHaveAttribute('stroke-dasharray');
  const before = output;
  await page.unroute('**/admin/analytics?**');
  await open(page, 'changed-input');
  await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
  expect(await coordinates(page, 'output-tokens', '#b883f4')).toEqual(before);
});

test('all panels align timestamps and keep one machine-readable series', async ({page}) => {
  await open(page, 'gaps');
  await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
  await expect(page.locator('[data-analytics-point]')).toHaveCount(3);
  await expect(page.locator('[data-analytics-series]')).toHaveCount(1);
  const requestX = (await coordinates(page, 'requests', '#ffb32c')).map((p) => p.x);
  expect(requestX).toHaveLength(3);
  expect((requestX[1] - requestX[0]) / (requestX[2] - requestX[0])).toBeCloseTo(1 / 6);
  for (const [name, color] of [['input-tokens', '#38d6a2'], ['output-tokens', '#b883f4']]) {
    expect((await coordinates(page, name, color)).map((p) => p.x)).toEqual(requestX);
  }
  const labels = await chart(page, 'requests').locator('text[aria-label]').allTextContents();
  for (const name of ['input-tokens', 'output-tokens', 'cache']) {
    expect(await chart(page, name).locator('text[aria-label]').allTextContents()).toEqual(labels);
  }
  expect(await page.locator('svg.analytics-chart').evaluateAll((svgs) => svgs.every((svg) => !/NaN|Infinity/.test(svg.outerHTML)))).toBe(true);
});

for (const name of ['zero', 'sparse']) {
  test(`${name} series retains reported values without invalid geometry`, async ({page}) => {
    await open(page, name);
    await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
    for (const [metric, color] of [['input-tokens', '#38d6a2'], ['output-tokens', '#b883f4']]) {
      const points = await coordinates(page, metric, color);
      expect(points).toHaveLength(name === 'zero' ? 2 : 1);
      expect(points.every((p) => p.y === (name === 'zero' ? 210 : 48))).toBe(true);
      await expect(chart(page, metric)).not.toContainText('No series data');
    }
    expect(await page.locator('svg.analytics-chart').evaluateAll((svgs) => svgs.every((svg) => !/NaN|Infinity/.test(svg.outerHTML)))).toBe(true);
  });
}

test('empty range has four honest messages and empty fallback tables', async ({page}) => {
  await open(page, 'empty');
  await expect(page.locator('svg.analytics-chart')).toHaveCount(0);
  await expect(page.locator('.chart-stage')).toHaveCount(4);
  for (const stage of await page.locator('.chart-stage').all()) await expect(stage).toHaveText('No series data is available for this range.');
  for (const details of await page.locator('.chart-fallback').all()) {
    await details.locator('summary').click();
    await expect(details.locator('tbody')).toHaveText('No series data is available for this range.');
  }
});

for (const width of [375, 768, 1440]) {
  test(`four full-width panels keep readable geometry at ${width}px`, async ({page}) => {
    await page.setViewportSize({width, height: 1000});
    await open(page);
    await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
    const panels = await page.locator('.chart-panel').evaluateAll((panels) => panels.map((p) => {
      const {x, y, width, height} = p.getBoundingClientRect(); return {x, y, width, height};
    }));
    expect(panels).toHaveLength(4);
    for (let i = 1; i < panels.length; i++) {
      expect(panels[i].x).toBe(panels[0].x);
      expect(panels[i].width).toBe(panels[0].width);
      expect(panels[i].y).toBeGreaterThanOrEqual(panels[i-1].y + panels[i-1].height);
    }
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    const frames = await page.locator('svg.analytics-chart').evaluateAll((svgs) => svgs.map((svg) => {
      const box = svg.getBoundingClientRect();
      return {width: box.width, height: box.height, canvas: svg.viewBox.baseVal.width, containerWidth: svg.parentElement.clientWidth,
        readable: [...svg.querySelectorAll('text')].every((label) => {
          const rect = label.getBoundingClientRect();
          return rect.x >= box.x - 1 && rect.right <= box.right + 1 && rect.y >= box.y - 1 && rect.bottom <= box.bottom + 1
            && Number.parseFloat(getComputedStyle(label).fontSize) * svg.getScreenCTM().a >= 10;
        })};
    }));
    expect(frames.map((f) => Math.round(f.height))).toEqual([280, 280, 280, 460]);
    for (const frame of frames) {
      expect(Math.abs(frame.width - frame.canvas)).toBeLessThanOrEqual(1);
      expect(frame.width).toBeLessThanOrEqual(frame.containerWidth + 1);
      expect(frame.readable).toBe(true);
    }
    expect(await page.locator('svg.analytics-chart').evaluateAll((svgs) => {
      const ids = svgs.flatMap((svg) => svg.getAttribute('aria-labelledby').split(' '));
      return new Set(ids).size === 8 && ids.every((id) => document.querySelectorAll(`[id="${id}"]`).length === 1);
    })).toBe(true);
  });
}

test('resize recalculates paths and labels without duplicating points or accessible IDs', async ({page}) => {
  await page.setViewportSize({width: 1440, height: 1000});
  await open(page, 'gaps');
  const path = chart(page, 'output-tokens').locator('path');
  const before = await path.getAttribute('d');
  for (const width of [375, 768, 1440, 375]) {
    await page.setViewportSize({width, height: 1000});
    await expect.poll(async () => chart(page, 'output-tokens').locator('svg').evaluate((svg) => Math.abs(svg.viewBox.baseVal.width - svg.getBoundingClientRect().width))).toBeLessThanOrEqual(1);
    await expect(page.locator('svg.analytics-chart')).toHaveCount(4);
    await expect(page.locator('[data-analytics-point]')).toHaveCount(3);
    expect(await coordinates(page, 'output-tokens', '#b883f4')).toHaveLength(3);
    expect(await page.locator('[id^="analytics-chart-"]').evaluateAll((nodes) => new Set(nodes.map((n) => n.id)).size === nodes.length)).toBe(true);
  }
  expect(await path.getAttribute('d')).not.toBe(before);
});

test('cache-known gaps remain discontinuities and explicit cache zero remains known', async ({page}) => {
  await open(page, 'gaps');
  const cache = chart(page, 'cache');
  await expect(cache.locator('svg')).toBeVisible();
  for (const path of await cache.locator('path').all()) {
    expect((await path.getAttribute('d')).match(/M/g)).toHaveLength(2);
    expect(await path.getAttribute('d')).not.toContain('L');
  }
  await expect(cache.locator('circle[fill="#78b8ff"]')).toHaveCount(2);
  await page.unroute('**/admin/analytics?**');
  await open(page, 'zero');
  await expect(chart(page, 'cache').locator('svg')).toBeVisible();
  await expect(chart(page, 'cache').locator('circle[fill="#78b8ff"]')).toHaveAttribute('cy', '184');
  await page.unroute('**/admin/analytics?**');
  await open(page, 'normal', undefined);
  // The second bucket is unknown, rather than a fabricated zero marker.
  await expect(chart(page, 'cache').locator('circle[fill="#78b8ff"]')).toHaveCount(1);
});

test('single cache annotations stay inside the plot and clear of subtitles and axes', async ({page}) => {
  await page.setViewportSize({width: 375, height: 1000});
  for (const name of ['sparse', 'normal']) {
    await page.unroute('**/admin/analytics?**');
    await open(page, name);
    await expect(chart(page, 'cache').locator('svg')).toBeVisible();
    expect(await chart(page, 'cache').locator('svg').evaluate((svg) => {
      const labels = [...svg.querySelectorAll('.chart-subtitle, .chart-axis-label-y')].map((text) => text.getBoundingClientRect());
      const plot = svg.querySelector('.chart-baseline').getBoundingClientRect();
      return [...svg.querySelectorAll('.chart-sparse-label')].every((text) => {
        const box = text.getBoundingClientRect();
        return box.left >= plot.left - 1 && box.right <= plot.right + 1
          && labels.every((s) => box.left >= s.right || box.right <= s.left || box.top >= s.bottom || box.bottom <= s.top);
      });
    }), name).toBe(true);
  }
});

test.describe('without JavaScript', () => {
  test.use({javaScriptEnabled: false});
  test('all four tables retain exact fixture values and UTC timestamps', async ({page}) => {
    await open(page);
    await expect(page.locator('svg')).toHaveCount(0);
    const expected = [
      ['2', '1'], ['1,000,000', '500,000'], ['1,000', '500'],
    ];
    for (const [index, details] of (await page.locator('.chart-fallback').all()).entries()) {
      await details.locator('summary').click();
      await expect(details.locator('table')).toBeVisible();
      await expect(details.locator('time').first()).toHaveText('2026-01-15 23:30:00.123 UTC');
      if (index < 3) expect(await details.locator('tbody tr td:last-child').allTextContents()).toEqual(expected[index]);
      else expect(await details.locator('tbody tr').first().locator('td').allTextContents()).toEqual(['2026-01-15 23:30:00.123 UTC', '20', '20%']);
    }
  });

  test('cache table distinguishes unavailable buckets from known zero', async ({page}) => {
    await open(page, 'normal');
    const cacheTable = page.getByRole('table', {name: 'Cache-known usage data'});
    await page.getByText('View cache-known usage data', {exact: true}).click();
    expect(await cacheTable.locator('tbody tr').nth(1).locator('td').allTextContents()).toEqual(['2026-01-16 00:30:00.123 UTC', '—', '—']);
    await page.unroute('**/admin/analytics?**');
    await open(page, 'zero');
    await page.getByText('View cache-known usage data', {exact: true}).click();
    expect(await cacheTable.locator('tbody tr').first().locator('td').allTextContents()).toEqual(['2026-01-15 23:30:00.123 UTC', '0', '0%']);
    expect(await cacheTable.locator('tbody tr').nth(1).locator('td').allTextContents()).toEqual(['2026-01-16 00:30:00.123 UTC', '—', '—']);
  });
});
