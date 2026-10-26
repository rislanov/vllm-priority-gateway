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
