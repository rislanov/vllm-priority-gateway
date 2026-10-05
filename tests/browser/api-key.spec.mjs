import {test, expect} from '@playwright/test';
import {randomUUID} from 'node:crypto';

const pageErrors = new WeakMap();
const clientNames = new WeakMap();
const expectedNetworkErrors = new WeakSet();

test.beforeEach(async ({page}) => {
  const errors = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') errors.push(message.text()); });
  pageErrors.set(page, errors);
  clientNames.set(page, `clipboard-${test.info().testId}-${randomUUID()}`);
  await waitForReplicas(page);
  await page.goto('/admin/clients');
  await page.getByLabel('Name', {exact: true}).fill(clientNames.get(page));
  await page.getByRole('button', {name: 'Create client', exact: true}).click();
  await expect(page.getByRole('cell', {name: clientNames.get(page), exact: true})).toBeVisible();
  if (process.env.LLMGW_BROWSER_POSTGRES_DSN) {
    for (const port of [18082, 18083]) {
      await expect.poll(async () => {
        const response = await page.request.get(`http://127.0.0.1:${port}/admin/api/status`);
        return (await response.json()).clients.some((client) => client.name === clientNames.get(page));
      }).toBe(true);
    }
  }
  await waitForReplicas(page);
});

async function waitForReplicas(page) {
  if (!process.env.LLMGW_BROWSER_POSTGRES_DSN) return;
  await expect.poll(async () => {
    const statuses = await Promise.all([18082, 18083].map(async (port) => (await page.request.get(`http://127.0.0.1:${port}/readyz`)).ok()));
    return statuses.every(Boolean);
  }, {timeout: 15_000}).toBe(true);
}

test.afterEach(async ({page}) => {
  const errors = pageErrors.get(page);
  expect(expectedNetworkErrors.has(page) ? errors.filter((error) => /Failed to load resource|net::ERR_FAILED/.test(error) === false) : errors).toEqual([]);
});

async function generateKey(page, origin = '') {
  await page.goto(`${origin}/admin/keys`);
  await page.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
  await page.getByRole('button', {name: 'Generate API key', exact: true}).click();
  const field = page.getByRole('textbox', {name: 'API key', exact: true});
  await expect(field).toBeVisible();
  await expect(field).toHaveAttribute('readonly', '');
  const key = await field.inputValue();
  expect(key).toMatch(/^llmgw_[A-Za-z0-9_-]{43}$/);
  await expect(page).toHaveTitle('API Keys · vLLM Priority Gateway');
  return key;
}

async function assertKeyFits(page) {
  const geometry = await page.getByRole('textbox', {name: 'API key', exact: true}).evaluate((field) => {
    const box = field.getBoundingClientRect();
    return {left: box.left, right: box.right, viewport: innerWidth, height: field.clientHeight, contentHeight: field.scrollHeight};
  });
  expect(geometry.left).toBeGreaterThanOrEqual(0);
  expect(geometry.right).toBeLessThanOrEqual(geometry.viewport);
  expect(geometry.contentHeight).toBeLessThanOrEqual(geometry.height);
}

test('receives the key in the creation response and refresh never repeats the POST', async ({page}) => {
  const posts = [];
  page.on('request', (request) => {
    if (request.method() === 'POST' && new URL(request.url()).pathname === '/admin/keys') posts.push(request);
  });
  const responsePromise = page.waitForResponse((response) => response.request().method() === 'POST' && new URL(response.url()).pathname === '/admin/keys');
  await generateKey(page);
  const response = await responsePromise;
  expect(response.status()).toBe(200);
  expect(response.headers()['cache-control']).toBe('no-store');
  expect(response.headers().location).toBeUndefined();
  expect(response.request().resourceType()).toBe('fetch');
  expect(new URL(page.url()).search).toBe('');
  await page.reload();
  await expect(page.locator('#one-time-secret')).toHaveCount(0);
  expect(posts).toHaveLength(1);
});

test('keeps generation disabled when JavaScript is unavailable', async ({browser}) => {
  const context = await browser.newContext({javaScriptEnabled: false, httpCredentials: {
    username: 'browser-test', password: 'browser-test-password-local',
  }});
  try {
    const page = await context.newPage();
    await page.goto('http://127.0.0.1:18081/admin/keys');
    await expect(page.getByRole('button', {name: 'Generate API key', exact: true})).toBeDisabled();
    await expect(page.locator('noscript p')).toBeVisible();
  } finally {
    await context.close();
  }
});

test('refresh does not resubmit after revoking and generating again', async ({page}) => {
  await generateKey(page);
  await waitForReplicas(page);
  page.on('dialog', (dialog) => dialog.accept());
  const revokedPromise = page.waitForResponse((response) => response.request().method() === 'POST' && new URL(response.url()).pathname === '/admin/keys');
  await page.getByRole('row').filter({has: page.getByRole('cell', {name: clientNames.get(page), exact: true})}).getByRole('button', {name: 'Revoke', exact: true}).click();
  expect((await revokedPromise).status()).toBe(303);
  await expect(page.locator('#one-time-secret')).toHaveCount(0);
  await waitForReplicas(page);
  await page.reload();
  await expect(page.getByRole('row').filter({has: page.getByRole('cell', {name: clientNames.get(page), exact: true})})).toContainText('revoked');
  await page.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
  const posts = [];
  page.on('request', (request) => { if (request.method() === 'POST') posts.push(request); });
  await page.getByRole('button', {name: 'Generate API key', exact: true}).click();
  await expect(page.locator('#one-time-secret')).toBeVisible();
  await page.reload();
  await expect(page.locator('#one-time-secret')).toHaveCount(0);
  expect(posts).toHaveLength(1);
});

test('can generate safely after a failed revoke without resubmitting it', async ({page}) => {
  expectedNetworkErrors.add(page);
  await generateKey(page);
  await waitForReplicas(page);
  page.on('dialog', (dialog) => dialog.accept());
  const row = page.getByRole('row').filter({has: page.getByRole('cell', {name: clientNames.get(page), exact: true})});
  await row.locator('[name="key_id"]').evaluate((input) => { input.value = '999999999'; });
  const revokedPromise = page.waitForResponse((response) => response.request().method() === 'POST' && new URL(response.url()).pathname === '/admin/keys');
  await row.getByRole('button', {name: 'Revoke', exact: true}).click();
  expect((await revokedPromise).status()).toBe(303);
  await expect(page.locator('.notice.error')).toContainText('Unable to revoke');
  await page.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
  const posts = [];
  page.on('request', (request) => { if (request.method() === 'POST') posts.push(request); });
  await page.getByRole('button', {name: 'Generate API key', exact: true}).click();
  await expect(page.locator('#one-time-secret')).toBeVisible();
  await page.reload();
  await expect(page.locator('#one-time-secret')).toHaveCount(0);
  await expect(page.locator('.notice.error')).toHaveCount(0);
  expect(posts).toHaveLength(1);
});

test('shows the full key when GET and POST reach different PostgreSQL replicas', async ({page}) => {
  test.skip(!process.env.LLMGW_BROWSER_POSTGRES_DSN, 'Requires an existing local PostgreSQL DSN; no image is downloaded by this test.');
  const get = await page.goto('/admin/keys');
  await page.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
  const createdPromise = page.waitForResponse((response) => response.request().method() === 'POST' && new URL(response.url()).pathname === '/admin/keys');
  await page.getByRole('button', {name: 'Generate API key', exact: true}).click();
  const created = await createdPromise;
  expect(created.status()).toBe(200);
  expect(created.headers()['x-test-replica']).not.toBe(get.headers()['x-test-replica']);
  await expect(page.getByRole('textbox', {name: 'API key', exact: true})).toBeVisible();
  const key = await page.getByRole('textbox', {name: 'API key', exact: true}).inputValue();
  expect(/^llmgw_[A-Za-z0-9_-]{43}$/.test(key)).toBe(true);
  const refresh = await page.reload();
  expect(refresh.headers()['x-test-replica']).not.toBe(created.headers()['x-test-replica']);
  await expect(page.locator('#one-time-secret')).toHaveCount(0);
  await expect(page.locator('body')).not.toContainText(key);
});

test('concurrent tabs receive their own full keys', async ({page, context}) => {
  const other = await context.newPage();
  const errors = [];
  other.on('pageerror', (error) => errors.push(error.message));
  try {
    for (const tab of [page, other]) {
      await tab.goto('/admin/keys');
      await tab.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
    }
    await Promise.all([page, other].map((tab) => tab.getByRole('button', {name: 'Generate API key', exact: true}).click()));
    const keys = [];
    for (const tab of [page, other]) {
      const field = tab.getByRole('textbox', {name: 'API key', exact: true});
      await expect(field).toBeVisible();
      const key = await field.inputValue();
      expect(/^llmgw_[A-Za-z0-9_-]{43}$/.test(key)).toBe(true);
      keys.push(key);
    }
    expect(keys[0] === keys[1]).toBe(false);
    await expect.poll(async () => {
      const response = await page.request.get('/admin/api/status');
      const records = (await response.json()).keys;
      return keys.every((key) => records.filter((record) => record.client === clientNames.get(page) && key.startsWith(record.prefix)).length === 1);
    }).toBe(true);
    expect(errors).toEqual([]);
  } finally {
    await other.close();
  }
});

test('blocks duplicate submits while creation is pending', async ({page}) => {
  await page.goto('/admin/keys');
  await page.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
  let release;
  let started;
  const entered = new Promise((resolve) => { started = resolve; });
  const gate = new Promise((resolve) => { release = resolve; });
  let posts = 0;
  await page.route('**/admin/keys', async (route) => {
    if (route.request().method() !== 'POST') return route.continue();
    posts++;
    started();
    await gate;
    await route.continue();
  });
  const button = page.getByRole('button', {name: 'Generate API key', exact: true});
  await button.click();
  await entered;
  await expect(button).toBeDisabled();
  await button.evaluate((control) => control.form.dispatchEvent(new Event('submit', {bubbles: true, cancelable: true})));
  release();
  await expect(page.locator('#one-time-secret')).toBeVisible();
  expect(posts).toBe(1);
});

for (const failure of ['rejected', 'lost response']) {
  test(`reports ${failure} without false success or automatic retries`, async ({page}) => {
    expectedNetworkErrors.add(page);
    await page.goto('/admin/keys');
    await page.getByLabel('Client', {exact: true}).selectOption({label: clientNames.get(page)});
    const before = await page.locator('tbody tr:has([name="key_id"])').count();
    let posts = 0;
    await page.route('**/admin/keys', async (route) => {
      if (route.request().method() !== 'POST') return route.continue();
      posts++;
      if (failure === 'rejected') return route.fulfill({status: 503, contentType: 'text/plain', body: 'Unavailable'});
      const response = await route.fetch(); // Commit a real key, then lose the response.
      expect(response.status()).toBe(200);
      await route.abort('failed');
    });
    await page.getByRole('button', {name: 'Generate API key', exact: true}).click();
    await expect(page.locator('[data-key-create-status]')).toContainText(/failed|could not|unable/i);
    await expect(page.locator('#one-time-secret')).toHaveCount(0);
    await expect(page.getByRole('button', {name: 'Generate API key', exact: true})).toBeEnabled();
    await page.reload();
    expect(posts).toBe(1);
    if (failure === 'lost response') await expect(page.locator('tbody tr:has([name="key_id"])')).toHaveCount(before + 1);
  });
}

for (const viewport of [{width: 1440, height: 900}, {width: 390, height: 844}, {width: 320, height: 800}]) {
  test(`copies the full one-time key at ${viewport.width}px and removes it on refresh`, async ({page, context}) => {
    await page.setViewportSize(viewport);
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    const key = await generateKey(page);
    await assertKeyFits(page);
    await page.getByRole('button', {name: 'Copy key', exact: true}).click();
    await expect(page.getByRole('button', {name: 'Copied', exact: true})).toBeVisible();
    expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(key);
    // Repeated copying must remain usable.
    await page.getByRole('button', {name: 'Copied', exact: true}).click();
    expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(key);
    await page.reload();
    await expect(page.getByRole('textbox', {name: 'API key', exact: true})).toHaveCount(0);
    await expect(page.locator('body')).not.toContainText(key);
  });
}

test('copies on plain HTTP where Clipboard API is absent', async ({page}) => {
  const key = await generateKey(page, 'http://gateway.test:18081');
  expect(await page.evaluate(() => window.isSecureContext)).toBe(false);
  expect(await page.evaluate(() => typeof navigator.clipboard)).toBe('undefined');
  await page.getByRole('button', {name: 'Copy key', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Copied', exact: true})).toBeVisible();
  // Paste proves real clipboard contents, even without Clipboard API access.
  await page.getByRole('link', {name: 'Clients', exact: true}).click();
  const name = page.getByLabel('Name', {exact: true});
  await name.focus();
  await page.keyboard.press(process.platform === 'darwin' ? 'Meta+V' : 'Control+V');
  await expect(name).toHaveValue(key);
});

test('copies with the fallback after Clipboard API permission is denied', async ({page, context}) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {value: {writeText: async () => { throw new DOMException('Denied', 'NotAllowedError'); }}});
  });
  const key = await generateKey(page);
  await page.getByRole('button', {name: 'Copy key', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Copied', exact: true})).toBeVisible();
  await page.getByRole('link', {name: 'Clients', exact: true}).click();
  const name = page.getByLabel('Name', {exact: true});
  await name.focus();
  await page.keyboard.press(process.platform === 'darwin' ? 'Meta+V' : 'Control+V');
  await expect(name).toHaveValue(key);
});

test('exposes copy feedback before interaction and keeps manual-copy instructions accessible', async ({page}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {value: undefined});
    document.execCommand = () => false;
  });
  const key = await generateKey(page);
  const cdp = await page.context().newCDPSession(page);
  const {root} = await cdp.send('DOM.getDocument');
  const {nodeId} = await cdp.send('DOM.querySelector', {nodeId: root.nodeId, selector: '[data-copy-status]'});
  const {node: {backendNodeId}} = await cdp.send('DOM.describeNode', {nodeId});
  const {nodes: initialNodes} = await cdp.send('Accessibility.getPartialAXTree', {backendNodeId, fetchRelatives: false});
  const initialStatus = initialNodes.find((node) => node.backendDOMNodeId === backendNodeId);
  expect(initialStatus?.ignored).toBe(false);
  expect(initialStatus?.role?.value).toBe('status');
  await page.getByRole('button', {name: 'Copy key', exact: true}).click();
  await expect(page.locator('[data-copy-status]')).toContainText('copy it manually');
  await expect(page.getByRole('button', {name: 'Copy key', exact: true})).toBeEnabled();
  const field = page.getByRole('textbox', {name: 'API key', exact: true});
  await expect(field).toBeFocused();
  expect(await field.evaluate((element) => element.value.slice(element.selectionStart, element.selectionEnd))).toBe(key);
  const {nodes: updatedNodes} = await cdp.send('Accessibility.getPartialAXTree', {backendNodeId, fetchRelatives: true});
  expect(updatedNodes.some((node) => node.role?.value === 'StaticText' && /copy it manually/.test(node.name?.value))).toBe(true);
  await cdp.detach();
});
