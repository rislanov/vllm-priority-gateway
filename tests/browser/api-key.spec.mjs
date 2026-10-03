import {test, expect} from '@playwright/test';

const pageErrors = new WeakMap();

test.beforeEach(async ({page}) => {
  const errors = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') errors.push(message.text()); });
  await page.goto('/admin/clients');
  await page.getByLabel('Name', {exact: true}).fill(`clipboard-${test.info().testId}`);
  await page.getByRole('button', {name: 'Create client', exact: true}).click();
  await expect(page.getByRole('cell', {name: `clipboard-${test.info().testId}`, exact: true})).toBeVisible();
  pageErrors.set(page, errors);
});

test.afterEach(async ({page}) => {
  expect(pageErrors.get(page)).toEqual([]);
});

async function generateKey(page, origin = '') {
  await page.goto(`${origin}/admin/keys`);
  await page.getByLabel('Client', {exact: true}).selectOption({label: `clipboard-${test.info().testId}`});
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
