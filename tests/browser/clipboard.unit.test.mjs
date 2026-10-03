import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../internal/web/static/app.js', import.meta.url), 'utf8');
const key = 'llmgw_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG';

// Only the browser clipboard boundary is replaced; execute the shipped click handler.
function fixture({clipboard, legacy = true, secretPresent = true} = {}) {
  let click;
  let selected = '';
  let clipboardText = '';
  let legacyCalls = 0;
  const copy = {textContent: 'Copy key', disabled: false, addEventListener: (_, handler) => { click = handler; }};
  const status = {textContent: ''};
  const secret = {
    value: key, textContent: key,
    focus() {}, select() { selected = this.value; },
    addEventListener() {},
  };
  const document = {
    body: {hasAttribute: () => false},
    querySelector: (selector) => ({
      '[data-copy]': copy, '[data-secret]': secretPresent ? secret : null, '[data-copy-status]': status,
    })[selector] ?? null,
    querySelectorAll: () => [],
    execCommand(command) {
      assert.equal(command, 'copy');
      legacyCalls++;
      if (legacy instanceof Error) throw legacy;
      if (legacy) clipboardText = selected;
      return legacy;
    },
  };
  const navigator = {clipboard: clipboard === 'working' ? {writeText: async (value) => { clipboardText = value; }} : clipboard};
  vm.runInNewContext(source, {document, navigator, window: {}}, {filename: 'app.js'});
  return {copy, status, secret, click: () => click(), clipboard: () => clipboardText, selection: () => selected, legacyCalls: () => legacyCalls};
}

test('copies the complete key using Clipboard API and reports success', async () => {
  const f = fixture({clipboard: 'working'});
  await f.click();
  assert.equal(f.clipboard(), key);
  assert.equal(f.copy.textContent, 'Copied');
  assert.match(f.status.textContent, /copied/i);
  assert.equal(f.legacyCalls(), 0);
  assert.equal(f.copy.disabled, false);
});

test('copies the complete key when Clipboard API is unavailable', async () => {
  const f = fixture();
  await f.click();
  assert.equal(f.clipboard(), key);
  assert.equal(f.copy.textContent, 'Copied');
  assert.equal(f.copy.disabled, false);
});

test('falls back to selected text when Clipboard API rejects', async () => {
  const f = fixture({clipboard: {writeText: async () => { throw new Error('permission denied'); }}});
  await f.click();
  assert.equal(f.clipboard(), key);
  assert.equal(f.copy.textContent, 'Copied');
});

for (const legacy of [false, new Error('copy denied')]) {
  test(`leaves the whole key selected for manual copying when fallback ${legacy === false ? 'fails' : 'throws'}`, async () => {
    const f = fixture({legacy});
    await f.click();
    assert.equal(f.clipboard(), '');
    assert.equal(f.selection(), key);
    assert.equal(f.copy.textContent, 'Copy key');
    assert.match(f.status.textContent, /copy.*manually/i);
    assert.equal(f.copy.disabled, false);
  });
}

test('does not report success if the secret is missing', async () => {
  const f = fixture({secretPresent: false});
  await f.click();
  assert.equal(f.clipboard(), '');
  assert.equal(f.copy.textContent, 'Copy key');
});

test('disables the control while copying and permits copying again', async () => {
  let complete;
  const f = fixture({clipboard: {writeText: () => new Promise((resolve) => { complete = resolve; })}});
  const pending = f.click();
  assert.equal(f.copy.disabled, true);
  complete();
  await pending;
  assert.equal(f.copy.disabled, false);
  const second = f.click();
  assert.equal(f.copy.disabled, true);
  complete();
  await second;
  assert.equal(f.copy.disabled, false);
});
