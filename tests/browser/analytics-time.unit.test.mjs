import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../internal/web/static/analytics-time.js', import.meta.url), 'utf8');
function helper() {
  const window = {};
  vm.runInNewContext(source, {window, Intl, Date});
  assert.ok(window.LLMGWAnalyticsTime, 'time conversion API is exported');
  return window.LLMGWAnalyticsTime;
}
function inZone(timeZone, expression) {
  const child = spawnSync(process.execPath, ['--input-type=commonjs', '-e',
    `const window = {}; ${source}\nconst api = window.LLMGWAnalyticsTime; console.log(JSON.stringify(${expression}));`],
  {env: {...process.env, TZ: timeZone}, encoding: 'utf8'});
  assert.equal(child.status, 0, child.stderr);
  return JSON.parse(child.stdout);
}

test('formats fractional positive offset with date rollover and three fractional digits', () => {
  assert.equal(helper().formatTimestamp('2026-01-15T23:30:00.123456789Z', {timeZone: 'Asia/Kathmandu', precision: 'millisecond'}),
    '2026-01-16 05:15:00.123 UTC+05:45');
});
test('formats UTC and negative offsets using the offset at that instant', () => {
  const api = helper();
  assert.equal(api.formatTimestamp('2026-01-15T02:30:00Z', {timeZone: 'UTC', precision: 'minute'}), '2026-01-15 02:30 UTC+00:00');
  assert.equal(api.formatTimestamp('2026-01-15T02:30:00Z', {timeZone: 'America/New_York', precision: 'minute'}), '2026-01-14 21:30 UTC-05:00');
  assert.equal(api.formatTimestamp('2026-07-15T02:30:00Z', {timeZone: 'America/New_York', precision: 'minute'}), '2026-07-14 22:30 UTC-04:00');
  assert.equal(api.formatAxisTimestamp('2026-01-15T23:30:00Z', 'Asia/Kathmandu'), '2026-01-16 05:15');
});
test('distinguishes the two occurrences of a repeated time', () => {
  const api = helper();
  assert.equal(api.formatTimestamp('2026-11-01T05:30:00Z', {timeZone: 'America/New_York'}), '2026-11-01 01:30:00.000 UTC-04:00');
  assert.equal(api.formatTimestamp('2026-11-01T06:30:00Z', {timeZone: 'America/New_York'}), '2026-11-01 01:30:00.000 UTC-05:00');
});
test('converts UTC minute values to browser-local editable fields', () => {
  helper();
  assert.equal(inZone('Asia/Kathmandu', `api.toLocalInput('2026-01-15T23:30')`), '2026-01-16T05:15');
  assert.equal(inZone('America/New_York', `api.toLocalInput('2026-01-15T02:30')`), '2026-01-14T21:30');
});
test('parses edited values to absolute UTC instants with fractional offsets', () => {
  helper();
  assert.deepEqual(inZone('Asia/Kathmandu', `api.parseLocalInput('2026-01-16T05:15')`),
    {canonical: '2026-01-15T23:30:00.000Z', offsetLabel: 'UTC+05:45'});
});
test('rejects invalid components, calendar normalization, and spring-forward gaps', () => {
  const api = helper();
  for (const value of ['', '2026-02-29T12:00', '2026-04-31T12:00', '2026-01-01T24:00', '2026-01-01T12:60', '2026-13-01T00:00', '2026-01-00T00:00', '2026-01-01T12:00Z', '2026-1-01T12:00']) {
    assert.ok(api.parseLocalInput(value).error, value);
  }
  assert.ok(inZone('America/New_York', `api.parseLocalInput('2026-03-08T02:30')`).error);
});
test('new repeated local times choose the earlier occurrence', () => {
  helper();
  assert.deepEqual(inZone('America/New_York', `api.parseLocalInput('2026-11-01T01:30')`),
    {canonical: '2026-11-01T05:30:00.000Z', offsetLabel: 'UTC-04:00'});
});
test('unchanged later occurrence preserves original nanoseconds and its own offset', () => {
  helper();
  assert.deepEqual(inZone('America/New_York', `api.resolveBoundary('2026-11-01T01:30', '2026-11-01T01:30', '2026-11-01T06:30:00.123456789Z')`),
    {canonical: '2026-11-01T06:30:00.123456789Z', offsetLabel: 'UTC-05:00'});
});
test('unchanged expanded end field preserves the exact sub-minute boundary', () => {
  const api = helper();
  assert.equal(api.resolveBoundary('2026-01-15T12:01', '2026-01-15T12:01', '2026-01-15T12:00:00.123456790Z').canonical,
    '2026-01-15T12:00:00.123456790Z');
});
test('editing one boundary parses only that boundary', () => {
  helper();
  assert.equal(inZone('UTC', `api.resolveBoundary('2026-01-15T12:02', '2026-01-15T12:01', '2026-01-15T12:00:00.123456789Z')`).canonical,
    '2026-01-15T12:02:00.000Z');
});
test('years below 100 do not acquire a 1900 offset', () => {
  helper();
  assert.equal(inZone('UTC', `api.parseLocalInput('0099-01-15T12:00')`).canonical, '0099-01-15T12:00:00.000Z');
});
test('invalid canonical timestamps fail before enhancement', () => {
  const api = helper();
  assert.throws(() => api.formatTimestamp('invalid', {timeZone: 'UTC'}));
  assert.throws(() => api.toLocalInput('invalid'));
});
