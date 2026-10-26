import {spawnSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

export function analyticsFixtures() {
  const directory = mkdtempSync(join(tmpdir(), 'llmgw-analytics-'));
  const result = spawnSync('go', ['test', './internal/web', '-run', '^TestWriteAnalyticsBrowserFixtures$', '-count=1'], {
    cwd: fileURLToPath(new URL('../..', import.meta.url)),
    env: {...process.env, LLMGW_BROWSER_ANALYTICS_FIXTURE_DIR: directory}, encoding: 'utf8',
  });
  if (result.status !== 0) {
    rmSync(directory, {recursive: true, force: true});
    throw new Error(`Analytics fixture export failed: ${result.stdout}\n${result.stderr}`);
  }
  return {
    async route(page, name = 'normal', transform = (html) => html) {
      await page.route('**/admin/analytics?**', (route) => route.fulfill({
        contentType: 'text/html', body: transform(readFileSync(join(directory, `${name}.html`), 'utf8')),
      }));
    },
    close() { rmSync(directory, {recursive: true, force: true}); },
  };
}
