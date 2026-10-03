import {spawn, spawnSync} from 'node:child_process';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = fileURLToPath(new URL('../..', import.meta.url));
const directory = mkdtempSync(join(tmpdir(), 'llmgw-browser-'));
process.on('exit', () => rmSync(directory, {recursive: true, force: true}));
const binary = join(directory, 'gateway');
const build = spawnSync('go', ['build', '-o', binary, './cmd/gateway'], {cwd: root, stdio: 'inherit'});
if (build.status !== 0) process.exit(build.status ?? 1);
// Keep the browser workflow independent from developer/production configuration.
const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('LLMGW_')));
const gateway = spawn(binary, [], {
  cwd: root, stdio: 'inherit',
  env: {
    ...env,
    LLMGW_LISTEN_ADDRESS: '127.0.0.1:18081',
    LLMGW_DATABASE_PATH: join(directory, 'gateway.db'),
    LLMGW_ADMIN_USERNAME: 'browser-test',
    LLMGW_ADMIN_PASSWORD: 'browser-test-password-local',
    LLMGW_API_KEY_HMAC_SECRET: 'browser-test-hmac-secret-local-32-bytes',
  },
});
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => gateway.kill(signal));
gateway.on('error', (error) => { console.error(error); process.exit(1); });
gateway.on('exit', (code) => process.exit(code ?? 0));
