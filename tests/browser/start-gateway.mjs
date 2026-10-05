import {spawn, spawnSync} from 'node:child_process';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer, request as proxyRequest} from 'node:http';

const root = fileURLToPath(new URL('../..', import.meta.url));
const directory = mkdtempSync(join(tmpdir(), 'llmgw-browser-'));
process.on('exit', () => rmSync(directory, {recursive: true, force: true}));
const binary = join(directory, 'gateway');
const build = spawnSync('go', ['build', '-o', binary, './cmd/gateway'], {cwd: root, stdio: 'inherit'});
if (build.status !== 0) process.exit(build.status ?? 1);
// Keep the browser workflow independent from developer/production configuration.
const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('LLMGW_')));
const postgresDSN = process.env.LLMGW_BROWSER_POSTGRES_DSN;
const ports = postgresDSN ? [18082, 18083] : [18081];
const gateways = [];
let stopping = false;
let proxy;
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  proxy?.close();
  for (const gateway of gateways) gateway.kill('SIGTERM');
  process.exitCode = code;
}
for (const port of ports) {
  const gateway = spawn(binary, [], {
    cwd: root, stdio: 'inherit',
    env: {
      ...env,
      LLMGW_LISTEN_ADDRESS: `127.0.0.1:${port}`,
      LLMGW_DATABASE_PATH: join(directory, 'gateway.db'),
      LLMGW_ADMIN_USERNAME: 'browser-test',
      LLMGW_ADMIN_PASSWORD: 'browser-test-password-local',
      LLMGW_API_KEY_HMAC_SECRET: 'browser-test-hmac-secret-local-32-bytes',
      ...(postgresDSN ? {
        LLMGW_DATABASE_DRIVER: 'postgres', LLMGW_DATABASE_URL: postgresDSN,
        LLMGW_CONFIG_POLL_INTERVAL: '100ms', LLMGW_COORDINATION_TIMEOUT: '2s',
      } : {}),
    },
  });
  gateways.push(gateway);
  gateway.on('error', (error) => { console.error(error); stop(1); });
  gateway.on('exit', (code) => { if (!stopping) stop(code ?? 1); });
}
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => stop());
if (postgresDSN) {
  // Wait for both real processes before exposing the load balancer.
  const deadline = Date.now() + 30_000;
  for (const port of ports) {
    while (!stopping) {
      try {
        if ((await fetch(`http://127.0.0.1:${port}/readyz`)).ok) break;
      } catch {}
      if (Date.now() > deadline) { stop(1); break; }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  let next = 0;
  let nextKeys = 0;
  if (!stopping) {
    proxy = createServer((incoming, outgoing) => {
      const index = new URL(incoming.url, 'http://127.0.0.1').pathname === '/admin/keys' ? nextKeys++ : next++;
      const port = ports[index % ports.length];
      const upstream = proxyRequest({
        hostname: '127.0.0.1', port, path: incoming.url,
        method: incoming.method, headers: incoming.headers,
      }, (response) => {
        outgoing.writeHead(response.statusCode, {...response.headers, 'x-test-replica': String(port)});
        response.pipe(outgoing);
      });
      upstream.on('error', () => {
        if (!outgoing.headersSent) outgoing.writeHead(502);
        outgoing.end('Replica unavailable');
      });
      incoming.pipe(upstream);
    });
    proxy.listen(18081, '127.0.0.1');
  }
}
