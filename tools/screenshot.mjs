/**
 * Headless screenshot + console capture over the Chrome DevTools Protocol.
 *
 * Node 22+ ships a WHATWG WebSocket client, so this needs no dependencies.
 * Used to verify that the observer actually renders against live data, and to
 * surface any page-level exception instead of assuming the UI works.
 *
 *   node tools/screenshot.mjs --url http://127.0.0.1:8787 --out shot.png \
 *        --wait 12000 --width 1920 --height 1080 \
 *        --eval "document.getElementById('btn-pause').click()" --eval-wait 1500
 */

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const args = process.argv.slice(2);
function arg(name, def = null) {
  const i = args.indexOf('--' + name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const URL_ = arg('url', 'http://127.0.0.1:8787');
const OUT = arg('out', 'shot.png');
const WAIT = Number(arg('wait', 10000));
const WIDTH = Number(arg('width', 1920));
const HEIGHT = Number(arg('height', 1080));
const EVAL = arg('eval', null);
const EVAL_WAIT = Number(arg('eval-wait', 2000));
const PORT = Number(arg('port', 9333));

const CHROME_CANDIDATES = [
  process.env.CHROME,
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/Applications/Chromium.app/Contents/MacOS/Chromium',
  '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
  '/usr/bin/google-chrome',
  '/usr/bin/chromium',
].filter(Boolean);

const chromePath = CHROME_CANDIDATES.find((p) => {
  try {
    return fs.statSync(p).isFile();
  } catch {
    return false;
  }
});
if (!chromePath) {
  console.error('no chrome binary found; set CHROME=/path/to/chrome');
  process.exit(2);
}

const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'obs-chrome-'));
const chrome = spawn(
  chromePath,
  [
    '--headless=new',
    `--remote-debugging-port=${PORT}`,
    `--user-data-dir=${profile}`,
    `--window-size=${WIDTH},${HEIGHT}`,
    '--hide-scrollbars',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-extensions',
    '--disable-background-timer-throttling',
    '--disable-renderer-backgrounding',
    '--disable-backgrounding-occluded-windows',
    '--force-device-scale-factor=1',
    '--mute-audio',
    'about:blank',
  ],
  { stdio: ['ignore', 'pipe', 'pipe'] },
);
let chromeErr = '';
chrome.stderr.on('data', (d) => (chromeErr += d.toString()));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitForDevtools() {
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}/json/version`);
      if (r.ok) return await r.json();
    } catch {
      /* not up yet */
    }
    await sleep(150);
  }
  throw new Error('devtools endpoint never came up\n' + chromeErr.slice(-2000));
}

function cdp(ws) {
  let id = 0;
  const pending = new Map();
  const listeners = [];
  ws.addEventListener('message', (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.id !== undefined && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      msg.error ? reject(new Error(JSON.stringify(msg.error))) : resolve(msg.result);
    } else if (msg.method) {
      for (const l of listeners) l(msg);
    }
  });
  const send = (method, params = {}) =>
    new Promise((resolve, reject) => {
      const myId = ++id;
      pending.set(myId, { resolve, reject });
      ws.send(JSON.stringify({ id: myId, method, params }));
      setTimeout(() => {
        if (pending.has(myId)) {
          pending.delete(myId);
          reject(new Error('cdp timeout: ' + method));
        }
      }, 30000);
    });
  return { send, on: (fn) => listeners.push(fn) };
}

async function main() {
  await waitForDevtools();

  const t = await (
    await fetch(`http://127.0.0.1:${PORT}/json/new?about:blank`, { method: 'PUT' })
  ).json();

  const ws = new WebSocket(t.webSocketDebuggerUrl);
  await new Promise((res, rej) => {
    ws.addEventListener('open', res, { once: true });
    ws.addEventListener('error', rej, { once: true });
  });

  const client = cdp(ws);
  const consoleLines = [];
  const exceptions = [];

  client.on((msg) => {
    if (msg.method === 'Runtime.consoleAPICalled') {
      const text = (msg.params.args || [])
        .map((a) => (a.value !== undefined ? a.value : a.description || a.type))
        .join(' ');
      consoleLines.push(`[${msg.params.type}] ${text}`);
    } else if (msg.method === 'Runtime.exceptionThrown') {
      const d = msg.params.exceptionDetails;
      exceptions.push(d.exception?.description || d.text || JSON.stringify(d));
    } else if (msg.method === 'Log.entryAdded') {
      const e = msg.params.entry;
      if (e.level === 'error' || e.level === 'warning') consoleLines.push(`[${e.level}] ${e.text} ${e.url || ''}`);
    }
  });

  await client.send('Page.enable');
  await client.send('Runtime.enable');
  await client.send('Log.enable');
  await client.send('Emulation.setDeviceMetricsOverride', {
    width: WIDTH, height: HEIGHT, deviceScaleFactor: 1, mobile: false,
  });

  await client.send('Page.navigate', { url: URL_ });
  await sleep(WAIT);

  if (EVAL) {
    try {
      const r = await client.send('Runtime.evaluate', {
        expression: EVAL, awaitPromise: true, returnByValue: true,
      });
      if (r.exceptionDetails) exceptions.push(JSON.stringify(r.exceptionDetails));
      else if (r.result && r.result.value !== undefined) console.log('eval ->', JSON.stringify(r.result.value));
    } catch (err) {
      exceptions.push('eval failed: ' + err.message);
    }
    await sleep(EVAL_WAIT);
  }

  const shot = await client.send('Page.captureScreenshot', { format: 'png', fromSurface: true });
  fs.writeFileSync(OUT, Buffer.from(shot.data, 'base64'));

  // Pull a few live values out of the page so a screenshot is backed by numbers.
  const probe = await client.send('Runtime.evaluate', {
    expression: `JSON.stringify({
      events: document.getElementById('counter-total')?.textContent,
      rate1s: document.getElementById('m-events-1')?.textContent,
      drawn: document.getElementById('fh-drawn')?.textContent,
      ingest: document.getElementById('ingest-mode')?.textContent,
      bookBid: document.getElementById('book-best-bid')?.textContent,
      bookAsk: document.getElementById('book-best-ask')?.textContent,
      iaP50: document.getElementById('ia-p50')?.textContent,
      procP50: document.getElementById('lat-pr-p50')?.textContent,
      viewClock: document.getElementById('view-clock')?.textContent,
      queue: document.getElementById('view-queue')?.textContent,
      gaps: document.getElementById('ig-gaps')?.textContent,
      bursts: document.querySelectorAll('.burst-item').length
    })`,
    returnByValue: true,
  });
  console.log('page state:', probe.result.value);

  console.log(`screenshot -> ${OUT} (${(fs.statSync(OUT).size / 1024).toFixed(0)} KiB)`);
  if (consoleLines.length) {
    console.log('\n--- console (' + consoleLines.length + ') ---');
    for (const l of consoleLines.slice(0, 40)) console.log(l);
  }
  if (exceptions.length) {
    console.log('\n--- PAGE EXCEPTIONS (' + exceptions.length + ') ---');
    for (const e of exceptions.slice(0, 12)) console.log(e.split('\n').slice(0, 4).join('\n'));
  } else {
    console.log('\nno page exceptions');
  }

  ws.close();
  chrome.kill('SIGKILL');
  fs.rmSync(profile, { recursive: true, force: true });
  process.exit(exceptions.length ? 1 : 0);
}

main().catch((err) => {
  console.error('screenshot tool failed:', err.message);
  console.error(chromeErr.slice(-1500));
  try { chrome.kill('SIGKILL'); } catch { /* ignore */ }
  process.exit(2);
});
