/**
 * Assemble the static (GitHub Pages) build into `dist/`.
 *
 * The observer's ingestion pipeline is host-agnostic, so the same code that
 * runs as a Node process can run inside a browser worker and talk to Binance
 * directly.  This build publishes that variant:
 *
 *   dist/
 *     index.html style.css app.js worker.js decode.js ring.js
 *     shared/   protocol, emitter, runtime, metrics-payload
 *     core/     pipeline, feed, orderbook, metrics, clock
 *
 * Differences from the server build, and why:
 *
 *  - `window.__OBSERVER_STANDALONE__ = true` is injected into index.html, which
 *    switches the page from "read the Node stream" to "run the pipeline here".
 *  - Everything is addressed relatively, because Pages serves the site from
 *    /<repo>/ rather than /.
 *  - No COOP/COEP headers are available on Pages, so SharedArrayBuffer is
 *    unavailable and the worker falls back to transferring chunks to the render
 *    thread.  Same decoder, same record layout; only buffer ownership differs.
 *
 * `server/` is not published: it is the Node host, and the static build does
 * not need it.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DIST = path.join(ROOT, 'dist');

const MARKER = '<script>window.__OBSERVER_STANDALONE__ = true;</script>';

function copyDir(from, to, filter = () => true) {
  fs.mkdirSync(to, { recursive: true });
  let n = 0;
  for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
    const src = path.join(from, entry.name);
    const dst = path.join(to, entry.name);
    if (entry.isDirectory()) {
      n += copyDir(src, dst, filter);
    } else if (filter(entry.name)) {
      fs.copyFileSync(src, dst);
      n++;
    }
  }
  return n;
}

function build() {
  fs.rmSync(DIST, { recursive: true, force: true });
  fs.mkdirSync(DIST, { recursive: true });

  let files = 0;
  files += copyDir(path.join(ROOT, 'web'), DIST);
  files += copyDir(path.join(ROOT, 'shared'), path.join(DIST, 'shared'));
  files += copyDir(path.join(ROOT, 'core'), path.join(DIST, 'core'));

  // Switch the page into in-browser pipeline mode.
  const indexPath = path.join(DIST, 'index.html');
  let html = fs.readFileSync(indexPath, 'utf8');
  if (html.includes('__OBSERVER_STANDALONE__')) {
    throw new Error('index.html already carries the standalone marker');
  }
  const anchor = '<script type="module" src="./app.js"></script>';
  if (!html.includes(anchor)) throw new Error('could not find the app.js script tag to anchor the marker');
  html = html.replace(anchor, `${MARKER}\n${anchor}`);
  fs.writeFileSync(indexPath, html);

  // Keep Pages from running Jekyll over the output.
  fs.writeFileSync(path.join(DIST, '.nojekyll'), '');

  // GitHub Pages does not reliably map .mjs to a JavaScript MIME type, and a
  // module served as octet-stream is refused outright by the browser. Rename
  // the published modules to .js and fix the specifiers that point at them.
  let renamed = 0;
  const walk = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      if (!/\.(mjs|js)$/.test(entry.name)) continue;
      let text = fs.readFileSync(full, 'utf8');
      const fixed = text.replace(/(\.\/|\.\.\/)[^'"]*?\.mjs(?=['"])/g, (m) => m.replace(/\.mjs$/, '.js'));
      if (fixed !== text) fs.writeFileSync(full, fixed);
      if (entry.name.endsWith('.mjs')) {
        fs.renameSync(full, full.replace(/\.mjs$/, '.js'));
        renamed++;
      }
    }
  };
  walk(DIST);

  // A tiny sanity check: no absolute asset URLs survived, or the site would
  // 404 on every file once served from /<repo>/.
  const offenders = [];
  for (const rel of ['index.html', 'app.js', 'worker.js', 'decode.js', 'ring.js']) {
    const text = fs.readFileSync(path.join(DIST, rel), 'utf8');
    for (const m of text.matchAll(/(?:href|src)="(\/[^/][^"]*)"|from\s+'(\/[^/][^']*)'|fetch\('(\/[^/][^']*)'/g)) {
      offenders.push(`${rel}: ${m[1] || m[2] || m[3]}`);
    }
  }
  if (offenders.length) {
    throw new Error('absolute paths would break subpath hosting:\n  ' + offenders.join('\n  '));
  }

  const bytes = totalSize(DIST);
  console.log(`built dist/  ${files + 2} files  ${(bytes / 1024).toFixed(0)} KiB  (${renamed} .mjs renamed to .js)`);
  console.log('  index.html  (standalone marker injected)');
  console.log('  web/        -> dist/');
  console.log('  shared/     -> dist/shared/');
  console.log('  core/       -> dist/core/');
  console.log('  server/     not published (Node host only)');
}

function totalSize(dir) {
  let total = 0;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    total += entry.isDirectory() ? totalSize(p) : fs.statSync(p).size;
  }
  return total;
}

build();
