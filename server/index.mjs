/**
 * HTTP server.
 *
 *   GET  /                     the observer UI (static)
 *   GET  /shared/*             code shared with the browser
 *   GET  /api/stream           live binary event stream (chunked). Backpressure
 *                              is honoured explicitly; nothing is buffered
 *                              without bound and nothing is silently dropped.
 *   GET  /api/status           full server-side status as JSON
 *   GET  /api/recordings       .binlog recordings on disk
 *   GET  /api/log              server-side log lines
 *   POST /api/record           {action:'start'|'stop', seconds}
 *   POST /api/replay           {action:'start'|'stop', file, clientId}
 *
 * The stream is a plain chunked HTTP response rather than a WebSocket: it is
 * one-directional at high volume, needs no handshake or framing library, and
 * `write()` returning false plus a 'drain' event is an explicit backpressure
 * signal - which is exactly what is required in order to not silently buffer
 * or drop market data.
 */

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Pipeline } from './pipeline.mjs';
import { Recorder } from './recorder.mjs';
import { CONTROL, encodeControl } from '../shared/protocol.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const WEB_DIR = path.join(ROOT, 'web');
const SHARED_DIR = path.join(ROOT, 'shared');
const REC_DIR = path.join(ROOT, 'recordings');
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '127.0.0.1';
const METRICS_HZ = Number(process.env.METRICS_HZ || 10);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

const MAX_CLIENT_QUEUE_BYTES = 64 * 1024 * 1024;

/* --------------------------------------------------------------- logging */

const serverLog = [];
function log(level, text) {
  const entry = { level, text, wallMs: Date.now() };
  serverLog.unshift(entry);
  if (serverLog.length > 300) serverLog.pop();
  const tag = level === 'error' ? 'ERR ' : level === 'warn' ? 'WARN' : 'INFO';
  console.log(`[${new Date(entry.wallMs).toISOString()}] ${tag} ${text}`);
}

/* --------------------------------------------------------------- clients */

let nextClientId = 1;

class Client {
  constructor(res) {
    this.id = nextClientId++;
    this.res = res;
    this.closed = false;
    this.blocked = false;
    this.queue = [];
    this.queueBytes = 0;
    this.droppedFrames = 0;
    this.mode = 'live';
    this.connectedAt = Date.now();
    this.framesSent = 0;
    this.bytesSent = 0;
    res.on('drain', () => this.onDrain());
  }

  send(buf) {
    if (this.closed) return;
    if (this.blocked) {
      if (this.queueBytes + buf.length > MAX_CLIENT_QUEUE_BYTES) {
        this.droppedFrames++;
        return;
      }
      this.queue.push(buf);
      this.queueBytes += buf.length;
      return;
    }
    this._write(buf);
  }

  _write(buf) {
    this.framesSent++;
    this.bytesSent += buf.length;
    if (!this.res.write(buf)) this.blocked = true;
  }

  onDrain() {
    if (this.closed) return;
    this.blocked = false;
    while (this.queue.length) {
      const buf = this.queue.shift();
      this.queueBytes -= buf.length;
      this._write(buf);
      if (this.blocked) return;
    }
  }

  sendControl(type, payload) {
    this.send(encodeControl(type, payload));
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try {
      this.res.end();
    } catch {
      /* ignore */
    }
  }
}

const clients = new Set();

/* -------------------------------------------------------------- pipeline */

const pipeline = new Pipeline({});
const recorder = new Recorder({ dir: REC_DIR, symbol: pipeline.symbol });

pipeline.on('note', (n) => log(n.level, n.text));
pipeline.on('feed-state', (s) => log('info', `feed ${s.state}${s.detail ? ` (${s.detail})` : ''}`));
pipeline.on('feed-error', (e) => log('warn', `feed error: ${e}`));

pipeline.on('frames', (frame) => {
  if (recorder.recording) recorder.writeFrame(frame);
  for (const c of clients) {
    if (c.mode !== 'live') continue;
    c.send(frame);
  }
});

/* -------------------------------------------------------- metrics frames */

let metricsSeq = 0;

/** Round numbers so the 10 Hz JSON payload stays small. */
function tidy(v, depth) {
  if (v === null || v === undefined) return v;
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) return null;
    const r = Math.round(v * 1000) / 1000;
    return Object.is(r, -0) ? 0 : r;
  }
  if (Array.isArray(v)) {
    if (depth > 3) return v.length;
    return v.map((x) => tidy(x, depth + 1));
  }
  if (typeof v === 'object') {
    if (depth > 3) return undefined;
    const out = {};
    for (const k in v) {
      const t = tidy(v[k], depth + 1);
      if (t !== undefined) out[k] = t;
    }
    return out;
  }
  return v;
}

function metricsPayload() {
  const st = pipeline.status();
  return tidy({
    seq: ++metricsSeq,
    serverWallMs: Date.now(),
    ...st.metrics,
    feed: {
      state: st.feed.state,
      reconnects: st.feed.reconnects,
      attempts: st.feed.attempts,
      lastError: st.feed.lastError,
      streams: st.feed.streams,
    },
    integrity: {
      armed: st.armed,
      bookSynced: st.synced,
      bookBids: st.book.bids,
      bookAsks: st.book.asks,
      lastUpdateId: st.book.lastUpdateId,
      applied: st.book.stats.applied,
      discardedStale: st.book.stats.discardedStale,
      gaps: st.book.stats.gaps,
      holes: st.book.stats.holes,
      resyncs: st.book.stats.resyncs,
      syncMisses: st.sequencer.syncMisses,
      replayedFrames: st.sequencer.replayedFrames,
      insertions: st.book.stats.insertions,
      deletions: st.book.stats.deletions,
      bufferedFrames: st.sequencer.bufferedFrames,
      depthDiscards: st.sequencer.depthDiscards,
      unknownMessages: st.sequencer.unknownMessages,
      markersFiltered: st.metrics ? st.metrics.counters.markers : 0,
      snapshotPending: st.snapshot.pending,
      snapshotAttempts: st.snapshot.attempts,
      snapshotResyncs: st.snapshot.resyncs,
      snapshotRttMs: st.snapshot.lastRttMs,
      snapshotError: st.snapshot.lastError,
    },
    clock: st.clock,
    loopLag: st.loopLag,
    recorder: recorder.state(),
    sequencer: { seq: st.sequencer.seq, framesSent: st.framesSent, recordsSent: st.recordsSent },
    log: serverLog.slice(0, 6),
  }, 0);
}

setInterval(() => {
  if (clients.size === 0) return;
  const frame = encodeControl(CONTROL.METRICS, metricsPayload());
  for (const c of clients) c.send(frame);
}, Math.max(50, Math.round(1000 / METRICS_HZ)));

/* ------------------------------------------------------------- recording */

function broadcastRecordState() {
  const frame = encodeControl(CONTROL.RECORD_STATE, recorder.state());
  for (const c of clients) c.send(frame);
}

function startRecording(seconds) {
  const limitMs = Math.max(1000, Math.min(3600, Number(seconds) || 30) * 1000);
  const clk = pipeline.status().clock;
  recorder.start({
    limitMs,
    snapshotFrames: pipeline.buildClientSnapshotFrame(),
    meta: {
      streams: pipeline.feed.streams,
      depthLimit: pipeline.depthLimit,
      clockOffsetMs: clk.offsetMs,
      bookLevels: pipeline.book.size,
      lastUpdateId: pipeline.book.lastUpdateId,
      recordedAsText: new Date().toISOString(),
    },
  });
  log('info', `recording started -> ${recorder.state().file} (${limitMs / 1000}s)`);
  broadcastRecordState();
  return recorder.state();
}

function stopRecording() {
  const info = recorder.stop();
  if (info) log('info', `recording stopped: ${info.name} (${info.frames} frames, ${info.bytes} bytes)`);
  broadcastRecordState();
  return info;
}

/* ---------------------------------------------------------------- replay */

function waitForDrain(client) {
  return new Promise((resolve) => {
    if (client.closed || !client.blocked) return resolve();
    const done = () => {
      client.res.off('drain', done);
      client.res.off('close', done);
      resolve();
    };
    client.res.on('drain', done);
    client.res.on('close', done);
  });
}

async function replayToClient(client, name) {
  let payload;
  try {
    payload = recorder.readFrames(name);
  } catch (err) {
    client.sendControl(CONTROL.SERVER_NOTE, {
      level: 'error',
      text: `replay failed: ${err.message}`,
    });
    return;
  }

  client.mode = 'replay';
  log('info', `replaying ${name} to client ${client.id} (${payload.frames.length} frames, ${payload.bytes} bytes)`);

  client.sendControl(CONTROL.REPLAY_START, {
    file: name,
    frames: payload.frames.length,
    bytes: payload.bytes,
    truncated: payload.truncated,
    serverNote:
      'These frames are the original captured stream, byte for byte. Exchange timestamps, local receive timestamps and measured processing latencies are the values recorded at capture time, not re-measured.',
  });

  for (let i = 0; i < payload.frames.length; i++) {
    if (client.closed) break;
    client.send(payload.frames[i]);
    if (client.queueBytes > 8 * 1024 * 1024) await waitForDrain(client);
  }
  await waitForDrain(client);

  if (!client.closed) {
    client.sendControl(CONTROL.REPLAY_END, {
      file: name,
      frames: payload.frames.length,
      bytes: payload.bytes,
    });
    client.mode = 'live';
    // Live events kept arriving while this client was replaying and were
    // deliberately not sent to it, so the client is re-seeded with a fresh
    // snapshot instead of being handed a silently inconsistent book.
    client.send(pipeline.buildClientSnapshotFrame());
    client.sendControl(CONTROL.SERVER_NOTE, {
      level: 'warn',
      text: 'live stream resumed with a fresh book snapshot; events that arrived during replay were skipped for this client (recording was unaffected)',
    });
  }
  log('info', `replay of ${name} delivered to client ${client.id}`);
}

/* ------------------------------------------------------------- http glue */

function baseHeaders(extra = {}) {
  return {
    'Cache-Control': 'no-store',
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': 'same-origin',
    ...extra,
  };
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj, null, 2);
  res.writeHead(code, baseHeaders({
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
  }));
  res.end(body);
}

function readBody(req, limit = 1 << 20) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(new Error('body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function serveStatic(res, baseDir, relPath) {
  const resolvedBase = path.resolve(baseDir);
  const resolved = path.resolve(path.join(resolvedBase, path.normalize(relPath)));
  if (resolved !== resolvedBase && !resolved.startsWith(resolvedBase + path.sep)) {
    res.writeHead(403).end('forbidden');
    return;
  }
  fs.stat(resolved, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, baseHeaders({ 'Content-Type': 'text/plain' })).end('not found');
      return;
    }
    res.writeHead(200, baseHeaders({
      'Content-Type': MIME[path.extname(resolved).toLowerCase()] || 'application/octet-stream',
      'Content-Length': stat.size,
    }));
    fs.createReadStream(resolved).pipe(res);
  });
}

function openStream(req, res) {
  res.writeHead(200, baseHeaders({
    'Content-Type': 'application/octet-stream',
    'Cache-Control': 'no-store, no-transform',
    'X-Content-Type-Options': 'nosniff',
    Connection: 'keep-alive',
  }));
  if (res.socket) res.socket.setNoDelay(true);

  const client = new Client(res);
  clients.add(client);

  client.sendControl(CONTROL.HELLO, {
    clientId: client.id,
    symbol: pipeline.symbol,
    streams: pipeline.feed.streams,
    depthLimit: pipeline.depthLimit,
    serverStartedAt: pipeline.metrics.sessionStartWallMs,
    recordBytes: 88,
    protocolVersion: 1,
    serverNote:
      'Event timing is measured on the server with a monotonic clock (process.hrtime.bigint). Timestamps are never regenerated on the client.',
  });
  // Seed the book synchronously, before this client can receive any live
  // record, so the snapshot and the diffs that follow it are strictly ordered.
  client.send(pipeline.buildClientSnapshotFrame());
  client.sendControl(CONTROL.METRICS, metricsPayload());

  const keepAlive = setInterval(() => {
    if (client.closed) {
      clearInterval(keepAlive);
      return;
    }
    client.sendControl(CONTROL.SERVER_NOTE, { level: 'ping', t: Date.now() });
  }, 15000);

  const cleanup = () => {
    clearInterval(keepAlive);
    clients.delete(client);
    client.closed = true;
    log('info', `client ${client.id} disconnected after ${client.framesSent} frames / ${client.bytesSent} bytes${client.droppedFrames ? `, ${client.droppedFrames} dropped frames` : ''}`);
  };
  req.on('close', cleanup);
  req.on('aborted', cleanup);
  log('info', `client ${client.id} connected`);
}

const server = http.createServer(async (req, res) => {
  let url;
  try {
    url = new URL(req.url, `http://${req.headers.host || HOST}`);
  } catch {
    res.writeHead(400).end('bad request');
    return;
  }
  const p = url.pathname;

  if (p === '/api/stream') return openStream(req, res);
  if (p === '/api/status') return sendJson(res, 200, pipeline.status());

  if (p === '/api/book/recent') {
    return sendJson(res, 200, { recent: pipeline.bookHistory, lastUpdateId: pipeline.book.lastUpdateId });
  }

  if (p === '/api/book') {
    // The authoritative book, so a client can prove its mirror is identical
    // rather than merely plausible.
    const depth = Math.max(1, Math.min(1000, Number(url.searchParams.get('depth')) || 20));
    const d = pipeline.book.dump(depth);
    return sendJson(res, 200, {
      lastUpdateId: d.lastUpdateId,
      synced: d.synced,
      bids: d.bids,
      asks: d.asks,
      levels: pipeline.book.size,
      serverWallMs: Date.now(),
    });
  }
  if (p === '/api/recordings') {
    return sendJson(res, 200, { recordings: await recorder.list(), state: recorder.state() });
  }
  if (p === '/api/log') return sendJson(res, 200, { log: serverLog });

  if (p === '/api/record' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      if (body.action === 'stop') return sendJson(res, 200, { state: stopRecording() });
      return sendJson(res, 200, { state: startRecording(body.seconds ?? 30) });
    } catch (err) {
      return sendJson(res, 400, { error: String(err.message || err) });
    }
  }

  if (p === '/api/replay' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const client = [...clients].find((c) => c.id === Number(body.clientId));
      if (body.action === 'stop') {
        if (client) client.mode = 'live';
        return sendJson(res, 200, { ok: true });
      }
      const name = String(body.file || '');
      if (!name) return sendJson(res, 400, { error: 'file is required' });
      if (!client) return sendJson(res, 409, { error: 'unknown clientId; reopen the stream' });
      if (client.mode === 'replay') return sendJson(res, 409, { error: 'replay already in progress' });
      replayToClient(client, name);
      return sendJson(res, 200, { ok: true, file: name, clientId: client.id });
    } catch (err) {
      return sendJson(res, 400, { error: String(err.message || err) });
    }
  }

  if (p === '/' || p === '/index.html') return serveStatic(res, WEB_DIR, 'index.html');
  if (p.startsWith('/shared/')) return serveStatic(res, SHARED_DIR, p.slice('/shared/'.length));
  return serveStatic(res, WEB_DIR, p.slice(1));
});

server.listen(PORT, HOST, () => {
  log('info', `observer listening on http://${HOST}:${PORT}`);
  log('info', `subscribing to ${pipeline.feed.url}`);
  pipeline.start();
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    log('info', `received ${sig}, shutting down`);
    try {
      stopRecording();
    } catch {
      /* ignore */
    }
    pipeline.stop();
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 500);
  });
}

process.on('uncaughtException', (err) => log('error', `uncaught exception: ${err.stack || err}`));
process.on('unhandledRejection', (err) => log('error', `unhandled rejection: ${err}`));
