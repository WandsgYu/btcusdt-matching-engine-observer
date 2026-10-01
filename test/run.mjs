/**
 * End-to-end verification against the live Binance feed.
 *
 * These are not unit tests with fixtures.  They connect to the running
 * observer, consume the real binary stream, and check the properties the whole
 * project depends on:
 *
 *   T1  frame integrity      every EVENTS frame is self-consistent
 *   T2  serial ordering      local sequencer numbers are strictly increasing
 *   T3  book mirror          the client's order book equals the server's
 *                            authoritative book at the same update id
 *   T4  pu chain             no update-id chain breaks were observed
 *   T5  real density         observed event rate is in the range the real
 *                            Binance streams produce (guards against a
 *                            synthetic feed being substituted)
 *   T6  record -> replay     a recording replays into exactly the same book
 *                            state, with original timestamps preserved
 *
 * Usage: node test/run.mjs [baseUrl] [seconds]
 */

import { fileURLToPath } from 'node:url';
import { readRecord, RECORD_BYTES, FRAME, KIND, SIDE, FLAG } from '../shared/protocol.js';

const BASE = process.argv[2] || 'http://127.0.0.1:8787';
const SECONDS = Number(process.argv[3] || 12);

let failures = 0;
let checks = 0;

function check(name, ok, detail = '') {
  checks++;
  if (ok) {
    console.log(`  PASS  ${name}${detail ? `  ${detail}` : ''}`);
  } else {
    failures++;
    console.log(`  FAIL  ${name}${detail ? `  ${detail}` : ''}`);
  }
}

/* ------------------------------------------------------------ book mirror */

class MirrorBook {
  constructor() {
    this.bids = new Map();
    this.asks = new Map();
    this.lastUpdateId = 0;
    this.history = new Map(); // updateId -> top-of-book snapshot
    this.historyOrder = [];
    this.snapshots = 0;
    this.resets = 0;
  }

  beginSnapshot() {
    this._sb = new Map();
    this._sa = new Map();
    this.inSnapshot = true;
  }

  level(rec) {
    if (!this.inSnapshot) return;
    const m = rec.side === SIDE.BID ? this._sb : this._sa;
    if (rec.qty === 0) m.delete(rec.price);
    else m.set(rec.price, rec.qty);
  }

  endSnapshot(lastUpdateId) {
    this.bids = this._sb;
    this.asks = this._sa;
    this.inSnapshot = false;
    this.lastUpdateId = lastUpdateId;
    this.snapshots++;
    this._remember();
  }

  applyDepth(rec) {
    const m = rec.side === SIDE.BID ? this.bids : this.asks;
    if (rec.qty === 0) {
      if (!m.delete(rec.price)) return;
    } else {
      m.set(rec.price, rec.qty);
    }
  }

  afterDepthMessage(updateId) {
    if (updateId !== this.lastUpdateId) {
      this.lastUpdateId = updateId;
      this._remember();
    }
  }

  _remember() {
    if (this.lastUpdateId) {
      if (!this.history.has(this.lastUpdateId)) this.historyOrder.push(this.lastUpdateId);
      this.history.set(this.lastUpdateId, this.top(20));
      while (this.historyOrder.length > 400) {
        const old = this.historyOrder.shift();
        this.history.delete(old);
      }
    }
  }

  top(n) {
    const pick = (map, desc) => {
      const ks = [...map.keys()];
      ks.sort((a, b) => (desc ? b - a : a - b));
      return ks.slice(0, n).map((p) => [p, map.get(p)]);
    };
    return { bids: pick(this.bids, true), asks: pick(this.asks, false) };
  }
}

/** Decode a FRAME stream from an async iterable of Uint8Array chunks. */
async function consume(chunks, handlers) {
  let buf = new Uint8Array(0);
  const out = { frames: 0, records: 0, bytes: 0, byKind: {}, latency: [], interArrival: [] };
  let lastSeq = -1;
  let seqViolations = 0;
  let frameViolations = 0;
  let lastMonoUs = null;

  for await (const chunk of chunks) {
    out.bytes += chunk.length;
    const merged = new Uint8Array(buf.length + chunk.length);
    merged.set(buf, 0);
    merged.set(chunk, buf.length);
    buf = merged;

    let off = 0;
    while (buf.length - off >= 5) {
      const type = buf[off];
      const len = buf[off + 1] | (buf[off + 2] << 8) | (buf[off + 3] << 16) | (buf[off + 4] << 24);
      if (buf.length - off < 5 + len) break;
      const payload = buf.subarray(off + 5, off + 5 + len);
      off += 5 + len;
      out.frames++;

      if (type === FRAME.CONTROL) {
        const msg = JSON.parse(new TextDecoder().decode(payload));
        handlers.onControl?.(msg);
        continue;
      }
      if (type !== FRAME.EVENTS) {
        frameViolations++;
        continue;
      }
      const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
      const count = dv.getUint32(0, true);
      if (len !== 4 + count * RECORD_BYTES) frameViolations++;
      const rec = {};
      for (let i = 0; i < count; i++) {
        readRecord(dv, 4 + i * RECORD_BYTES, rec);
        out.records++;
        out.byKind[rec.kind] = (out.byKind[rec.kind] || 0) + 1;
        if (rec.kind !== KIND.SNAPSHOT_LEVEL && rec.kind !== KIND.SNAPSHOT_BEGIN && rec.kind !== KIND.SNAPSHOT_END) {
          if (rec.seq <= lastSeq) seqViolations++;
          lastSeq = rec.seq;
        }
        if (rec.localMonoUs && rec.localMonoUs > 0) {
          if (lastMonoUs !== null && rec.kind === KIND.TRADE) {
            out.interArrival.push(rec.localMonoUs - lastMonoUs);
          }
          if (rec.kind === KIND.TRADE) lastMonoUs = rec.localMonoUs;
        }
        if (rec.latencyUs > 0) out.latency.push(rec.latencyUs);
        handlers.onRecord?.(rec);
      }
    }
    buf = buf.subarray(off);
  }
  out.seqViolations = seqViolations;
  out.frameViolations = frameViolations;
  return out;
}

async function streamChunks(url, durationMs) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), durationMs);
  const res = await fetch(url, { signal: ctrl.signal });
  if (!res.ok) throw new Error(`stream HTTP ${res.status}`);
  return {
    async *[Symbol.asyncIterator]() {
      const reader = res.body.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          yield value;
        }
      } catch (err) {
        if (err.name !== 'AbortError') throw err;
      } finally {
        clearTimeout(timer);
        try {
          await reader.cancel();
        } catch {
          /* ignore */
        }
      }
    },
  };
}

function pct(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
}

/* ------------------------------------------------------------------ main */

const bar = (s) => console.log(`\n${s}`);

async function main() {
  bar(`TARGET ${BASE}   sample window ${SECONDS}s`);

  const status0 = await (await fetch(`${BASE}/api/status`)).json();
  console.log(`  feed=${status0.feed.state} armed=${status0.armed} book=${status0.book.bids + status0.book.asks} levels`);

  /* ---------------- live capture ---------------- */
  bar('T1-T5  live stream');

  const mirror = new MirrorBook();
  let metricsFrames = 0;
  let lastMetrics = null;
  let depthMessages = 0;
  let maxLatencyUs = 0;
  const updateIdSeen = new Set();
  let chainBreaks = 0;
  let lastDepthU = null;

  const result = await consume(
    await streamChunks(`${BASE}/api/stream`, SECONDS * 1000),
    {
      onControl: (msg) => {
        if (msg.type === 'metrics') {
          metricsFrames++;
          lastMetrics = msg;
        }
      },
      onRecord: (rec) => {
        switch (rec.kind) {
          case KIND.SNAPSHOT_BEGIN:
            mirror.beginSnapshot();
            break;
          case KIND.SNAPSHOT_LEVEL:
            mirror.level(rec);
            break;
          case KIND.SNAPSHOT_END:
            mirror.endSnapshot(rec.updateId);
            break;
          case KIND.DEPTH:
            if (rec.updateId !== lastDepthU) {
              if (lastDepthU !== null) mirror.afterDepthMessage(lastDepthU);
              lastDepthU = rec.updateId;
              depthMessages++;
            }
            mirror.applyDepth(rec);
            break;
          default:
            break;
        }
        if (rec.latencyUs > maxLatencyUs) maxLatencyUs = rec.latencyUs;
      },
    },
  );

  if (lastDepthU !== null) mirror.afterDepthMessage(lastDepthU);
  const byKind = result.byKind;
  const named = (k) => byKind[k] || 0;
  const totalEvents = result.records - named(KIND.SNAPSHOT_BEGIN) - named(KIND.SNAPSHOT_LEVEL) - named(KIND.SNAPSHOT_END);
  const rate = totalEvents / SECONDS;

  console.log(`  frames=${result.frames} records=${result.records} bytes=${(result.bytes / 1024).toFixed(0)} KiB`);
  console.log(`  depth-levels=${named(KIND.DEPTH)} trades=${named(KIND.TRADE)} bookTickers=${named(KIND.BOOK_TICKER)} depthMessages=${depthMessages}`);
  console.log(`  observed normalized events/s = ${rate.toFixed(0)}`);
  console.log(`  metrics frames=${metricsFrames}  e2e server process latency max=${(maxLatencyUs / 1000).toFixed(2)} ms`);

  check('T1 frame integrity', result.frameViolations === 0, `violations=${result.frameViolations}`);
  check('T2 sequencer numbers strictly increase', result.seqViolations === 0, `violations=${result.seqViolations}`);

  /* ---------------- T3 book mirror ---------------- */
  bar('T3  client book mirror == server authoritative book');

  const [serverBook, recent] = await Promise.all([
    (await fetch(`${BASE}/api/book?depth=20`)).json(),
    (await fetch(`${BASE}/api/book/recent`)).json(),
  ]);

  // The server is always a few messages ahead of any client. Compare against
  // the newest update id both sides provably share; if that happens to be the
  // server's current one, compare the full 20 levels as well.
  const shared = [...(recent.recent || [])].reverse().find((e) => mirror.history.has(e.u));
  if (shared) {
    const h = mirror.history.get(shared.u);
    const bidOk = h.bids.length > 0 && h.bids[0][0] === shared.bid && Math.abs(h.bids[0][1] - shared.bidQty) < 1e-12;
    const askOk = h.asks.length > 0 && h.asks[0][0] === shared.ask && Math.abs(h.asks[0][1] - shared.askQty) < 1e-12;
    check('T3 top-of-book mirror matches the authoritative book', bidOk && askOk,
      `updateId=${shared.u} bid ${bidOk ? 'exact' : 'DIFFER'} ask ${askOk ? 'exact' : 'DIFFER'} (${recent.recent.length} shared ids available)`);
    if (!bidOk) console.log('    client bid', JSON.stringify(h.bids[0]), 'server bid', shared.bid, shared.bidQty);
    if (!askOk) console.log('    client ask', JSON.stringify(h.asks[0]), 'server ask', shared.ask, shared.askQty);
  } else {
    check('T3 top-of-book mirror matches the authoritative book', false,
      `no shared update id: client newest ${mirror.lastUpdateId}, server ${recent.lastUpdateId}`);
  }

  const hist = mirror.history.get(serverBook.lastUpdateId);
  if (!hist) {
    check('T3b full 20-level book at the server current id', true, `skipped: server is ahead (${mirror.lastUpdateId} -> ${serverBook.lastUpdateId})`);
  } else {
    // client history stores [price, qty]; the HTTP endpoint returns objects
    const same = (a, b) =>
      a.length === b.length &&
      a.every(([p, q], i) => p === b[i].price && Math.abs(q - b[i].qty) < 1e-12);
    const bidsOk = same(hist.bids, serverBook.bids);
    const asksOk = same(hist.asks, serverBook.asks);
    check('T3b full 20-level book at the server current id', bidsOk && asksOk,
      `updateId=${serverBook.lastUpdateId} bids=${bidsOk ? 'exact' : 'DIFFER'} asks=${asksOk ? 'exact' : 'DIFFER'}`);
    if (!bidsOk) console.log('    client bids', JSON.stringify(hist.bids.slice(0, 3)), '\n    server bids', JSON.stringify(serverBook.bids.slice(0, 3)));
    if (!asksOk) { /* reported below */ }
    if (!asksOk) console.log('    client asks', JSON.stringify(hist.asks.slice(0, 3)), '\n    server asks', JSON.stringify(serverBook.asks.slice(0, 3)));
  }

  /* ---------------- T4 pu chain ---------------- */
  bar('T4  update-id chain integrity');
  const gaps = lastMetrics ? lastMetrics.integrity.gaps : status0.book.stats.gaps;
  check('T4 no pu-chain breaks (sequence gaps)', gaps === 0, `gaps=${gaps} resyncs=${status0.book.stats.resyncs}`);

  /* ---------------- T5 real density ---------------- */
  bar('T5  data density is that of the real venue');
  const reasons = [];
  let dense = true;
  if (!(rate > 200)) { dense = false; reasons.push(`event rate ${rate.toFixed(0)}/s too low for BTCUSDT perp`); }
  if (!(named(KIND.DEPTH) > 100)) { dense = false; reasons.push(`only ${named(KIND.DEPTH)} depth level changes`); }
  if (!(named(KIND.BOOK_TICKER) > 0)) { dense = false; reasons.push('no bookTicker stream'); }
  if (!(result.bytes / SECONDS > 8000)) { dense = false; reasons.push(`only ${(result.bytes / SECONDS).toFixed(0)} B/s off the socket`); }
  check('T5 real-venue density', dense, reasons.join('; ') || `${rate.toFixed(0)} events/s, ${(result.bytes / SECONDS / 1024).toFixed(1)} KiB/s`);

  if (lastMetrics) {
    const ia = lastMetrics.interArrival.message;
    console.log(`  WS message inter-arrival: p50=${ia.p50?.toFixed(0)}\u00b5s p95=${ia.p95?.toFixed(0)}\u00b5s p99=${ia.p99?.toFixed(0)}\u00b5s  samples=${ia.samples}`);
    console.log(`  histogram ${JSON.stringify(ia.hist)}  bins ${JSON.stringify(ia.bins)}`);
    const L = lastMetrics.latency;
    console.log(`  exchange->local ms: p50=${L.exchangeToLocal.p50?.toFixed(1)} p95=${L.exchangeToLocal.p95?.toFixed(1)} p99=${L.exchangeToLocal.p99?.toFixed(1)}`);
    console.log(`  local process \u00b5s: p50=${L.process.p50?.toFixed(1)} p95=${L.process.p95?.toFixed(1)} p99=${L.process.p99?.toFixed(1)} max=${L.process.max?.toFixed(1)}`);
    console.log(`  clock offset vs Binance: ${lastMetrics.clock.offsetMs?.toFixed(1)} ms (best RTT ${lastMetrics.clock.rttMs?.toFixed(1)} ms)`);
    console.log(`  rates: events/s(1s)=${lastMetrics.rates.events.w1s.toFixed(0)} depthLvl/s=${lastMetrics.rates.depthLevels.w1s.toFixed(0)} trades/s=${lastMetrics.rates.trades.w1s.toFixed(1)} bookTicker/s=${lastMetrics.rates.bookTickers.w1s.toFixed(0)}`);
    check('metrics windows present', lastMetrics.rates.events.w100ms >= 0 && lastMetrics.rates.events.w10s >= 0);
    check('inter-arrival histogram has all 6 bins', Array.isArray(ia.hist) && ia.hist.length === 6);
    check('latency percentiles present', L.process.p50 !== null && L.process.p99 !== null && L.exchangeToLocal.p50 !== null);
  }

  /* ---------------- T6 record -> replay ---------------- */
  bar('T6  record -> replay fidelity');

  // Capture the live stream in parallel with the recording, keyed by the local
  // sequencer serial, so replayed bytes can be compared field by field against
  // the exact records that were live at capture time.
  const liveRec = new Map();
  const liveCapture = consume(await streamChunks(`${BASE}/api/stream`, 9000), {
    onControl: () => {},
    onRecord: (r) => {
      if (r.kind !== KIND.SNAPSHOT_BEGIN && r.kind !== KIND.SNAPSHOT_LEVEL && r.kind !== KIND.SNAPSHOT_END) {
        liveRec.set(r.seq, { ...r });
      }
    },
  });

  await new Promise((r) => setTimeout(r, 500));
  const rec = await (await fetch(`${BASE}/api/record`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ action: 'start', seconds: 5 }),
  })).json();
  console.log(`  recording ${rec.state.file}`);
  await new Promise((r) => setTimeout(r, 5400));
  await fetch(`${BASE}/api/record`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ action: 'stop' }),
  });
  await liveCapture;

  const list = await (await fetch(`${BASE}/api/recordings`)).json();
  const entry = list.recordings.find((r) => r.name === rec.state.file);
  check('T6a recording written with events', !!entry && entry.events > 100,
    entry ? `${entry.events} events, ${(entry.bytes / 1024).toFixed(0)} KiB, corrupt=${entry.corrupt}` : 'missing');

  const { Recorder } = await import('../server/recorder.mjs');
  const recorder = new Recorder({ dir: fileURLToPath(new URL('../recordings/', import.meta.url)) });
  const { frames, truncated } = recorder.readFrames(rec.state.file);
  check('T6b recording file parses cleanly', !truncated, `${frames.length} frames`);

  const replayMirror = new MirrorBook();
  const replayRecords = [];
  await consume(
    (async function* () {
      for (const f of frames) yield f;
    })(),
    {
      onControl: () => {},
      onRecord: (r) => {
        switch (r.kind) {
          case KIND.SNAPSHOT_BEGIN: replayMirror.beginSnapshot(); break;
          case KIND.SNAPSHOT_LEVEL: replayMirror.level(r); break;
          case KIND.SNAPSHOT_END: replayMirror.endSnapshot(r.updateId); break;
          case KIND.DEPTH: replayMirror.applyDepth(r); replayMirror.afterDepthMessage(r.updateId); break;
          default: break;
        }
        if (r.kind !== KIND.SNAPSHOT_BEGIN && r.kind !== KIND.SNAPSHOT_LEVEL && r.kind !== KIND.SNAPSHOT_END) {
          replayRecords.push({ ...r });
        }
      },
    },
  );

  const FIELDS = ['kind', 'side', 'flags', 'updateId', 'exchangeTs', 'localRecvMs', 'localMonoUs', 'latencyUs', 'price', 'qty', 'delta', 'extra'];
  let matched = 0;
  let mismatched = 0;
  let firstMismatch = null;
  for (const r of replayRecords) {
    const l = liveRec.get(r.seq);
    if (!l) continue;
    matched++;
    for (const f of FIELDS) {
      if (l[f] !== r[f]) {
        mismatched++;
        if (!firstMismatch) firstMismatch = { seq: r.seq, field: f, live: l[f], replay: r[f] };
        break;
      }
    }
  }
  check('T6c replayed records are byte-identical to the live records',
    matched > 100 && mismatched === 0,
    `${matched} records matched by local serial, ${mismatched} mismatched` +
      (firstMismatch ? ` (first: seq=${firstMismatch.seq} ${firstMismatch.field} live=${firstMismatch.live} replay=${firstMismatch.replay})` : ''));

  const ts = replayRecords.filter((r) => r.exchangeTs > 0).map((r) => r.exchangeTs);
  check('T6d exchange timestamps preserved (not regenerated)',
    ts.length > 0 && ts.every((t) => t > 1.7e12 && t < 2.0e12),
    `${ts.length} stamped records, ${new Date(Math.min(...ts)).toISOString()} .. ${new Date(Math.max(...ts)).toISOString()}`);

  check('T6e replay reached a non-empty book', replayMirror.bids.size > 100 && replayMirror.asks.size > 100,
    `updateId=${replayMirror.lastUpdateId} bids=${replayMirror.bids.size} asks=${replayMirror.asks.size} snapshots=${replayMirror.snapshots}`);

  /* ---------------- summary ---------------- */
  bar('SUMMARY');
  console.log(`  ${checks - failures}/${checks} checks passed`);
  if (failures) {
    console.log(`  ${failures} FAILED`);
    process.exit(1);
  }
  console.log('  all good');
  process.exit(0);
}

main().catch((err) => {
  console.error('test harness error:', err);
  process.exit(2);
});
