/**
 * BTCUSDT PERP · MATCHING ENGINE OBSERVER — render + playback layer.
 *
 * Division of labour, and why it matters:
 *
 *   Node server   ingests Binance, validates the order book against the
 *                 exchange's own `pu` update-id chain, and measures every
 *                 timestamp with a monotonic clock.  It is the authority.
 *   ingest worker reads the binary stream and decodes records into a
 *                 SharedArrayBuffer ring.  It never slows down for the UI.
 *   this file     is a *reader*.  It walks the ring at whatever rate the user
 *                 asks for (realtime, slowed, paused, single-stepped) and
 *                 paints.  It cannot lose an event, because it never owns the
 *                 arrival path - it only decides how fast to look at history.
 *
 * That separation is what makes TIME DILATION honest: 1000x slower does not
 * slow the market down, it moves the playhead through already-captured records
 * a thousand times more slowly.
 */

import {
  KIND, KIND_FIREHOSE, SIDE, FLAG, RECORD_BYTES,
} from './shared/protocol.js';
import { EventRing, DEFAULT_CAPACITY } from './ring.js';
import { decodeChunk, joinChunks } from './decode.js';

/* ═══════════════════════════════════ state ═══════════════════════════════ */

const params = new URLSearchParams(location.search);
const RING_CAPACITY = Math.max(1 << 14, Number(params.get('ring')) || DEFAULT_CAPACITY);

/**
 * Two ways to run the same observer:
 *
 *   server      a Node process ingests Binance and streams binary frames over
 *               HTTP.  Needed for disk recordings.
 *   standalone  the whole pipeline runs inside this page's worker, connected to
 *               Binance directly.  This is what makes the app deployable as a
 *               static site; recordings live in memory instead of on disk.
 *
 * The build injects the flag for the Pages bundle; `?mode=standalone` forces it
 * locally, which is how the static path is tested without deploying.
 */
const STANDALONE = window.__OBSERVER_STANDALONE__ === true || params.get('mode') === 'standalone';

/**
 * In the static build, ask a service worker to make this page cross-origin
 * isolated.  Without it Chrome clamps `performance.now()` to ~100 us, which is
 * coarse enough to swallow the sub-100 us bins of the inter-arrival histogram,
 * and SharedArrayBuffer stays unavailable so the worker must copy every frame.
 * GitHub Pages cannot send COOP/COEP itself; a service worker can.
 *
 * Reloads at most once per session, so a browser that refuses to isolate us
 * cannot put the page in a reload loop.
 */
const COI_RELOAD_FLAG = 'observer-coi-reloaded';

function enableCrossOriginIsolation() {
  if (crossOriginIsolated) return;
  if (!('serviceWorker' in navigator) || !window.isSecureContext) return;
  navigator.serviceWorker
    .register('./coi-serviceworker.js', { scope: './' })
    .then((reg) => {
      if (navigator.serviceWorker.controller) return;
      if (sessionStorage.getItem(COI_RELOAD_FLAG)) return;
      const reload = () => {
        sessionStorage.setItem(COI_RELOAD_FLAG, '1');
        location.reload();
      };
      const sw = reg.installing || reg.waiting || reg.active;
      if (!sw) return;
      if (sw.state === 'activated') reload();
      else sw.addEventListener('statechange', () => {
        if (sw.state === 'activated') reload();
      });
    })
    .catch(() => {
      /* no isolation available; the app runs correctly, just coarser */
    });
}

const workerParams = new URLSearchParams(location.search);
if (STANDALONE) workerParams.set('mode', 'standalone');
const WORKER_URL = './worker.js?' + workerParams.toString();

const S = {
  ring: null,
  shared: false,
  head: 0,                 // records written so far
  cursor: 0,               // next record index the playhead will consume
  lastRendered: 0,         // last consumed index (for panels)
  overwritten: 0,          // records lost because the playhead fell behind
  skippedFormat: 0,        // consumed but never formatted (scrolled past)
  paused: false,
  rate: 1,
  dilation: 1,
  mode: 'live',
  playheadMs: 0,
  lastFrameMs: 0,
  lastConsumeMs: 0,
  replayStart: -1,
  replayEnd: -1,
  replayFile: null,
  clientId: null,
  metrics: null,
  hello: null,
  recState: null,
  recordings: [],
  workerStats: { frames: 0, records: 0, badFrames: 0, chunks: 0, bytes: 0, controls: 0, reconnects: 0 },
  renderedTotal: 0,
  renderRate: 0,
  renderRateAcc: 0,
  renderRateWall: 0,
  lastSnapIndex: -1,
  /**
   * True when the playhead has been moved (seek, replay load, rate change), so
   * its virtual market clock must be re-anchored to the first record of the new
   * segment before any time-based consumption happens.
   */
  needsAnchor: true,
  /**
   * Pending single-step requests. STEP means "advance the sequencer by exactly
   * one event" - which, sitting at the live edge, may require waiting for that
   * event to arrive. Requests queue here and are satisfied by the render loop
   * as ingress supplies them, so a burst of clicks advances exactly that many
   * events instead of being silently dropped.
   */
  stepPending: 0,
  bookReady: false,
  iaMode: 'message',
  sim: false,
  serverLog: [],
};

const book = {
  bids: new Map(),
  asks: new Map(),
  bidPulse: new Map(),
  askPulse: new Map(),
  levels: 0,
  lastUpdateId: 0,
  snapshots: 0,
  applied: 0,
  inSnapshot: false,
  tmpB: null,
  tmpA: null,
};

const EFFECTIVE_HELP = [1, 10, 100, 1000];

/* ════════════════════════════════ helpers ═══════════════════════════════ */

const $ = (id) => document.getElementById(id);
/** Monotonic, same base as requestAnimationFrame. Never mixes with wall ms. */
const perfNow = () => performance.now();
const clamp = (v, a, b) => (v < a ? a : v > b ? b : v);

const nf0 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const nf1 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 });
const nf2 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
const nf3 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 });

const fmtInt = (n) => (Number.isFinite(n) ? nf0.format(n) : '—');
const fmt2 = (n) => (Number.isFinite(n) ? nf2.format(n) : '—');
const fmt3 = (n) => (Number.isFinite(n) ? nf3.format(n) : '—');
const fmt1 = (n) => (Number.isFinite(n) ? nf1.format(n) : '—');

function fmtCompact(n) {
  if (!Number.isFinite(n)) return '—';
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (a >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return nf0.format(n);
}

function fmtBytes(n) {
  if (!Number.isFinite(n)) return '—';
  if (n >= 1 << 30) return (n / (1 << 30)).toFixed(2) + ' GiB';
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(1) + ' MiB';
  if (n >= 1 << 10) return (n / (1 << 10)).toFixed(1) + ' KiB';
  return n + ' B';
}

/** Microseconds -> human, switching unit at the point it stops being readable. */
function fmtUs(us) {
  if (!Number.isFinite(us)) return '—';
  if (us < 1) return us.toFixed(2) + ' \u00b5s';
  if (us < 1000) return us.toFixed(us < 10 ? 1 : 0) + ' \u00b5s';
  if (us < 1e6) return (us / 1000).toFixed(us < 1e4 ? 2 : 1) + ' ms';
  return (us / 1e6).toFixed(2) + ' s';
}

function fmtMs(ms) {
  if (!Number.isFinite(ms)) return '—';
  if (Math.abs(ms) < 1) return (ms * 1000).toFixed(0) + ' \u00b5s';
  if (Math.abs(ms) < 1000) return ms.toFixed(1) + ' ms';
  return (ms / 1000).toFixed(2) + ' s';
}

function fmtHMS(ms) {
  if (!Number.isFinite(ms) || ms < 0) return '00:00:00';
  const t = Math.floor(ms / 1000);
  const h = String(Math.floor(t / 3600)).padStart(2, '0');
  const m = String(Math.floor((t % 3600) / 60)).padStart(2, '0');
  const s = String(t % 60).padStart(2, '0');
  return `${h}:${m}:${s}`;
}

/** HH:MM:SS.ffffff — microsecond display, the resolution the server measured. */
function fmtClock(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return '—';
  const d = new Date(ms);
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  const us = Math.floor((ms % 1000) * 1000);
  return `${hh}:${mm}:${ss}.${String(us).padStart(6, '0')}`;
}

const sideName = (s) => (s === SIDE.BID ? 'BID' : s === SIDE.ASK ? 'ASK' : '—');
const pad = (s, n) => (s.length >= n ? s : s + ' '.repeat(n - s.length));
const padL = (s, n) => (s.length >= n ? s : ' '.repeat(n - s.length) + s);
const signed = (v) => (v >= 0 ? '+' : '') + v.toFixed(3);

/* ═════════════════════════════ canvas plumbing ══════════════════════════ */

const FONT = '12px ui-monospace, "SF Mono", Menlo, Consolas, monospace';
const canvases = {};

function surface(id, font) {
  let c = canvases[id];
  if (!c) {
    const el = $(id);
    c = canvases[id] = { el, ctx: el.getContext('2d', { alpha: false }), w: 0, h: 0, dpr: 0, font };
  }
  const dpr = window.devicePixelRatio || 1;
  const r = c.el.getBoundingClientRect();
  const w = Math.max(1, Math.round(r.width));
  const h = Math.max(1, Math.round(r.height));
  if (c.w !== w || c.h !== h || c.dpr !== dpr) {
    c.w = w;
    c.h = h;
    c.dpr = dpr;
    c.el.width = Math.round(w * dpr);
    c.el.height = Math.round(h * dpr);
  }
  const ctx = c.ctx;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.font = c.font || FONT;
  ctx.textBaseline = 'middle';
  return c;
}

/* ══════════════════════════ stream + ring wiring ════════════════════════ */

const worker = new Worker(WORKER_URL, { type: 'module' });

worker.onerror = (err) => {
  // A dead ingest worker means no data at all; say so instead of showing a
  // blank instrument panel that looks like a quiet market.
  const msg = err.message || 'worker failed';
  setText('feed-state', 'INGEST ERROR', 'st-bad');
  console.error('ingest worker error:', msg, err.filename, err.lineno);
  for (const id of ['firehose', 'seq-canvas', 'book']) {
    const el = $(id);
    if (!el) continue;
    const ctx = el.getContext('2d');
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.fillStyle = '#080c11';
    ctx.fillRect(0, 0, el.width, el.height);
    ctx.fillStyle = '#ff5f56';
    ctx.font = '13px ui-monospace, monospace';
    ctx.fillText('INGEST WORKER FAILED: ' + msg, 12, 24);
  }
};

worker.onmessage = (ev) => {
  const m = ev.data;
  switch (m.type) {
    case 'ready':
      S.shared = m.shared;
      if (m.shared && m.buffer) {
        S.ring = new EventRing(m.capacity, { shared: true, buffer: m.buffer });
      } else {
        S.ring = new EventRing(m.capacity, { shared: false });
      }
      $('ingest-mode').textContent = m.standalone
        ? `IN-BROWSER PIPELINE · ${m.shared ? 'SHARED RING' : 'TRANSFER'} · ${(m.capacity / 1024).toFixed(0)}K`
        : m.shared
          ? `SHARED RING · ${(m.capacity / 1024).toFixed(0)}K`
          : `TRANSFER · ${(m.capacity / 1024).toFixed(0)}K`;
      if (m.standalone) {
        document.body.classList.add('standalone');
        const link = document.querySelector('.topbar-right .link');
        if (link) link.hidden = true;
        $('rec-state').textContent = 'static build: recordings are in memory only';
      }
      $('ig-caps').textContent = fmtInt(m.capacity);
      break;
    case 'control':
      handleControl(m.msg, m.ringIndex);
      break;
    case 'stats':
      S.workerStats = m.stats;
      break;
    case 'status':
      if (m.state === 'streaming') setText('feed-state', 'STREAM OPEN', 'st-open');
      else if (m.state === 'reconnecting') setText('feed-state', 'RECONNECTING', 'st-wait');
      else if (m.state === 'error') setText('feed-state', 'STREAM ERROR', 'st-bad');
      break;
    case 'recordings':
      S.recordings = m.list || [];
      S.recState = m.state || S.recState;
      renderRecordings();
      renderRecState();
      break;
    case 'chunk': {
      // Fallback mode: this thread owns the ring and does the decode.
      const u8 = new Uint8Array(m.buf);
      const chunk = joinChunks(fallbackRest, u8);
      const used = decodeChunk(chunk, S.ring, (msg) => handleControl(msg, S.ring.writeIndex), S.workerStats);
      fallbackRest = used < chunk.length ? chunk.subarray(used) : new Uint8Array(0);
      break;
    }
    default:
      break;
  }
};
let fallbackRest = new Uint8Array(0);

function setText(id, text, cls) {
  const el = $(id);
  if (!el) return;
  el.textContent = text;
  if (cls !== undefined) el.className = cls;
}

function handleControl(msg, ringIndex) {
  switch (msg.type) {
    case 'hello':
      S.hello = msg;
      S.clientId = msg.clientId;
      if (msg.standalone) {
        document.body.classList.add('standalone');
        setText('counter-scope', 'REAL \u00b7 THIS BROWSER', 'tag tag-observed');
      } else {
        // Only the Node host has an /api/status endpoint.
        const link = document.createElement('a');
        link.className = 'link';
        link.href = './api/status';
        link.target = '_blank';
        link.rel = 'noopener';
        link.textContent = '/api/status';
        document.querySelector('.topbar-right')?.appendChild(link);
      }
      renderTimingSource();
      break;
    case 'metrics':
      S.metrics = msg;
      break;
    case 'record-state':
      S.recState = msg;
      break;
    case 'replay-start':
      S.mode = 'replay';
      S.replayFile = msg.file;
      S.replayStart = ringIndex >= 0 ? ringIndex : S.ring.writeIndex;
      S.replayEnd = -1;
      S.bookReady = false;
      seekTo(S.replayStart, true);
      setText('source-badge', 'REPLAY', 'badge badge-replay');
      $('btn-live').disabled = false;
      renderRecordings();
      break;
    case 'replay-end':
      S.replayEnd = ringIndex >= 0 ? ringIndex : S.ring.writeIndex;
      break;
    case 'note':
      if (msg.level && msg.level !== 'ping') {
        S.serverLog.unshift({ level: msg.level, text: msg.text, wallMs: msg.t || Date.now() });
      }
      break;
    default:
      break;
  }
}

/* ═══════════════════════════ record application ═════════════════════════ */

const rec = {
  kind: 0, side: 0, flags: 0, rawBytes: 0, seq: 0, updateId: 0, exchangeTs: 0,
  localRecvMs: 0, localMonoUs: 0, latencyUs: 0, price: 0, qty: 0, delta: 0, extra: 0,
};
const scratch = Object.assign({}, rec);

function applyBook(rec) {
  switch (rec.kind) {
    case KIND.SNAPSHOT_BEGIN:
      book.inSnapshot = true;
      book.tmpB = new Map();
      book.tmpA = new Map();
      break;
    case KIND.SNAPSHOT_LEVEL: {
      const m = rec.side === SIDE.BID ? book.tmpB : book.tmpA;
      if (m) {
        if (rec.qty === 0) m.delete(rec.price);
        else m.set(rec.price, rec.qty);
      }
      break;
    }
    case KIND.SNAPSHOT_END:
      book.bids = book.tmpB || new Map();
      book.asks = book.tmpA || new Map();
      book.bidPulse.clear();
      book.askPulse.clear();
      book.inSnapshot = false;
      book.levels = book.bids.size + book.asks.size;
      book.lastUpdateId = rec.updateId;
      book.snapshots++;
      S.bookReady = true;
      break;
    case KIND.DEPTH: {
      const m = rec.side === SIDE.BID ? book.bids : book.asks;
      const p = rec.side === SIDE.BID ? book.bidPulse : book.askPulse;
      if (rec.qty === 0) m.delete(rec.price);
      else m.set(rec.price, rec.qty);
      p.set(rec.price, rec.localRecvMs);
      book.levels = book.bids.size + book.asks.size;
      book.lastUpdateId = rec.updateId;
      book.applied++;
      break;
    }
    case KIND.RESET:
      // A reset is followed by a fresh snapshot; the book is rebuilt from it.
      book.bids = new Map();
      book.asks = new Map();
      book.bidPulse.clear();
      book.askPulse.clear();
      book.levels = 0;
      book.lastUpdateId = rec.updateId;
      break;
    default:
      break;
  }
}

/* ═════════════════════════════ firehose buffer ══════════════════════════ */

const FH_RING = 512;
const fh = {
  left: new Array(FH_RING),
  right: new Array(FH_RING),
  kind: new Uint8Array(FH_RING),
  side: new Uint8Array(FH_RING),
  flash: new Float64Array(FH_RING),
  head: 0,
  count: 0,
  charW: 7.2,
  cols: 46,
};

const KIND_COLOR = {
  [KIND.DEPTH]: '#7fb2d9',
  [KIND.TRADE]: '#ffd166',
  [KIND.BOOK_TICKER]: '#4cc9f0',
  [KIND.MARK_PRICE]: '#b388ff',
  [KIND.LIQUIDATION]: '#ff5f56',
  [KIND.SNAPSHOT_BEGIN]: '#4a5a6b',
  [KIND.SNAPSHOT_LEVEL]: '#42505f',
  [KIND.SNAPSHOT_END]: '#4a5a6b',
  [KIND.GAP]: '#ff5f56',
  [KIND.RESET]: '#ffc247',
  [KIND.RAW_MSG]: '#4a5a6b',
};

function firehoseFormat(r) {
  const t = fmtClock(r.localRecvMs);
  const label = pad(KIND_FIREHOSE[r.kind] || '?', 9);
  let id = '';
  let detail = '';

  switch (r.kind) {
    case KIND.DEPTH:
      id = `U=${r.updateId}`;
      detail = `${sideName(r.side)} ${r.price.toFixed(2)}  \u0394${signed(r.delta)}  \u2192 ${r.qty.toFixed(3)}`;
      break;
    case KIND.TRADE:
      id = `#${r.updateId}`;
      detail = `${r.side === SIDE.BID ? 'BUY ' : 'SELL'} ${r.qty.toFixed(4)} BTC @ ${r.price.toFixed(2)}  $${fmtCompact(r.delta)}`;
      break;
    case KIND.BOOK_TICKER:
      id = `U=${r.updateId}`;
      detail = `BID ${r.price.toFixed(2)} x ${r.extra.toFixed(3)}   ASK ${r.qty.toFixed(2)} x ${r.delta.toFixed(3)}`;
      break;
    case KIND.MARK_PRICE:
      id = 'MARK';
      detail = `mark ${r.price.toFixed(2)}  index ${r.qty.toFixed(2)}  funding ${(r.delta * 100).toFixed(4)}%`;
      break;
    case KIND.LIQUIDATION:
      id = 'LIQ';
      detail = `${sideName(r.side)} ${r.qty.toFixed(4)} BTC @ ${r.price.toFixed(2)}`;
      break;
    case KIND.SNAPSHOT_BEGIN:
      id = `lastUpdateId=${r.updateId}`;
      detail = `snapshot begins  bid ${r.price.toFixed(2)}  ask ${r.qty.toFixed(2)}`;
      break;
    case KIND.SNAPSHOT_LEVEL:
      id = `@${r.updateId}`;
      detail = `${sideName(r.side)} ${r.price.toFixed(2)}  ${r.qty.toFixed(3)}`;
      break;
    case KIND.SNAPSHOT_END:
      id = `@${r.updateId}`;
      detail = 'snapshot complete';
      break;
    case KIND.GAP:
      id = 'CHAIN';
      detail = `expected pu=${r.delta}  got pu=${r.extra}  RESYNC`;
      break;
    case KIND.RESET:
      id = `@${r.updateId}`;
      detail = `RESET reason=${r.extra === 3 ? 'replay' : r.extra === 0 ? 'snapshot' : 'resync'}`;
      break;
    default:
      id = '\u2014';
      detail = `${r.rawBytes} B`;
      break;
  }
  return { left: `${t}  ${label} ${id}`, detail };
}

function firehosePush(r) {
  const i = fh.head;
  const f = firehoseFormat(r);
  fh.left[i] = f.left;
  fh.right[i] = f.detail;
  fh.kind[i] = r.kind;
  fh.side[i] = r.side;
  fh.flash[i] = perfNow();
  fh.head = (i + 1) % FH_RING;
  if (fh.count < FH_RING) fh.count++;
}

/* ═══════════════════════════ playback machinery ═════════════════════════ */

const MAX_CONSUME_PER_FRAME = 250000;

function effectiveRate() {
  return S.rate / S.dilation;
}

function consumeUpTo(limitIndex) {
  const cap = S.ring.capacity;
  S.head = S.ring.writeIndex;
  if (S.head - S.cursor > cap) {
    const lost = S.head - cap - S.cursor;
    S.overwritten += lost;
    S.cursor = S.head - cap;
    S.bookReady = false;
  }
  if (limitIndex > S.head) limitIndex = S.head;
  if (limitIndex <= S.cursor) return 0;

  let remaining = limitIndex - S.cursor;
  // Only the last FH_RING events can be on screen; formatting earlier ones
  // would be wasted work. They are still in the ring and still steppable.
  const formatAll = remaining <= FH_RING;
  let skipped = 0;
  let n = 0;
  while (S.cursor < limitIndex && n < MAX_CONSUME_PER_FRAME) {
    S.ring.read(S.cursor, rec);
    applyBook(rec);
    if (rec.kind === KIND.SNAPSHOT_BEGIN) S.lastSnapIndex = S.cursor;
    if (formatAll || remaining - n <= FH_RING) {
      firehosePush(rec);
    } else {
      skipped++;
    }
    S.lastRendered = S.cursor;
    S.cursor++;
    n++;
  }
  if (skipped) S.skippedFormat += skipped;
  S.renderedTotal += n;
  S.renderRateAcc += n;
  return n;
}

function consumeByTime(marketMs) {
  // Consume every record whose local receive time has been reached.
  const head = S.ring.writeIndex;
  let limit = S.cursor;
  const cap = S.cursor + MAX_CONSUME_PER_FRAME;
  while (limit < head && limit < cap) {
    S.ring.read(limit, scratch);
    if (scratch.localRecvMs > marketMs) break;
    limit++;
  }
  return consumeUpTo(limit);
}

function seekTo(index, clearBook) {
  index = clamp(index, 0, S.ring.writeIndex);
  if (clearBook) {
    book.bids = new Map();
    book.asks = new Map();
    book.bidPulse.clear();
    book.askPulse.clear();
    book.levels = 0;
    book.applied = 0;
    S.bookReady = false;
  }
  S.cursor = index;
  S.lastRendered = index - 1;
  S.needsAnchor = true;
  fh.head = 0;
  fh.count = 0;
  rebuildBook(index);
  settleAnchor();
}

/**
 * Reconstruct the book state at `index` by replaying from the nearest snapshot.
 *
 * This is what makes single-stepping and seeking real: the mirrored book at
 * cursor N is by construction the book the server's state machine held after
 * event N, because the server only ever emits a depth event it actually
 * applied, in the order it applied it.
 */
function rebuildBook(index) {
  const ring = S.ring;
  let start = -1;

  // 1. short backward scan (covers the usual case: the playhead moved a little)
  const shortFrom = Math.max(0, index - 4000);
  for (let i = index - 1; i >= shortFrom; i--) {
    ring.read(i, scratch);
    if (scratch.kind === KIND.SNAPSHOT_BEGIN) { start = i; break; }
  }
  // 2. the last snapshot we consumed while playing forward
  if (start < 0 && S.lastSnapIndex >= 0 && S.lastSnapIndex <= index) start = S.lastSnapIndex;
  // 3. long backward scan (seeking into a replay segment)
  if (start < 0) {
    const longFrom = Math.max(0, index - 400000);
    for (let i = index - 1; i >= longFrom; i--) {
      ring.read(i, scratch);
      if (scratch.kind === KIND.SNAPSHOT_BEGIN) { start = i; break; }
    }
  }

  book.bids = new Map();
  book.asks = new Map();
  book.bidPulse.clear();
  book.askPulse.clear();
  book.levels = 0;
  book.applied = 0;
  book.snapshots = 0;
  S.bookReady = false;

  if (start < 0) return false;
  for (let i = start; i < index; i++) {
    ring.read(i, scratch);
    applyBook(scratch);
    if (scratch.kind === KIND.SNAPSHOT_BEGIN) S.lastSnapIndex = i;
  }
  S.bookReady = true;
  return true;
}

function anchorPlayhead() {
  // Anchor to the last consumed record so that changing speed mid-playback
  // continues from where the playhead is, rather than jumping.
  if (S.cursor > 0 && S.cursor <= S.ring.writeIndex) {
    S.ring.read(S.cursor - 1, scratch);
    S.playheadMs = scratch.localRecvMs;
    S.needsAnchor = false;
  } else {
    S.playheadMs = 0;
    S.needsAnchor = true;
  }
}

/**
 * After a seek the playhead must start at the *first* record of the new
 * segment. Live records carry "now" timestamps while a replay segment carries
 * the capture-time timestamps, so anchoring on the preceding record would put
 * the playhead arbitrarily far in the future and the whole recording would be
 * consumed in a single frame.
 */
function settleAnchor() {
  if (!S.needsAnchor) return true;
  if (S.cursor >= S.ring.writeIndex) return false;
  S.ring.read(S.cursor, scratch);
  S.playheadMs = scratch.localRecvMs;
  S.needsAnchor = false;
  return true;
}

function returnToLive() {
  S.mode = 'live';
  S.replayStart = -1;
  S.replayEnd = -1;
  S.replayFile = null;
  S.paused = false;
  // "Return to live" means real time: keeping a 0.01x replay rate would leave
  // the playhead 100x behind the socket while claiming to be live.
  S.rate = 1;
  S.dilation = 1;
  S.stepPending = 0;
  seekTo(S.ring.writeIndex, true);
  setText('source-badge', 'LIVE', 'badge badge-live');
  $('btn-live').disabled = true;
  syncTransportUi();
}

function step(n = 1) {
  S.paused = true;
  S.stepPending += n;
}

/** Consume as many pending single-steps as the ingress can satisfy right now. */
function servicePendingSteps() {
  if (!S.stepPending) return 0;
  const available = S.ring.writeIndex - S.cursor;
  if (available <= 0) return 0;
  const n = Math.min(S.stepPending, available);
  consumeUpTo(S.cursor + n);
  S.stepPending -= n;
  anchorPlayhead();
  return n;
}

/* ════════════════════════════════ renderers ═════════════════════════════ */

/* ---- firehose ---- */

function drawFirehose(now) {
  const su = surface('firehose');
  const { ctx, w, h } = su;
  ctx.fillStyle = '#080c11';
  ctx.fillRect(0, 0, w, h);

  ctx.font = FONT;
  const charW = ctx.measureText('0').width || 7.2;
  fh.charW = charW;
  const lineH = 15;
  const rows = Math.max(1, Math.floor(h / lineH));
  const detailX = fh.cols * charW;
  const maxDetailChars = Math.max(8, Math.floor((w - detailX - 6) / charW));

  if (fh.count === 0) {
    ctx.fillStyle = '#3f4d5c';
    ctx.fillText('waiting for events…', 10, h / 2);
    return;
  }

  const n = Math.min(rows, fh.count);
  const startIdx = (fh.head - n + FH_RING) % FH_RING;
  const newest = S.lastRendered >= 0 && S.cursor > 0;

  for (let k = 0; k < n; k++) {
    const i = (startIdx + k) % FH_RING;
    const y = k * lineH + lineH / 2 + 1;
    const age = now - fh.flash[i];
    // Newest line at the bottom; the whole list slides upward every frame.
    const recency = k / Math.max(1, n - 1);
    let alpha = 0.35 + 0.65 * recency;
    if (newest && k === n - 1) alpha = 1;

    ctx.globalAlpha = alpha;
    ctx.fillStyle = KIND_COLOR[fh.kind[i]] || '#7d8fa1';
    ctx.fillText(fh.left[i], 6, y);

    const side = fh.side[i];
    ctx.fillStyle =
      fh.kind[i] === KIND.DEPTH || fh.kind[i] === KIND.SNAPSHOT_LEVEL
        ? side === SIDE.BID ? '#1fc97a' : '#ff5f56'
        : fh.kind[i] === KIND.TRADE
          ? side === SIDE.BID ? '#8ef0b8' : '#ffa39c'
          : '#c3d0de';
    const right = fh.right[i].slice(0, maxDetailChars);
    ctx.fillText(right, detailX, y);

    // A sub-millisecond flash marks the line that just arrived.
    if (age < 90) {
      ctx.globalAlpha = (1 - age / 90) * 0.16;
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, k * lineH, w, lineH);
    }
  }
  ctx.globalAlpha = 1;

  // Fade the top edge so the scroll reads as continuous motion.
  const grad = ctx.createLinearGradient(0, 0, 0, lineH * 3);
  grad.addColorStop(0, 'rgba(8,12,17,0.95)');
  grad.addColorStop(1, 'rgba(8,12,17,0)');
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, w, lineH * 3);
}

/* ---- sequencer waterfall ---- */

function drawSequencer(now) {
  const su = surface('seq-canvas');
  const { ctx, w, h } = su;
  ctx.fillStyle = '#080c11';
  ctx.fillRect(0, 0, w, h);

  const lineH = 15;
  const topPad = 13;
  const rows = Math.max(3, Math.floor((h - topPad) / lineH));
  const ring = S.ring;
  const behind = Math.max(2, Math.floor(rows * 0.35));
  const from = S.cursor - behind;
  const headIdx = ring.writeIndex - 1;
  let predictedSeq = -1;
  if (headIdx >= 0) {
    ring.read(headIdx, scratch);
    predictedSeq = scratch.seq + 1;
  }
  const statusW = 86;
  const seqW = 62;

  for (let r = 0; r < rows; r++) {
    const i = from + r;
    const y = topPad + r * lineH + lineH / 2 + 1;
    if (i < 0) continue;

    const procIdx = S.cursor - 1;
    let status;
    let color;
    if (i < procIdx) {
      status = 'DONE';
      color = '#3f4d5c';
    } else if (i === procIdx) {
      status = 'PROCESSING';
      color = '#1fc97a';
    } else if (i < S.head) {
      status = 'WAIT';
      color = '#4cc9f0';
    } else {
      status = 'AWAIT';
      color = '#2b3746';
    }

    if (i === procIdx) {
      ctx.fillStyle = 'rgba(31,201,122,0.10)';
      ctx.fillRect(0, topPad + r * lineH, w, lineH);
    }

    ctx.font = FONT;
    ctx.fillStyle = color;
    ctx.fillText(status, 5, y);

    if (i >= ring.writeIndex) {
      // Beyond the ingress head the serial does not exist yet.  The sequencer
      // assigns serials densely (+1 per emitted event), so the next ones are
      // exactly predictable - shown dimmed to make clear they are projected,
      // not received.
      ctx.fillStyle = '#2b3746';
      ctx.fillText(predictedSeq >= 0 ? `#${fmtInt(predictedSeq + (i - headIdx))}` : '#—', statusW + 4, y);
      continue;
    }

    ring.read(i, scratch);
    const isProc = i === procIdx;
    ctx.fillStyle = isProc ? '#e8f2fb' : i < S.cursor ? '#4f6070' : '#7d8fa1';
    ctx.fillText(`#${scratch.seq}`, statusW + 4, y);

    const kindTxt = KIND_FIREHOSE[scratch.kind] || '?';
    ctx.fillStyle = isProc ? KIND_COLOR[scratch.kind] || '#c3d0de' : '#39485a';
    ctx.fillText(kindTxt, statusW + seqW + 4, y);

    let tail = '';
    if (scratch.kind === KIND.DEPTH) {
      tail = `${sideName(scratch.side)} ${scratch.price.toFixed(2)} ${signed(scratch.delta)}`;
    } else if (scratch.kind === KIND.TRADE) {
      tail = `${scratch.qty.toFixed(4)} @ ${scratch.price.toFixed(2)}`;
    } else if (scratch.kind === KIND.BOOK_TICKER) {
      tail = `${scratch.price.toFixed(2)} / ${scratch.qty.toFixed(2)}`;
    } else if (scratch.kind === KIND.SNAPSHOT_LEVEL) {
      tail = `${sideName(scratch.side)} ${scratch.price.toFixed(2)}`;
    } else if (scratch.kind === KIND.SNAPSHOT_BEGIN) {
      tail = 'book snapshot';
    } else if (scratch.kind === KIND.RESET) {
      tail = 'reset';
    }
    ctx.fillStyle = isProc ? '#a9bccd' : '#33404f';
    const tailX = statusW + seqW + 70;
    const tailMax = Math.max(6, Math.floor((w - tailX - 4) / 7.2));
    ctx.fillText(tail.slice(0, tailMax), tailX, y);
  }

  ctx.font = FONT;
  ctx.fillStyle = '#2b3746';
  ctx.fillText('▲ newer · dimmed serials are projected, not yet received', 6, 7);
  void now;
}

/* ---- order book ---- */

function topLevels(map, n, desc) {
  const ks = Array.from(map.keys());
  ks.sort((a, b) => (desc ? b - a : a - b));
  const out = [];
  let cum = 0;
  for (let i = 0; i < Math.min(n, ks.length); i++) {
    const q = map.get(ks[i]);
    cum += q;
    out.push({ price: ks[i], qty: q, cum });
  }
  return out;
}

function drawBook(now) {
  const su = surface('book');
  const { ctx, w, h } = su;
  ctx.fillStyle = '#080c11';
  ctx.fillRect(0, 0, w, h);

  if (!S.bookReady || book.bids.size === 0) {
    ctx.fillStyle = '#3f4d5c';
    ctx.fillText(S.mode === 'replay' ? 'rebuilding book from snapshot…' : 'waiting for book snapshot…', 10, h / 2);
    return;
  }

  const rowTarget = 41; // 20 asks + spread + 20 bids
  const rowH = clamp(Math.floor(h / rowTarget), 9, 17);
  const levels = clamp(Math.floor((h - rowH) / (2 * rowH)), 5, 20);
  const asks = topLevels(book.asks, levels, false).reverse();
  const bids = topLevels(book.bids, levels, true);

  // Cumulative depth grows with distance from the touch, so the deepest level
  // on either side sets the scale for the background bars.
  let cumMax = 1e-9;
  for (const a of asks) if (a.cum > cumMax) cumMax = a.cum;
  for (const b of bids) if (b.cum > cumMax) cumMax = b.cum;

  const priceW = 96;
  const qtyW = 100;
  const fontSize = clamp(rowH - 3, 8, 12);
  ctx.font = `${fontSize}px ui-monospace, "SF Mono", Menlo, Consolas, monospace`;

  const drawRow = (y, lvl, side) => {
    const isBid = side === SIDE.BID;
    const pulseMap = isBid ? book.bidPulse : book.askPulse;
    const last = pulseMap.get(lvl.price);
    const age = last === undefined ? Infinity : now - last;

    // depth bar, grows leftward from the right edge
    const barW = (lvl.cum / cumMax) * (w - priceW - qtyW - 8);
    const barX = w - barW - 4;
    ctx.fillStyle = isBid ? 'rgba(31,201,122,0.15)' : 'rgba(255,95,86,0.15)';
    ctx.fillRect(barX, y + 0.5, barW, rowH - 1);

    // pulse: a level that changed in the last 260 ms glows
    if (age < 260) {
      const a = 0.42 * Math.exp(-age / 110);
      ctx.fillStyle = isBid ? `rgba(31,201,122,${a})` : `rgba(255,95,86,${a})`;
      ctx.fillRect(6, y, w - 12, rowH);
      if (age < 40) {
        ctx.fillStyle = 'rgba(255,255,255,0.20)';
        ctx.fillRect(6, y, w - 12, rowH);
      }
    }

    const ty = y + rowH / 2 + 0.5;
    ctx.fillStyle = isBid ? '#1fc97a' : '#ff5f56';
    ctx.fillText(lvl.price.toFixed(2), 6, ty);

    ctx.fillStyle = isBid ? '#9fe6c2' : '#f4b3ae';
    ctx.fillText(lvl.qty.toFixed(3), priceW, ty);

    ctx.fillStyle = '#54677a';
    ctx.fillText(lvl.cum.toFixed(3), priceW + qtyW, ty);
  };

  let y = 0;
  for (const a of asks) {
    drawRow(y, a, SIDE.ASK);
    y += rowH;
  }

  // spread row
  const bestBid = bids.length ? bids[0].price : NaN;
  const bestAsk = asks.length ? asks[asks.length - 1].price : NaN;
  const spread = bestAsk - bestBid;
  ctx.fillStyle = '#0f1720';
  ctx.fillRect(0, y, w, rowH);
  ctx.fillStyle = '#3f4d5c';
  ctx.fillText('SPREAD', 6, y + rowH / 2 + 0.5);
  ctx.fillStyle = '#ffc247';
  ctx.fillText(Number.isFinite(spread) ? spread.toFixed(2) : '—', priceW, y + rowH / 2 + 0.5);
  ctx.fillStyle = '#54677a';
  ctx.fillText(
    Number.isFinite(spread) ? `${((spread / bestAsk) * 1e4).toFixed(3)} bps` : '',
    priceW + qtyW,
    y + rowH / 2 + 0.5,
  );
  y += rowH;

  for (const b of bids) {
    drawRow(y, b, SIDE.BID);
    y += rowH;
  }
}

/* ---- inter-arrival ---- */

/**
 * Compact axis labels. The server reports the bin edges with full unit text;
 * six of those do not fit side by side, so the axis is relabelled in ms with
 * the unit stated once.
 */
const IA_SHORT = ['<0.1', '0.1\u20130.5', '0.5\u20131', '1\u20135', '5\u201310', '>10 ms'];

function drawHistogram(now, ia) {
  const su = surface('ia-canvas');
  const { ctx, w, h } = su;
  ctx.fillStyle = '#080c11';
  ctx.fillRect(0, 0, w, h);
  if (!ia || !ia.hist) return;

  const hist = ia.hist;
  const total = hist.reduce((a, b) => a + b, 0) || 1;
  const max = Math.max(...hist, 1);
  const padL = 6;
  const padR = 6;
  const labelH = 24;
  const barAreaW = w - padL - padR;
  const barW = barAreaW / hist.length;
  let peak = 0;
  for (let i = 1; i < hist.length; i++) if (hist[i] > hist[peak]) peak = i;

  ctx.font = '9px ui-monospace, Menlo, monospace';
  const labels = IA_SHORT;
  const labelW = Math.max(...labels.map((b) => ctx.measureText(b).width)) + 4;
  const rotate = labelW > barW - 2;
  for (let i = 0; i < hist.length; i++) {
    const v = hist[i];
    const bh = Math.max(1, ((h - labelH - 4) * v) / max);
    const x = padL + i * barW;
    const y = h - labelH - bh;
    const isPeak = i === peak;
    ctx.fillStyle = isPeak ? 'rgba(76,201,240,0.55)' : 'rgba(76,201,240,0.20)';
    ctx.fillRect(x + 2, y, barW - 4, bh);
    ctx.fillStyle = isPeak ? '#4cc9f0' : '#54677a';
    ctx.fillText(`${((v / total) * 100).toFixed(0)}%`, x + 3, y - 6);
    ctx.fillStyle = '#3f4d5c';
    const label = labels[i] || ia.bins[i];
    if (rotate) {
      ctx.save();
      ctx.translate(x + 3, h - labelH + 7);
      ctx.rotate(-Math.PI / 4);
      ctx.fillText(label, 0, 0);
      ctx.restore();
    } else {
      ctx.fillText(label, x + 2, h - labelH + 8);
    }
  }
  void now;
}

function renderIaText(ia) {
  if (!ia) return;
  const last = ia.last || [];
  const parts = last.slice(-24).map((v) => {
    const cls = v >= 1000 ? 'ms' : 'us';
    return `<span class="${cls}">${fmtUs(v)}</span>`;
  });
  $('ia-last').innerHTML = parts.join(' <span style="color:#2b3746">·</span> ') || '—';
  $('ia-p50').textContent = fmtUs(ia.p50);
  $('ia-p95').textContent = fmtUs(ia.p95);
  $('ia-p99').textContent = fmtUs(ia.p99);
  $('ia-min').textContent = fmtUs(ia.min);
  $('ia-max').textContent = fmtUs(ia.max);
  $('ia-samples').textContent = fmtCompact(ia.samples);
}

/* ---- charts ---- */

function drawSeries(canvasId, series, opts) {
  const su = surface(canvasId);
  const { ctx, w, h } = su;
  ctx.fillStyle = '#080c11';
  ctx.fillRect(0, 0, w, h);

  const padT = 14;
  const padB = 12;
  const padL = 6;
  const padR = 6;
  const vals = (series || []).filter((v) => Number.isFinite(v));
  ctx.font = '9px ui-monospace, Menlo, monospace';

  if (vals.length < 2) {
    ctx.fillStyle = '#2b3746';
    ctx.fillText(opts.label, padL, padT);
    return;
  }
  let max;
  let min = 0;
  if (opts.auto) {
    max = Math.max(...vals);
    min = Math.min(...vals);
    const span = max - min || Math.abs(max) || 1;
    max += span * 0.15;
    min -= span * 0.15;
  } else {
    max = Math.max(...vals, opts.min || 0) * 1.15 || 1;
  }
  const innerW = w - padL - padR;
  const innerH = h - padT - padB;
  const n = series.length;
  const xAt = (i) => padL + (innerW * i) / Math.max(1, n - 1);
  const yAt = (v) => padT + innerH - ((v - min) / (max - min)) * innerH;

  ctx.strokeStyle = '#131b25';
  ctx.lineWidth = 1;
  for (let g = 0; g <= 3; g++) {
    const y = padT + (innerH * g) / 3;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(w - padR, y);
    ctx.stroke();
  }

  let started = false;
  ctx.beginPath();
  for (let i = 0; i < n; i++) {
    const v = series[i];
    if (!Number.isFinite(v)) { started = false; continue; }
    const x = xAt(i);
    const y = yAt(v);
    if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
  }
  ctx.strokeStyle = opts.color;
  ctx.lineWidth = 1.4;
  ctx.stroke();

  ctx.lineTo(xAt(n - 1), padT + innerH);
  ctx.lineTo(xAt(0), padT + innerH);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0, padT, 0, padT + innerH);
  grad.addColorStop(0, opts.fill);
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.fillStyle = grad;
  ctx.fill();

  const cur = vals[vals.length - 1];
  ctx.fillStyle = opts.color;
  ctx.fillText(opts.label, padL, 7);
  ctx.textAlign = 'right';
  ctx.fillStyle = '#c3d0de';
  ctx.fillText(opts.fmt(cur), w - padR, 7);
  ctx.textAlign = 'left';
  ctx.fillStyle = '#2b3746';
  ctx.textAlign = 'right';
  ctx.fillText(opts.fmt(max), w - padR, h - 3);
  ctx.fillText(opts.fmt(min), padL, h - 3);
  ctx.textAlign = 'left';
}

/* ---- metrics ---- */

let lastMetricsSeq = -1;

function renderMetrics() {
  const m = S.metrics;
  if (!m) return;
  if (m.seq === lastMetricsSeq) return;
  lastMetricsSeq = m.seq;

  const r = m.rates;
  setNum('m-events-1', fmtCompact(r.events.w1s));
  setNum('m-events-100', fmtCompact(r.events.w100ms));
  setNum('m-events-1b', fmtCompact(r.events.w1s));
  setNum('m-events-10', fmtCompact(r.events.w10s));
  setNum('m-depthlvl', fmtCompact(r.depthLevels.w1s));
  setNum('m-depthmsg', fmtCompact(r.depthMsgs.w1s));
  setNum('m-depthlvl-1', fmtCompact(r.depthLevels.w1s));
  setNum('m-trades', fmt1(r.trades.w1s));
  setNum('m-bookticker', fmtCompact(r.bookTickers.w1s));
  setNum('m-btc', fmt3(r.btc.w1s));
  setNum('m-btc-total', fmt2(m.counters.btc));
  setNum('m-notional', '$' + fmtCompact(r.notional.w1s));
  setNum('m-notional-total', '$' + fmtCompact(m.counters.notional));
  setNum('m-bookchanges', fmtCompact(r.bookChanges.w1s));
  setNum('m-wsbytes', fmtBytes(r.wsBytes.w1s) + '/s');
  setNum('m-wsmsgs', fmtCompact(r.wsMsgs.w1s));
  setNum('m-wsbytes-total', fmtBytes(m.counters.wsBytes));

  const base = m.baseline && m.baseline.rate1s ? m.baseline.rate1s : 0;
  const idx = base > 0 ? r.events.w1s / base : 0;
  const burstEl = $('m-burst');
  burstEl.textContent = (idx ? idx.toFixed(2) : '—') + '×';
  burstEl.style.color = idx >= 2.5 ? '#ff5f56' : idx >= 1.4 ? '#ffc247' : '#c3d0de';
  setNum('m-baseline', fmtCompact(base));

  // event counter
  $('counter-total').textContent = fmtInt(m.counters.events);
  $('counter-session').textContent = fmtHMS(m.sessionMs);
  $('counter-avg').textContent = fmtCompact(m.avgEventsPerSec);
  $('counter-peak').textContent = fmtCompact(m.peak.events1s);
  $('counter-now').textContent = fmtCompact(r.events.w1s);
  $('counter-breakdown').innerHTML =
    `<b>${STANDALONE ? 'counted in this browser since page load' : 'server-side count since server start'}</b> · depth levels ${fmtInt(m.counters.depthLevels)} · ` +
    `trades ${fmtInt(m.counters.trades)} · bookTickers ${fmtInt(m.counters.bookTickers)} · ` +
    `depth msgs ${fmtInt(m.counters.depthMsgs)} · buy ${fmtInt(m.counters.buyTrades)} / sell ${fmtInt(m.counters.sellTrades)}` +
    (m.counters.markers ? ` · ${fmtInt(m.counters.markers)} non-economic trade markers filtered` : '');

  // latency
  const L = m.latency;
  setNum('lat-ex-p50', fmtMs(L.exchangeToLocal.p50));
  setNum('lat-ex-p95', fmtMs(L.exchangeToLocal.p95));
  setNum('lat-ex-p99', fmtMs(L.exchangeToLocal.p99));
  setNum('lat-ex-max', fmtMs(L.exchangeToLocal.max));
  setNum('lat-pr-p50', fmtUs(L.process.p50));
  setNum('lat-pr-p95', fmtUs(L.process.p95));
  setNum('lat-pr-p99', fmtUs(L.process.p99));
  setNum('lat-pr-max', fmtUs(L.process.max));

  const off = m.clock ? m.clock.offsetMs : null;
  const rtt = m.clock ? m.clock.rttMs : null;
  const unc = rtt != null ? rtt / 2 : null;
  setNum('clock-offset', off === null ? '—' : `${off > 0 ? '+' : ''}${off.toFixed(0)} ±${unc !== null ? unc.toFixed(0) : '?'} ms`);
  $('lat-offset').textContent = off === null ? '—' : `${off > 0 ? '+' : ''}${off.toFixed(1)} ms`;
  $('lat-offset-unc').textContent = unc === null ? '±?' : `±${unc.toFixed(0)} ms (½ RTT)`;
  const corr = off !== null && Number.isFinite(L.exchangeToLocal.p50) ? L.exchangeToLocal.p50 - off : null;
  $('lat-corrected').textContent = corr === null ? '—' : fmtMs(corr);

  renderTimingSource();
  const ll = m.loopLag;
  if (ll) {
    setNum('lag-p50', `${ll.p50Ms.toFixed(2)} ms`);
    setNum('lag-p99', `${ll.p99Ms.toFixed(2)} ms`);
    setNum('lag-max', `${ll.maxMs.toFixed(2)} ms`);
    const el = $('lag-verdict');
    if (el) {
      const worst = Math.max(ll.p99Ms, ll.maxMs);
      const affected = L.process.p99 != null && worst > L.process.p99 / 1000;
      el.textContent = affected
        ? `loop lag p99 (${ll.p99Ms.toFixed(2)} ms) exceeds processing p99 - latency numbers are contaminated`
        : 'loop lag is well below measured processing latency: the latency figures are the work, not queueing';
      el.className = affected ? 'bad' : 'ok';
    }
  }

  // book meta
  const bk = m.book || {};
  setNum('book-best-bid', Number.isFinite(bk.bid) ? bk.bid.toFixed(2) : '—');
  setNum('book-best-ask', Number.isFinite(bk.ask) ? bk.ask.toFixed(2) : '—');
  setNum('book-spread', Number.isFinite(bk.spread) ? bk.spread.toFixed(2) : '—');
  setNum('book-spread-bps', Number.isFinite(bk.spread) && bk.mid ? `${((bk.spread / bk.mid) * 1e4).toFixed(3)} bps` : '—');
  setNum('book-mid', Number.isFinite(bk.mid) ? bk.mid.toFixed(2) : '—');
  setNum('book-levels', fmtInt(bk.levels));
  setNum('book-applied', fmtInt(bk.applied));
  setNum('book-gaps', fmtInt(bk.gaps));
  const syncEl = $('book-sync');
  syncEl.textContent = bk.bookSynced ? 'IN SYNC' : 'RESYNCING';
  syncEl.className = bk.bookSynced ? 'ok' : 'bad';

  // integrity
  const ig = m.integrity || {};
  setNum('ig-gaps', fmtInt(ig.gaps));
  setNum('ig-resyncs', fmtInt(ig.resyncs));
  setNum('ig-stale', fmtInt(ig.discardedStale));
  setNum('ig-markers', fmtInt(ig.markersFiltered));
  setNum('ig-syncmiss', fmtInt(ig.syncMisses));
  setNum('ig-levels', fmtInt((ig.bookBids || 0) + (ig.bookAsks || 0)));
  setNum('ig-snaptt', ig.snapshotRttMs != null ? `${ig.snapshotRttMs.toFixed(0)} ms` : '—');

  const feedEl = $('feed-state');
  if (m.feed) {
    const st = m.feed.state;
    feedEl.textContent = st === 'open' ? 'OPEN' : st.toUpperCase();
    feedEl.className = st === 'open' ? 'st-open' : st === 'reconnecting' ? 'st-wait' : 'st-bad';
  }

  // server log
  if (m.log && m.log.length) {
    S.serverLog = m.log;
  }
  $('server-log').innerHTML = (S.serverLog || [])
    .slice(0, 8)
    .map((l) => `<div class="${l.level}">${escapeHtml(l.text).slice(0, 190)}</div>`)
    .join('');

  // burst list
  renderBursts(m);

  // inter-arrival + histograms
  const ia = m.interArrival ? m.interArrival[S.iaMode] : null;
  renderIaText(ia);
  drawHistogram(0, ia);
  $('ia-note').innerHTML =
    (S.iaMode === 'message'
      ? 'One sample per WebSocket message from Binance - the true arrival process at this machine. Percentiles are exact over the last 8192 samples (sorted, not bucketed).'
      : 'One sample per normalized event, i.e. after depth level expansion. Sub-100 µs gaps are the expansion, not the network.') +
    ' <b>Arrival is bursty:</b> messages arrive in tight clusters and the tail is long, so a 100&nbsp;ms rate window can legitimately read zero. Windows are 50&nbsp;ms-bucket quantised.';

  // charts
  setNum('chart-cursor', `${m.series.events.length} × 1 s samples`);
  drawSeries('chart-events', m.series.events, {
    color: '#4cc9f0', fill: 'rgba(76,201,240,0.22)', label: 'EVENTS / SEC', fmt: fmtCompact, min: 1,
  });
  drawSeries('chart-latency', m.series.latencyP50, {
    color: '#ffc247', fill: 'rgba(255,194,71,0.18)', label: 'PROCESS P50', fmt: fmtUs, auto: true,
  });
  drawSeries('chart-lag', m.loopLag ? m.loopLag.series : [], {
    color: '#b388ff', fill: 'rgba(179,136,255,0.18)', label: 'SERVER LOOP LAG ms', fmt: (v) => v.toFixed(1), auto: true,
  });
  drawSeries('chart-spread', m.series.spread, {
    color: '#1fc97a', fill: 'rgba(31,201,122,0.18)', label: 'SPREAD $', fmt: (v) => v.toFixed(2), min: 0.01,
  });

  if (m.recorder) {
    S.recState = m.recorder;
    renderRecState();
  }

  renderSimModel(m);

  updateUptime(m);
}

/**
 * Which clock the timestamps came from, and how fine it actually is.
 *
 * In the static build the pipeline timestamps events with the browser's
 * `performance.now()`, which browsers deliberately coarsen. If that resolution
 * is not far below the histogram's smallest bin, the sub-100 us bins are
 * measuring the timer rather than the market, and the UI has to say so.
 */
function renderTimingSource() {
  const h = S.hello;
  const el = $('timing-source');
  if (!el) return;
  if (!h) {
    el.textContent = '—';
    return;
  }
  const res = h.timingResolutionUs;
  const resTxt = res == null ? 'unknown' : res < 1 ? `${(res * 1000).toFixed(0)} ns` : `${res.toFixed(res < 10 ? 1 : 0)} \u00b5s`;
  el.innerHTML = `${escapeHtml(h.timingSource || 'unknown')} &middot; resolution ${resTxt}`;
  el.className = res != null && res > 100 ? 'bad' : res != null && res > 10 ? 'warn' : 'ok';

  const coarse = res != null && res > 10;
  const note = $('ia-resolution-warn');
  if (note) {
    note.hidden = !coarse;
    if (coarse) {
      note.textContent =
        `Timer resolution here is ~${res.toFixed(0)} \u00b5s, so gaps below that are quantised: ` +
        'treat the smallest histogram bins as an upper bound on resolution rather than a measurement.';
    }
  }
}

function updateUptime(m) {
  if (!m.serverWallMs || !m.sessionMs) return;
  $('server-uptime').textContent = fmtHMS(m.sessionMs);
  const skew = Date.now() - m.serverWallMs;
  $('exchange-clock').textContent = `${fmtClock(m.serverWallMs)} (${skew >= 0 ? '+' : ''}${skew} ms rtt)`;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

function setNum(id, v) {
  const el = $(id);
  if (el) el.textContent = v;
}

function renderBursts(m) {
  const active = $('burst-active');
  if (m.activeBurst) {
    const b = m.activeBurst;
    active.hidden = false;
    active.innerHTML = `
      <div class="burst-title">● BURST DETECTED — ONGOING</div>
      <div class="burst-rate">${fmtCompact(b.peakRate)} events/sec</div>
      <div class="burst-detail">
        started ${fmtClock(b.startWallMs)} · baseline ${fmtCompact(b.baseline)}/s ·
        <b>+${(((b.peakRate - b.baseline) / b.baseline) * 100).toFixed(0)}%</b><br>
        depth ${fmtCompact(b.peakDepthRate)}/s · trades ${fmt1(b.peakTradeRate)}/s ·
        price ${b.priceStart ? b.priceStart.toFixed(2) : '—'} → ${b.priceEnd ? b.priceEnd.toFixed(2) : '—'}
      </div>`;
  } else {
    active.hidden = true;
  }

  const list = $('burst-list');
  const bursts = m.bursts || [];
  if (!bursts.length) {
    list.innerHTML = '<div class="empty">no burst detected yet — a burst is recorded when the observed 1 s event rate exceeds 2.5× the trailing median and clears an absolute floor of 200 events/s.</div>';
    return;
  }
  list.innerHTML = bursts
    .slice(0, 10)
    .map(
      (b) => `
      <div class="burst-item">
        <div class="bi-head">
          <span>BURST ${fmtClock(b.startWallMs)}</span>
          <span>${b.durationMs != null ? (b.durationMs / 1000).toFixed(1) + ' s' : 'ongoing'}</span>
        </div>
        <div class="bi-rate">${fmtCompact(b.peakRate)} events/sec <span style="color:#5e7183">+${b.baseline ? (((b.peakRate - b.baseline) / b.baseline) * 100).toFixed(0) : '—'}% baseline</span></div>
        <div class="bi-detail">
          price ${b.priceMovePct != null ? (b.priceMovePct >= 0 ? '+' : '') + b.priceMovePct.toFixed(3) + '%' : '—'}
          (${b.priceStart ? b.priceStart.toFixed(2) : '—'} → ${b.priceEnd ? b.priceEnd.toFixed(2) : '—'}) ·
          ${fmt3(b.btc)} BTC · $${fmtCompact(b.notional ?? 0)} notional ·
          ${fmtInt(b.trades)} trades · depth ${fmtCompact(b.peakDepthRate)}/s · book lvl changes ${fmtCompact(b.levels)}
        </div>
      </div>`,
    )
    .join('');
}

/* ══════════════════════════ view / status render ════════════════════════ */

const panel = {
  seqIngress: $('seq-ingress'),
  seqCursor: $('seq-cursor'),
  queue: $('seq-queue'),
  queueFill: $('seq-queue-fill'),
  procEv: $('seq-proc-ev'),
  procDetail: $('seq-proc-detail'),
  nextEv: $('seq-next-ev'),
  bookState: $('seq-book'),
  emitted: $('seq-emitted'),
  engine: $('seq-engine'),
  viewRate: $('view-rate'),
  viewClock: $('view-clock'),
  viewLag: $('view-lag'),
  viewQueue: $('view-queue'),
  fhDrawn: $('fh-drawn'),
  fhReceived: $('fh-received'),
  fhPerSec: $('fh-persec'),
  fhSkipped: $('fh-skipped'),
  fhSkippedWrap: $('fh-skipped-wrap'),
  ringUsage: $('ig-records'),
  overwritten: $('ig-overwritten'),
  bookSeq: $('book-seq'),
};

function renderViewStatus() {
  const head = S.head;
  const queue = Math.max(0, head - S.cursor);
  panel.seqIngress.textContent = fmtInt(head);
  panel.seqCursor.textContent = fmtInt(S.cursor);
  panel.queue.textContent = fmtInt(queue);
  panel.viewQueue.textContent = fmtCompact(queue);
  panel.queueFill.style.width = Math.min(100, (queue / 5000) * 100).toFixed(1) + '%';

  panel.fhDrawn.textContent = fmtInt(S.renderedTotal);
  panel.fhReceived.textContent = fmtInt(head);
  panel.fhPerSec.textContent = fmtCompact(S.renderRate);
  if (S.skippedFormat > 0) {
    panel.fhSkippedWrap.hidden = false;
    panel.fhSkipped.textContent = fmtCompact(S.skippedFormat);
  }

  panel.ringUsage.textContent = fmtInt(head);
  panel.overwritten.textContent = fmtInt(S.overwritten);
  panel.bookSeq.textContent = '#' + fmtInt(S.cursor);
  $('ig-badframes').textContent = fmtInt(S.workerStats.badFrames || 0);

  const eff = effectiveRate();
  panel.viewRate.textContent =
    eff === 1 ? '1×  (realtime)' : eff === 0 ? '0×  (paused)' : `${eff >= 0.01 ? eff.toFixed(eff < 1 ? 3 : 2) : eff.toExponential(1)}×  (${(1 / eff).toFixed(0)}× slower)`;

  // the market clock value the playhead currently sits on
  if (S.cursor > 0) {
    S.ring.read(S.cursor - 1, scratch);
    const eventMs = scratch.localRecvMs;
    // MARKET TIME is the playhead's own position on the captured timeline, not
    // the timestamp of the last event consumed. Under heavy dilation the
    // playhead moves continuously while events are still far apart, and without
    // this distinction the clock would look frozen at 1000x slower.
    const playMs = !S.needsAnchor && S.playheadMs > 0 ? Math.min(S.playheadMs, eventMs + 1e9) : eventMs;
    panel.viewClock.textContent = fmtClock(playMs);
    let lagMs = NaN;
    if (head > 0) {
      S.ring.read(head - 1, scratch);
      lagMs = scratch.localRecvMs - playMs;
    }
    panel.viewLag.textContent =
      S.mode === 'live' && eff >= 1 ? 'live edge' : Number.isFinite(lagMs) ? fmtMs(lagMs) : '—';
    const evEl = $('view-event');
    if (evEl) evEl.textContent = fmtClock(eventMs);
  } else {
    panel.viewClock.textContent = '—';
    panel.viewLag.textContent = '—';
  }

  panel.viewQueue.textContent = fmtCompact(queue);
}

function renderProcessingPanels() {
  const idx = S.cursor - 1;
  if (idx < 0 || idx >= S.ring.writeIndex) {
    panel.procEv.textContent = '—';
    panel.procDetail.textContent = 'waiting for events…';
    panel.nextEv.textContent = '—';
    panel.bookState.textContent = '—';
    panel.emitted.textContent = '—';
    panel.engine.textContent = '—';
    return;
  }
  S.ring.read(idx, scratch);
  const procSeq = scratch.seq;
  panel.procEv.textContent = `#${fmtInt(scratch.seq)}  ${KIND_FIREHOSE[scratch.kind] || '?'}`;

  let detail = '';
  if (scratch.kind === KIND.DEPTH) {
    detail = `${sideName(scratch.side)} ${scratch.price.toFixed(2)}  \u0394${signed(scratch.delta)}  → ${scratch.qty.toFixed(3)} BTC   U=${scratch.updateId}`;
    panel.bookState.textContent = `${sideName(scratch.side)} ${scratch.price.toFixed(2)} → ${scratch.qty.toFixed(3)}`;
  } else if (scratch.kind === KIND.TRADE) {
    detail = `${scratch.qty.toFixed(4)} BTC @ ${scratch.price.toFixed(2)}  (${scratch.side === SIDE.BID ? 'buy' : 'sell'} aggressor)`;
    panel.bookState.textContent = 'no book mutation (trade)';
  } else if (scratch.kind === KIND.BOOK_TICKER) {
    detail = `bid ${scratch.price.toFixed(2)} x ${scratch.extra.toFixed(3)}  ask ${scratch.qty.toFixed(2)} x ${scratch.delta.toFixed(3)}`;
    panel.bookState.textContent = 'top-of-book tick';
  } else if (scratch.kind === KIND.SNAPSHOT_LEVEL) {
    detail = `${sideName(scratch.side)} ${scratch.price.toFixed(2)} = ${scratch.qty.toFixed(3)}`;
    panel.bookState.textContent = 'snapshot level';
  } else {
    detail = `${KIND_FIREHOSE[scratch.kind] || '?'}  ${scratch.rawBytes} B`;
    panel.bookState.textContent = '—';
  }
  panel.procDetail.textContent =
    S.paused && S.stepPending > 0 ? `${detail}   ·   waiting for ingress (${S.stepPending} step${S.stepPending > 1 ? 's' : ''} pending)` : detail;

  if (S.cursor < S.ring.writeIndex) {
    S.ring.read(S.cursor, scratch);
    panel.nextEv.textContent = `#${fmtInt(scratch.seq)}  ${KIND_FIREHOSE[scratch.kind] || '?'}`;
  } else {
    panel.nextEv.textContent = `#${fmtInt(S.cursor)}  (awaiting network)`;
  }

  panel.emitted.textContent = fmtInt(S.ring.writeIndex);
  panel.engine.textContent = `#${fmtInt(procSeq)}`;
}

/**
 * Optional, explicitly labelled model of what a matching engine *might* be
 * doing. Off by default. Binance publishes none of these quantities, so they
 * are never mixed into the observed panels.
 */
function renderSimModel(m) {
  const box = $('sim-detail');
  if (!S.sim || !m) {
    box.hidden = true;
    return;
  }
  const arrival = m.rates.events.w1s || 0;
  const levelsPerMsg = m.rates.depthMsgs.w1s > 0 ? m.rates.depthLevels.w1s / m.rates.depthMsgs.w1s : 0;
  const serviceUs = 0.9 + 0.11 * levelsPerMsg;
  const capacity = serviceUs > 0 ? 1e6 / serviceUs : 0;
  const rho = capacity > 0 ? arrival / capacity : 0;
  const queueDepth = rho < 1 ? (rho * rho) / (1 - rho) : (arrival - capacity) * 0.05;
  const matchLatUs = serviceUs * (1 + queueDepth);
  box.hidden = false;
  box.innerHTML = `
    <b>SIMULATED — NOT BINANCE TELEMETRY.</b>
    Model: single-threaded engine, <code>service = 0.90 µs + 0.11 µs × levels_per_message</code>,
    <code>capacity = 10⁶ / service</code>, <code>ρ = arrival / capacity</code>,
    <code>queue = ρ²/(1−ρ)</code>, <code>matching latency = service × (1 + queue)</code>.<br>
    inputs: arrival ${fmtCompact(arrival)} ev/s, ${levelsPerMsg.toFixed(1)} levels/msg<br>
    service ${serviceUs.toFixed(2)} µs · capacity ${fmtCompact(capacity)} ev/s ·
    ρ ${rho.toFixed(3)} · modeled queue ${queueDepth.toFixed(2)} · modeled match latency ${fmtUs(matchLatUs)}
  `;
}

/* ════════════════════════════ transport UI ══════════════════════════════ */

function syncTransportUi() {
  $('btn-pause').textContent = S.paused ? '▶ RESUME' : '❚❚ PAUSE';
  $('btn-pause').classList.toggle('active', S.paused);
  // STEP is always available: pressing it while playing is a request to pause
  // and then advance exactly one event, which is what a user means by it.
  $('btn-step').disabled = false;
  $('btn-step').title = S.paused
    ? 'advance the sequencer by exactly one event'
    : 'pause, then advance by exactly one event';
  document.querySelectorAll('.btn.rate').forEach((b) => {
    b.classList.toggle('active', Number(b.dataset.rate) === S.rate);
  });
  document.querySelectorAll('.btn.dil').forEach((b) => {
    b.classList.toggle('active', Number(b.dataset.dil) === S.dilation);
  });
}

function renderRecState() {
  const st = S.recState;
  const btn = $('btn-record');
  const el = $('rec-state');
  if (!st) return;
  if (st.recording) {
    btn.classList.add('recording');
    btn.textContent = '■ STOP';
    const left = st.endsAt ? Math.max(0, (st.endsAt - Date.now()) / 1000) : null;
    el.textContent = `${st.file} · ${(st.bytes / 1024).toFixed(0)} KiB${left !== null ? ` · ${left.toFixed(0)}s left` : ''}`;
    el.classList.add('on');
  } else {
    btn.classList.remove('recording');
    btn.textContent = '● REC 30 s';
    el.textContent = st.lastFile ? `last: ${st.lastFile.name} (${(st.lastFile.bytes / 1024).toFixed(0)} KiB)` : 'idle';
    el.classList.remove('on');
  }
}

async function loadRecordings() {
  if (STANDALONE) {
    worker.postMessage({ type: 'list-recordings' });
    return;
  }
  try {
    const r = await fetch('./api/recordings', { cache: 'no-store' });
    const j = await r.json();
    S.recordings = j.recordings || [];
    S.recState = j.state || S.recState;
    renderRecordings();
    renderRecState();
  } catch {
    /* ignore */
  }
}

function renderRecordings() {
  const sel = $('replay-select');
  const cur = sel.value;
  const items = S.recordings.filter((r) => !r.corrupt);
  if (!items.length) {
    sel.innerHTML = '<option value="">no recordings</option>';
    return;
  }
  sel.innerHTML = items
    .map((r) => {
      const when = r.startedAt ? new Date(r.startedAt).toISOString().slice(11, 19) : '?';
      return `<option value="${r.name}">${r.name} · ${fmtCompact(r.events || 0)} ev · ${(r.bytes / 1024).toFixed(0)} KiB · ${when}Z</option>`;
    })
    .join('');
  if (cur && items.some((r) => r.name === cur)) sel.value = cur;
}

/* ══════════════════════════════ controls ════════════════════════════════ */

$('btn-pause').onclick = () => {
  S.paused = !S.paused;
  anchorPlayhead();
  syncTransportUi();
};

$('btn-step').onclick = () => {
  S.paused = true;
  syncTransportUi();
  step(1);
  servicePendingSteps();
  renderProcessingPanels();
  renderViewStatus();
};

document.querySelectorAll('.btn.rate').forEach((b) => {
  b.onclick = () => {
    S.rate = Number(b.dataset.rate);
    anchorPlayhead();
    syncTransportUi();
  };
});

document.querySelectorAll('.btn.dil').forEach((b) => {
  b.onclick = () => {
    S.dilation = Number(b.dataset.dil);
    anchorPlayhead();
    syncTransportUi();
  };
});

document.querySelectorAll('.mini-btn').forEach((b) => {
  b.onclick = () => {
    S.iaMode = b.dataset.ia;
    document.querySelectorAll('.mini-btn').forEach((x) => x.classList.toggle('active', x === b));
    lastMetricsSeq = -1;
    renderMetrics();
  };
});

$('sim-toggle').onchange = (e) => {
  S.sim = e.target.checked;
  renderSimModel(S.metrics);
};

$('btn-record').onclick = async () => {
  const recording = S.recState && S.recState.recording;
  if (STANDALONE) {
    worker.postMessage(recording ? { type: 'record-stop' } : { type: 'record', seconds: 30 });
    return;
  }
  await fetch('./api/record', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(recording ? { action: 'stop' } : { action: 'start', seconds: 30 }),
  });
  await loadRecordings();
};

$('btn-replay').onclick = async () => {
  const file = $('replay-select').value;
  if (!file) return;
  if (STANDALONE) {
    worker.postMessage({ type: 'replay', file });
    return;
  }
  if (!S.clientId) {
    alert('stream not connected yet');
    return;
  }
  const res = await fetch('./api/replay', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ action: 'start', file, clientId: S.clientId }),
  });
  if (!res.ok) {
    const j = await res.json().catch(() => ({}));
    alert('replay failed: ' + (j.error || res.status));
  }
};

$('btn-live').onclick = () => returnToLive();

window.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'SELECT' || e.target.tagName === 'INPUT') return;
  if (e.code === 'Space') { e.preventDefault(); $('btn-pause').click(); }
  else if (e.code === 'ArrowRight') { e.preventDefault(); $('btn-step').click(); }
  else if (e.key === '1') document.querySelector('.btn.rate[data-rate="1"]').click();
  else if (e.key === '2') document.querySelector('.btn.rate[data-rate="0.1"]').click();
  else if (e.key === '3') document.querySelector('.btn.rate[data-rate="0.01"]').click();
});

/* ═══════════════════════════════ main loop ══════════════════════════════ */

function frame(now) {
  if (S.ring) {
    S.head = S.ring.writeIndex;
    const dt = S.lastFrameMs ? now - S.lastFrameMs : 16;
    S.lastFrameMs = now;

    if (S.paused) {
      if (servicePendingSteps() > 0) {
        renderProcessingPanels();
        renderViewStatus();
      }
    } else if (settleAnchor()) {
      const eff = effectiveRate();
      if (S.mode === 'live' && eff >= 1) {
        // Real time: stay pinned to the newest record, never lag behind.
        consumeUpTo(S.head);
      } else {
        S.playheadMs += dt * eff;
        consumeByTime(S.playheadMs);
      }
    }

    // Stop cleanly at the end of a replay segment rather than sliding into
    // live data that the client deliberately did not receive.
    if (S.mode === 'replay' && S.replayEnd >= 0 && S.cursor >= S.replayEnd && !S.paused) {
      S.paused = true;
      syncTransportUi();
    }

    if (now - S.renderRateWall >= 1000) {
      S.renderRate = (S.renderRateAcc * 1000) / (now - S.renderRateWall);
      S.renderRateAcc = 0;
      S.renderRateWall = now;
    }

    drawFirehose(now);
    drawSequencer(now);
    drawBook(now);
    renderProcessingPanels();
    renderViewStatus();
  }
  renderMetrics();
  requestAnimationFrame(frame);
}

/* ════════════════════════════════ boot ══════════════════════════════════ */

/**
 * Read/write handle for the console and for automated verification.
 * The observer is an instrument; being able to poke its playhead directly is
 * part of the tool, not a debug leftover.
 */
window.__observer = {
  state: S,
  book,
  step,
  seekTo,
  returnToLive,
  effectiveRate,
  ringInfo: () => ({ head: S.ring.writeIndex, capacity: S.ring.capacity, shared: S.shared, cursor: S.cursor }),
};

if (STANDALONE) enableCrossOriginIsolation();

renderRecordings();
loadRecordings();
setInterval(loadRecordings, 5000);
syncTransportUi();
requestAnimationFrame(frame);

if (!STANDALONE) {
  fetch('./api/status', { cache: 'no-store' })
    .then((r) => r.json())
    .then((j) => {
      if (j && j.feed) {
        setText('feed-state', j.feed.state === 'open' ? 'OPEN' : String(j.feed.state).toUpperCase(),
          j.feed.state === 'open' ? 'st-open' : 'st-wait');
      }
    })
    .catch(() => {});
}
