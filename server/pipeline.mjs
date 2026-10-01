/**
 * The local sequencer pipeline.
 *
 *     Binance WebSocket
 *        -> ingress buffer (until the book is provably in sync)
 *        -> normalizer          (raw JSON -> typed records)
 *        -> sequencer           (monotonic local event numbering)
 *        -> order book machine  (pu-chain validated L2 state)
 *        -> metrics aggregator
 *        -> frame writer -> clients + recorder
 *
 * INVARIANT
 * ---------
 * Every DEPTH record that leaves this pipeline was applied to the
 * authoritative book *before* it was emitted, and records leave in exactly the
 * order they were applied.  The browser mirrors the same transitions, so
 * "event N -> book state N" holds for every N, during start-up, during a
 * resynchronization, and during replay.  This is what makes single-stepping
 * meaningful rather than decorative.
 *
 * Nothing is emitted while the book is out of sync.  Incoming messages are
 * buffered and replayed in arrival order once a fresh REST snapshot has been
 * installed, so a resync never reorders history or leaks a partial book.
 */

import { EventEmitter } from 'node:events';
import { BinanceFeed, DEFAULT_STREAMS } from './feed.mjs';
import { OrderBook } from './orderbook.mjs';
import { Metrics } from './metrics.mjs';
import { monoUs, wallMsHiRes, resyncWallAnchor, probeClockOffset, clockOffset } from './clock.mjs';
import {
  KIND,
  SIDE,
  FLAG,
  RESET_REASON,
  FrameWriter,
  RECORD_BYTES,
} from '../shared/protocol.js';

const SNAPSHOT_DEPTH = 1000;
const MAX_INGRESS = 20000;

export class Pipeline extends EventEmitter {
  constructor({ symbol = 'BTCUSDT', streams = DEFAULT_STREAMS, depthLimit = SNAPSHOT_DEPTH } = {}) {
    super();
    this.symbol = symbol;
    this.depthLimit = depthLimit;
    this.book = new OrderBook();
    this.metrics = new Metrics();
    this.feed = new BinanceFeed({ streams, symbol });

    /** Monotonic local event serial. This is the sequencer's event number. */
    this.seq = 0;
    /** Ready to emit: the book has been snapshot-synced at least once. */
    this.armed = false;
    /** Raw messages held back while not armed. */
    this.ingress = [];
    this.snapshotPending = false;
    this.snapshotAttempts = 0;
    this.lastSnapshotError = null;
    this.lastSnapshotWallMs = 0;
    this.lastSnapshotRttMs = null;
    this.snapshotResyncs = 0;

    this.writer = new FrameWriter(8192);
    this._flushScheduled = false;
    this.framesSent = 0;
    this.recordsSent = 0;

    this.depthDiscards = 0;
    this.unknownMessages = 0;

    /**
     * Top-of-book history, one entry per applied depth message.
     *
     * Lets a client prove its mirrored book agrees with the authoritative one
     * at a shared update id. Comparing only against the *current* book is racy:
     * the server is always a few messages ahead of any client, so by the time
     * the client asks, its newest update id is already in the past. This ring
     * gives the overlap.
     */
    this.bookHistory = [];
    this.bookHistoryLimit = 256;

    /**
     * Event-loop lag.
     *
     * The single most important honesty metric in the whole system: it bounds
     * how much any "local processing latency" number can be trusted. If the
     * loop stalls for 180 ms, then every event processed after that stall
     * carries 180 ms of waiting that has nothing to do with the work itself.
     * Measured as the overshoot of a 50 ms timer, which is the same cadence as
     * the metric tick.
     */
    this.loopLag = { lastMs: 0, maxMs: 0, p50Ms: 0, p99Ms: 0, samples: 0, series: [], _ring: [] };
    /** Snapshots that arrived already behind the stream and were refetched. */
    this.syncMisses = 0;
    this.candidateSnapshotId = 0;
    this.replayedFrames = 0;

    this.feed.on('message', (data, recvMonoUs, recvWallMs, bytes) => {
      this.onMessage(data, recvMonoUs, recvWallMs, bytes);
    });
    this.feed.on('state', (s) => this.emit('feed-state', s));
    this.feed.on('error', (e) => this.emit('feed-error', e));
    this.feed.on('disconnect', () => {
      // A reconnect will deliver a fresh stream; the book must not be trusted
      // across the gap until it has been re-synced against a new snapshot.
      this.disarm('reconnect');
    });
    this.feed.on('open', () => {
      this.emit('note', { level: 'info', text: 'websocket open, requesting depth snapshot' });
      this.fetchSnapshot('startup');
    });
  }

  start() {
    this.feed.start();
    this.metrics.sessionStartWallMs = Date.now();
    this._ticks = setInterval(() => this.tick(), 50);
    this._lagTimer = setInterval(() => this.sampleLoopLag(), 50);
    this._lagExpected = monoUs() + 50000;
    this._clockTimer = setInterval(() => {
      resyncWallAnchor();
      probeClockOffset();
    }, 30000);
    probeClockOffset();
    return this;
  }

  stop() {
    clearInterval(this._ticks);
    clearInterval(this._clockTimer);
    clearInterval(this._lagTimer);
    this.feed.stop();
  }

  /** Timer overshoot: how late the loop actually ran a 50 ms scheduled task. */
  sampleLoopLag() {
    const nowUs = monoUs();
    const lagUs = nowUs - this._lagExpected;
    this._lagExpected = nowUs + 50000;
    const lag = this.loopLag;
    if (lagUs < 0 || lagUs > 1e7) return; // timer reset or a deliberate stall
    lag.lastMs = lagUs / 1000;
    if (lag.lastMs > lag.maxMs) lag.maxMs = lag.lastMs;
    lag.samples++;
    lag._ring.push(lagUs);
    if (lag._ring.length > 2048) lag._ring.shift();
    const sorted = [...lag._ring].sort((a, b) => a - b);
    lag.p50Ms = sorted[Math.floor(sorted.length * 0.5)] / 1000;
    lag.p99Ms = sorted[Math.floor(sorted.length * 0.99)] / 1000;
    lag.series.push(Number((lagUs / 1000).toFixed(3)));
    if (lag.series.length > 120) lag.series.shift();
  }

  /* ------------------------------------------------------------ ingress */

  onMessage(data, recvMonoUs, recvWallMs, bytes) {
    if (!this.armed) {
      if (this.ingress.length < MAX_INGRESS) {
        this.ingress.push({ data, recvMonoUs, recvWallMs, bytes });
      } else {
        this.ingress.shift();
        this.ingressOverflow = (this.ingressOverflow || 0) + 1;
      }
      return;
    }
    this.process(data, recvMonoUs, recvWallMs, bytes);
  }

  /** Normalize one raw WebSocket frame into records. */
  process(data, recvMonoUs, recvWallMs, bytes) {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      this.unknownMessages++;
      return;
    }
    const p = msg.data !== undefined ? msg.data : msg;
    if (!p || typeof p !== 'object') {
      this.unknownMessages++;
      return;
    }

    this.metrics.onRawMessage(bytes, recvMonoUs, recvWallMs);

    switch (p.e) {
      case 'depthUpdate':
        this.handleDepth(p, recvMonoUs, recvWallMs, bytes);
        break;
      case 'trade':
      case 'aggTrade':
        this.handleTrade(p, recvMonoUs, recvWallMs, bytes);
        break;
      case 'bookTicker':
        this.handleBookTicker(p, recvMonoUs, recvWallMs, bytes);
        break;
      case 'markPriceUpdate':
        this.handleMarkPrice(p, recvMonoUs, recvWallMs, bytes);
        break;
      case 'forceOrder':
        this.handleLiquidation(p, recvMonoUs, recvWallMs, bytes);
        break;
      default:
        this.unknownMessages++;
        this.metrics.onOtherEvent(recvWallMs);
        this.emitRecord({
          kind: KIND.RAW_MSG,
          side: SIDE.NONE,
          flags: 0,
          rawBytes: bytes,
          seq: ++this.seq,
          updateId: 0,
          exchangeTs: p.E || 0,
          localRecvMs: recvWallMs,
          localMonoUs: recvMonoUs,
          latencyUs: monoUs() - recvMonoUs,
          price: 0,
          qty: 0,
          delta: 0,
          extra: 0,
        });
    }
  }

  handleDepth(p, recvMonoUs, recvWallMs, bytes) {
    const res = this.book.applyDiff(p);

    if (res.status === 'gap') {
      this.metrics.markGap();
      const g = this.book.gapDetail || {};
      this.emitRecord({
        kind: KIND.GAP,
        side: SIDE.NONE,
        flags: 0,
        rawBytes: bytes,
        seq: ++this.seq,
        updateId: p.u,
        exchangeTs: p.E,
        localRecvMs: recvWallMs,
        localMonoUs: recvMonoUs,
        latencyUs: monoUs() - recvMonoUs,
        price: 0,
        qty: 0,
        delta: g.expectedPu || 0,
        extra: g.gotPu || 0,
      });
      this.emit('note', {
        level: 'warn',
        text: `update-id chain broken: expected pu=${g.expectedPu} got pu=${g.gotPu}; resynchronizing`,
      });
      this.disarm('gap');
      this.fetchSnapshot('gap');
      return;
    }

    if (res.status === 'discarded') {
      this.depthDiscards++;
      return;
    }

    if (res.status === 'hole') {
      // The snapshot we hold is already behind the stream.  Refetch, keeping
      // the buffered frames so the next snapshot is guaranteed bridgeable.
      const h = this.book.holeDetail || {};
      this.syncMisses++;
      this.emit('note', {
        level: 'warn',
        text: `snapshot id ${h.snapshotId} already behind the stream (U=${h.U} pu=${h.pu}); retaining ${this.ingress.length} buffered frames and refetching`,
      });
      this.disarm('hole');
      this.fetchSnapshot('hole');
      return;
    }

    this.recordBookHistory(p.u);
    const changes = res.changes;
    let bookChanges = 0;
    for (let i = 0; i < changes.length; i++) if (changes[i].delta !== 0) bookChanges++;
    this.metrics.onDepthMessage(recvWallMs, changes.length, bookChanges);

    // Processing latency is measured for the whole message: parse + validate +
    // apply to the book.  Every level event derived from it shares that cost,
    // which is why it is reported per message, not per level.
    const doneUs = monoUs();
    const latencyUs = doneUs - recvMonoUs;
    this.metrics.onLatency(recvWallMs - p.E, latencyUs);

    for (let i = 0; i < changes.length; i++) {
      const c = changes[i];
      this.metrics.onNormalizedEvent(doneUs);
      this.emitRecord({
        kind: KIND.DEPTH,
        side: c.side,
        flags: (i === 0 ? FLAG.MSG_FIRST : 0) | (res.status === 'bridged' ? FLAG.RESYNC : 0),
        rawBytes: i === 0 ? bytes : 0,
        seq: ++this.seq,
        updateId: p.u,
        exchangeTs: p.E,
        localRecvMs: recvWallMs,
        localMonoUs: recvMonoUs,
        latencyUs,
        price: c.price,
        qty: c.qty,
        delta: c.delta,
        extra: p.U,
      });
    }
  }

  handleTrade(p, recvMonoUs, recvWallMs, bytes) {
    const price = Number(p.p);
    const qty = Number(p.q);
    // Binance emits periodic {"e":"trade","p":"0","q":"0","X":"NA"} markers on
    // this symbol. They are not trades: counting them would inflate TRADES/SEC
    // and a zero would poison the reference price used for burst analysis.
    if (!Number.isFinite(price) || !Number.isFinite(qty) || price <= 0 || qty <= 0) {
      this.metrics.onMarker();
      return;
    }
    // Binance `m` = "is buyer the maker".  If the buyer was the maker then the
    // aggressor was a seller, so the trade is classified by aggressor side.
    const buyerMaker = p.m === true;
    this.metrics.onTrade(recvWallMs, price, qty, buyerMaker);
    this.metrics.onLatency(recvWallMs - p.T, monoUs() - recvMonoUs);
    this.metrics.onNormalizedEvent(recvMonoUs);
    this.emitRecord({
      kind: KIND.TRADE,
      side: buyerMaker ? SIDE.ASK : SIDE.BID,
      flags: buyerMaker ? FLAG.TRADE_BUYER_MAKER : 0,
      rawBytes: bytes,
      seq: ++this.seq,
      updateId: Number(p.t) || 0,
      exchangeTs: p.T,
      localRecvMs: recvWallMs,
      localMonoUs: recvMonoUs,
      latencyUs: monoUs() - recvMonoUs,
      price,
      qty,
      delta: price * qty,
      extra: 0,
    });
  }

  handleBookTicker(p, recvMonoUs, recvWallMs, bytes) {
    const bid = Number(p.b);
    const ask = Number(p.a);
    this.metrics.onBookTicker(recvWallMs);
    this.metrics.onNormalizedEvent(recvMonoUs);
    if (Number.isFinite(bid) && Number.isFinite(ask) && ask > bid) {
      this.metrics.lastBook = { bid, ask, spread: ask - bid, mid: (ask + bid) / 2 };
    }
    this.metrics.lastPrice = this.metrics.lastPrice ?? (bid + ask) / 2;
    this.metrics.onLatency(recvWallMs - (p.E || recvWallMs), monoUs() - recvMonoUs);
    this.emitRecord({
      kind: KIND.BOOK_TICKER,
      side: SIDE.NONE,
      flags: 0,
      rawBytes: bytes,
      seq: ++this.seq,
      updateId: Number(p.u) || 0,
      exchangeTs: p.E || 0,
      localRecvMs: recvWallMs,
      localMonoUs: recvMonoUs,
      latencyUs: monoUs() - recvMonoUs,
      price: bid,
      qty: ask,
      delta: Number(p.A) || 0,
      extra: Number(p.B) || 0,
    });
  }

  handleMarkPrice(p, recvMonoUs, recvWallMs, bytes) {
    this.metrics.onOtherEvent(recvWallMs);
    this.metrics.onNormalizedEvent(recvMonoUs);
    this.emitRecord({
      kind: KIND.MARK_PRICE,
      side: SIDE.NONE,
      flags: 0,
      rawBytes: bytes,
      seq: ++this.seq,
      updateId: 0,
      exchangeTs: p.E || 0,
      localRecvMs: recvWallMs,
      localMonoUs: recvMonoUs,
      latencyUs: monoUs() - recvMonoUs,
      price: Number(p.p) || 0,
      qty: Number(p.i) || 0,
      delta: Number(p.r) || 0,
      extra: p.T || 0,
    });
  }

  handleLiquidation(p, recvMonoUs, recvWallMs, bytes) {
    const o = p.o || {};
    this.metrics.onOtherEvent(recvWallMs);
    this.metrics.onNormalizedEvent(recvMonoUs);
    this.emitRecord({
      kind: KIND.LIQUIDATION,
      side: o.S === 'BUY' ? SIDE.BID : SIDE.ASK,
      flags: 0,
      rawBytes: bytes,
      seq: ++this.seq,
      updateId: 0,
      exchangeTs: p.E || 0,
      localRecvMs: recvWallMs,
      localMonoUs: recvMonoUs,
      latencyUs: monoUs() - recvMonoUs,
      price: Number(o.p) || 0,
      qty: Number(o.q) || 0,
      delta: (Number(o.p) || 0) * (Number(o.q) || 0),
      extra: 0,
    });
  }

  /* ------------------------------------------------- snapshot / arming */

  disarm(reason) {
    if (!this.armed) return;
    this.armed = false;
    this.disarmReason = reason;
  }

  async fetchSnapshot(reason = 'manual') {
    if (this.snapshotPending) return;
    this.snapshotPending = true;
    this.snapshotAttempts++;
    const t0 = monoUs();
    try {
      const res = await fetch(
        `https://fapi.binance.com/fapi/v1/depth?symbol=${this.symbol}&limit=${this.depthLimit}`,
        { signal: AbortSignal.timeout(10000) },
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const snap = await res.json();
      this.lastSnapshotRttMs = (monoUs() - t0) / 1000;
      this.lastSnapshotWallMs = Date.now();
      this.lastSnapshotError = null;
      this.installSnapshot(snap);
      if (reason === 'gap') this.snapshotResyncs++;
    } catch (err) {
      this.lastSnapshotError = String(err && err.message ? err.message : err);
      this.emit('note', { level: 'error', text: `depth snapshot failed: ${this.lastSnapshotError}` });
      setTimeout(() => {
        this.snapshotPending = false;
        this.fetchSnapshot(reason);
      }, 1500);
      return;
    }
    this.snapshotPending = false;
  }

  /**
   * Can the retained buffer bridge a snapshot with this id?
   *
   * @returns {number} index of the first depth event that continues the chain,
   *   -1 if the buffer has already moved past the snapshot (a genuine hole),
   *   -2 if no depth event has been buffered yet.
   */
  findBridgeIndex(snapshotId, maxScan = 20000) {
    const n = Math.min(this.ingress.length, maxScan);
    for (let i = 0; i < n; i++) {
      let p;
      try {
        p = JSON.parse(this.ingress[i].data);
      } catch {
        continue;
      }
      const ev = p && p.data !== undefined ? p.data : p;
      if (!ev || ev.e !== 'depthUpdate') continue;
      const u = Number(ev.u);
      if (u < snapshotId) continue;
      return Number(ev.pu) <= snapshotId ? i : -1;
    }
    return -2;
  }

  /**
   * Install a REST snapshot and replay the retained buffer behind it.
   *
   * The race this defends against: when the REST response is generated at the
   * very start of its round trip (network-dominated latency), every frame that
   * arrived while we waited is already *ahead* of the snapshot, so nothing in
   * the buffer bridges it.  Installing that snapshot and waiting would be
   * unrecoverable - `pu` only ever increases, so the bridging event can never
   * arrive, and every depth update would be dropped silently for the rest of
   * the session.  Instead the buffer is retained (never discarded on failure)
   * and a fresher snapshot is fetched, which the frames already held are
   * guaranteed to bridge.
   */
  installSnapshot(snap) {
    const snapshotId = Number(snap.lastUpdateId);
    this.book.loadSnapshot({
      lastUpdateId: snapshotId,
      bids: snap.bids || [],
      asks: snap.asks || [],
    });
    this.candidateSnapshotId = snapshotId;

    if (this.findBridgeIndex(snapshotId) === -1) {
      this.syncMisses++;
      this.snapshotPending = false;
      this.emit('note', {
        level: 'warn',
        text: `depth snapshot ${snapshotId} was stale on arrival; retaining ${this.ingress.length} buffered frames and refetching`,
      });
      setTimeout(() => this.fetchSnapshot('sync-miss'), 150);
      return false;
    }

    // Emit the reset and the full book, then replay the retained frames in
    // arrival order.  This block is synchronous, so no websocket message can
    // interleave between the snapshot and the diffs that follow it.
    this.emitRecord({
      kind: KIND.RESET,
      side: SIDE.NONE,
      flags: 0,
      rawBytes: 0,
      seq: ++this.seq,
      updateId: snapshotId,
      exchangeTs: snap.E || 0,
      localRecvMs: wallMsHiRes(),
      localMonoUs: monoUs(),
      latencyUs: 0,
      price: 0,
      qty: 0,
      delta: this.snapshotResyncs,
      extra: RESET_REASON.RESYNC,
    });
    this.emitSnapshotRecords();

    const buffered = this.ingress;
    this.ingress = [];
    this.armed = true;
    const dropped = this.ingressOverflow || 0;
    this.ingressOverflow = 0;
    for (let i = 0; i < buffered.length; i++) {
      const m = buffered[i];
      this.process(m.data, m.recvMonoUs, m.recvWallMs, m.bytes);
    }
    this.replayedFrames = buffered.length;
    this.emit('note', {
      level: this.book.synced ? 'info' : 'warn',
      text:
        `book synchronized at lastUpdateId=${snapshotId} ` +
        `(${this.book.bids.size + this.book.asks.size} levels, replayed ${buffered.length} buffered frames` +
        `${dropped ? `, ${dropped} frames dropped from buffer` : ''})` +
        (this.book.synced ? '' : ' - no depth frame in buffer, waiting for one to bridge'),
    });
    this.metrics.onBookState(this.bookStateArgs());
    return true;
  }

  recordBookHistory(updateId) {
    const b = this.book.best();
    const h = this.bookHistory;
    h.push({
      u: updateId,
      bid: b.bid,
      bidQty: b.bid === null ? 0 : this.book.bids.get(b.bid) || 0,
      ask: b.ask,
      askQty: b.ask === null ? 0 : this.book.asks.get(b.ask) || 0,
      levels: this.book.size,
    });
    if (h.length > this.bookHistoryLimit) h.shift();
  }

  bookStateArgs() {
    const b = this.book.best();
    return {
      bid: b.bid,
      ask: b.ask,
      synced: this.book.synced,
      levels: this.book.size,
      lastUpdateId: this.book.lastUpdateId,
    };
  }

  /** Records describing the current book, for seeding clients and recordings. */
  snapshotRecords() {
    const out = [];
    const nowWall = wallMsHiRes();
    const nowMono = monoUs();
    const base = {
      flags: FLAG.RESYNC,
      rawBytes: 0,
      exchangeTs: 0,
      localRecvMs: nowWall,
      localMonoUs: nowMono,
      latencyUs: 0,
      delta: 0,
      extra: 0,
    };
    const b = this.book.best();
    out.push({
      ...base,
      kind: KIND.SNAPSHOT_BEGIN,
      side: SIDE.NONE,
      seq: this.seq,
      updateId: this.book.lastUpdateId,
      price: b.bid ?? 0,
      qty: b.ask ?? 0,
    });
    let seq = this.seq;
    for (const [side, rows] of [
      [SIDE.BID, this.book.top(0, SNAPSHOT_DEPTH)],
      [SIDE.ASK, this.book.top(1, SNAPSHOT_DEPTH)],
    ]) {
      for (const r of rows) {
        out.push({
          ...base,
          kind: KIND.SNAPSHOT_LEVEL,
          side,
          seq, // snapshots carry the current serial; they are not new events
          updateId: this.book.lastUpdateId,
          price: r.price,
          qty: r.qty,
        });
      }
    }
    out.push({
      ...base,
      kind: KIND.SNAPSHOT_END,
      side: SIDE.NONE,
      seq,
      updateId: this.book.lastUpdateId,
      price: 0,
      qty: 0,
    });
    return out;
  }

  emitSnapshotRecords() {
    for (const r of this.snapshotRecords()) this.emitRecord(r);
  }

  /**
   * Build a standalone snapshot frame for a client that connects mid-stream.
   * Generated synchronously inside the connection handler so no live record
   * can be interleaved between the snapshot and the events that follow it.
   */
  buildClientSnapshotFrame() {
    const recs = this.snapshotRecords();
    const w = new FrameWriter(Math.max(16, recs.length + 1));
    for (const r of recs) w.push(r);
    return w.finish();
  }

  /* ------------------------------------------------------------ egress */

  emitRecord(rec) {
    if (!this.writer.push(rec)) {
      this.flush();
      this.writer.push(rec);
    }
    this.recordsSent++;
    if (!this._flushScheduled) {
      this._flushScheduled = true;
      setImmediate(() => this.flush());
    }
  }

  flush() {
    this._flushScheduled = false;
    if (this.writer.count === 0) return;
    const frame = this.writer.finish();
    this.framesSent++;
    this.writer.reset();
    this.emit('frames', frame);
  }

  /* --------------------------------------------------------------- tick */

  tick() {
    const now = wallMsHiRes();
    this.metrics.tick(now);
    this.metrics.onBookState(this.bookStateArgs());
  }

  /** Full status payload sent to clients and exposed over HTTP. */
  status() {
    const m = this.metrics.snapshot(wallMsHiRes());
    return {
      symbol: this.symbol,
      armed: this.armed,
      synced: this.book.synced,
      feed: this.feed.status(),
      book: {
        bids: this.book.bids.size,
        asks: this.book.asks.size,
        lastUpdateId: this.book.lastUpdateId,
        stats: this.book.stats,
      },
      sequencer: {
        seq: this.seq,
        bufferedFrames: this.ingress.length,
        ingressOverflow: this.ingressOverflow || 0,
        depthDiscards: this.depthDiscards,
        unknownMessages: this.unknownMessages,
        syncMisses: this.syncMisses,
        replayedFrames: this.replayedFrames,
        candidateSnapshotId: this.candidateSnapshotId,
      },
      snapshot: {
        pending: this.snapshotPending,
        attempts: this.snapshotAttempts,
        resyncs: this.snapshotResyncs,
        lastRttMs: this.lastSnapshotRttMs,
        lastWallMs: this.lastSnapshotWallMs,
        lastError: this.lastSnapshotError,
      },
      clock: {
        offsetMs: Number.isFinite(clockOffset.offsetMs) ? clockOffset.offsetMs : null,
        rttMs: Number.isFinite(clockOffset.rttMs) ? clockOffset.rttMs : null,
        attempts: clockOffset.attempts,
        successes: clockOffset.successes,
        lastError: clockOffset.lastError,
      },
      framesSent: this.framesSent,
      recordsSent: this.recordsSent,
      loopLag: {
        lastMs: this.loopLag.lastMs,
        maxMs: this.loopLag.maxMs,
        p50Ms: this.loopLag.p50Ms,
        p99Ms: this.loopLag.p99Ms,
        samples: this.loopLag.samples,
        series: this.loopLag.series,
      },
      metrics: m,
    };
  }
}

export { RECORD_BYTES };
