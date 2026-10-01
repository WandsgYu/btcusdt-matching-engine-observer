/**
 * Real-time metric aggregation.
 *
 * Everything here is derived from data that actually crossed the socket.  No
 * value in this file is modelled, smoothed for appearance, or invented.
 *
 * Windowed counters use 50 ms buckets.  A "1 s window" is therefore the sum of
 * the last 20 buckets: the windows are bucket-quantized, which is stated in the
 * UI rather than hidden.  Buckets are reused via an epoch stamp so the whole
 * 60 s of history costs a fixed amount of memory and zero allocation per event.
 *
 * Percentiles use bounded sample rings (last 8192 samples) and an exact sort,
 * not a histogram approximation, so P99 numbers are literally true.
 */

const BUCKET_MS = 50;
const BUCKET_COUNT = 1200; // 60 s of history
const SERIES_LEN = 120; // 120 one-second samples for sparklines
const SAMPLE_RING = 8192; // exact percentile window

/** Counter channels stored per bucket. */
const CH = Object.freeze({
  EVENTS: 0,
  DEPTH_MSGS: 1,
  DEPTH_LEVELS: 2,
  BOOK_CHANGES: 3,
  TRADES: 4,
  BTC: 5,
  NOTIONAL: 6,
  WS_BYTES: 7,
  WS_MSGS: 8,
  BOOK_TICKERS: 9,
});
const CH_COUNT = 10;

/** Inter-arrival histogram edges in microseconds, as required by the spec. */
export const IA_BINS = Object.freeze([
  { label: '<100 \u00b5s', lo: 0, hi: 100 },
  { label: '100\u2013500 \u00b5s', lo: 100, hi: 500 },
  { label: '0.5\u20131 ms', lo: 500, hi: 1000 },
  { label: '1\u20135 ms', lo: 1000, hi: 5000 },
  { label: '5\u201310 ms', lo: 5000, hi: 10000 },
  { label: '>10 ms', lo: 10000, hi: Infinity },
]);

class SampleRing {
  constructor(capacity = SAMPLE_RING) {
    this.capacity = capacity;
    this.data = new Float64Array(capacity);
    this.scratch = new Float64Array(capacity);
    this.n = 0;
    this.head = 0;
    this._stats = null;
    this._statsWall = 0;
  }

  /**
   * Percentiles, recomputed at most every `intervalMs`.
   *
   * Sorting 8192 samples costs ~0.1-0.3 ms. Doing that for four rings on every
   * 10 Hz metrics frame blocked the websocket receive path for ~2-3 ms several
   * times a second, which showed up as a p95 of several milliseconds in the
   * *processing* latency - the metrics were measuring themselves. Percentiles
   * do not need to move 10 times a second, so they are cached and refreshed on
   * a slower cadence while the counters stay exact and current.
   */
  stats(nowWallMs = Date.now(), intervalMs = 200) {
    if (this._stats && nowWallMs - this._statsWall < intervalMs) return this._stats;
    const s = this.sorted();
    this._stats = {
      p50: pct(s, 50),
      p95: pct(s, 95),
      p99: pct(s, 99),
      max: s.length ? s[s.length - 1] : null,
      min: s.length ? s[0] : null,
      samples: s.length,
    };
    this._statsWall = nowWallMs;
    return this._stats;
  }

  push(v) {
    this.data[this.head] = v;
    this.head = (this.head + 1) % this.capacity;
    if (this.n < this.capacity) this.n++;
  }

  /** Copy the live samples into scratch, sort ascending, return the view. */
  sorted() {
    const s = this.scratch.subarray(0, this.n);
    const d = this.data;
    const start = this.n < this.capacity ? 0 : this.head;
    for (let i = 0; i < this.n; i++) {
      s[i] = d[(start + i) % this.capacity];
    }
    s.sort();
    return s;
  }

  /** Last k samples in arrival order (oldest first). */
  tail(k, out = []) {
    out.length = 0;
    const count = Math.min(k, this.n);
    const start = (this.head - count + this.capacity) % this.capacity;
    for (let i = 0; i < count; i++) out.push(this.data[(start + i) % this.capacity]);
    return out;
  }
}

/** Tracks the arrival process of a stream of timestamps. */
/** Index of the histogram bin a gap belongs to. */
function binOf(v) {
  for (let b = 0; b < IA_BINS.length; b++) if (v < IA_BINS[b].hi) return b;
  return IA_BINS.length - 1;
}

export class InterArrivalTracker {
  constructor(name, capacity = SAMPLE_RING) {
    this.name = name;
    this.ring = new SampleRing(capacity);
    this.lastMonoUs = null;
    this.total = 0;
    this.min = Infinity;
    this.max = 0;
    /** Maintained incrementally: O(1) per sample instead of O(n x bins) per frame. */
    this.hist = new Array(IA_BINS.length).fill(0);
  }

  /** Feed a monotonic arrival timestamp in microseconds. */
  push(monoUsNow) {
    if (this.lastMonoUs !== null) {
      const d = monoUsNow - this.lastMonoUs;
      if (d >= 0) {
        const r = this.ring;
        if (r.n === r.capacity) this.hist[binOf(r.data[r.head])]--;
        r.data[r.head] = d;
        r.head = (r.head + 1) % r.capacity;
        if (r.n < r.capacity) r.n++;
        this.hist[binOf(d)]++;
        this.total++;
        if (d < this.min) this.min = d;
        if (d > this.max) this.max = d;
      }
    }
    this.lastMonoUs = monoUsNow;
  }

  reset() {
    this.lastMonoUs = null;
  }

  snapshot(wallMsNow = Date.now(), tailK = 28) {
    const st = this.ring.stats(wallMsNow);
    return {
      name: this.name,
      samples: st.samples,
      total: this.total,
      min: Number.isFinite(this.min) ? this.min : null,
      max: this.max || null,
      p50: st.p50,
      p95: st.p95,
      p99: st.p99,
      hist: this.hist.slice(),
      bins: IA_BINS.map((b) => b.label),
      last: this.ring.tail(tailK),
    };
  }
}

function pct(sorted, p) {
  const n = sorted.length;
  if (n === 0) return null;
  const idx = Math.min(n - 1, Math.max(0, Math.ceil((p / 100) * n) - 1));
  return sorted[idx];
}

export class Metrics {
  constructor() {
    this.sessionStartWallMs = Date.now();
    this.sessionStartMonoUs = null;

    this.bucketEpoch = new Int32Array(BUCKET_COUNT).fill(-1);
    this.ch = [];
    for (let i = 0; i < CH_COUNT; i++) this.ch.push(new Float64Array(BUCKET_COUNT));

    // session cumulative counters (exact, never windowed)
    this.totals = {
      events: 0,
      depthMsgs: 0,
      depthLevels: 0,
      bookChanges: 0,
      trades: 0,
      btc: 0,
      notional: 0,
      wsBytes: 0,
      wsMsgs: 0,
      bookTickers: 0,
      buyTrades: 0,
      sellTrades: 0,
      largestTradeBtc: 0,
      markers: 0,
    };

    this.peak = { events1s: 0, events1sWallMs: 0, depthLevels1s: 0, trades1s: 0, notional1s: 0 };

    /** One-second samples, oldest first. */
    this.series = {
      events: [],
      depthLevels: [],
      trades: [],
      notional: [],
      wsBytes: [],
      spread: [],
      mid: [],
      latencyP50: [],
    };

    this.msgInterArrival = new InterArrivalTracker('ws-message');
    this.eventInterArrival = new InterArrivalTracker('normalized-event');

    this.latencyExch = new SampleRing(); // exchange -> local, ms
    this.latencyProc = new SampleRing(); // local processing, us

    /**
     * Last price from a *real* trade. Binance periodically emits
     * {"e":"trade","p":"0","q":"0","X":"NA"} markers on this symbol; they carry
     * no economic content and must never move the reference price or be counted
     * as trades, or both TRADES/SEC and burst price movement become wrong.
     */
    this.lastPrice = null;
    this.lastBook = { bid: null, ask: null, spread: null, mid: null };

    this.baseline = { rate1s: null, samples: [], updatedWallMs: 0 };
    this.bursts = [];
    this.activeBurst = null;
    this._burstSeq = 0;

    this._lastSecondWall = Math.floor(Date.now() / 1000) * 1000;
    this._secAcc = new Float64Array(CH_COUNT);
    this._secSpread = [];
    this._secMid = [];
    this._secLatency = [];

    this.integrity = {
      seqGaps: 0,
      resyncs: 0,
      bookSynced: false,
      bookLevels: 0,
      lastUpdateId: 0,
      dupes: 0,
    };
  }

  /**
   * Access the bucket covering `wallMsNow`, resetting it if it is recycled.
   *
   * The stamp kept in `bucketEpoch` is a *generation* (bucket index divided by
   * the ring length), not the raw bucket index: the raw index is ~3.6e10 and
   * would silently overflow the Int32Array, making every comparison fail and
   * every window sum reset the buckets it read.
   */
  _slotFor(idx) {
    return ((idx % BUCKET_COUNT) + BUCKET_COUNT) % BUCKET_COUNT;
  }

  _bucket(wallMsNow) {
    const idx = Math.floor(wallMsNow / BUCKET_MS);
    const slot = this._slotFor(idx);
    const gen = Math.floor(idx / BUCKET_COUNT);
    if (this.bucketEpoch[slot] !== gen) {
      this.bucketEpoch[slot] = gen;
      for (let i = 0; i < CH_COUNT; i++) this.ch[i][slot] = 0;
    }
    return slot;
  }

  /** Sum channel `c` over the window of `ms` milliseconds ending at `now`. */
  windowSum(c, ms, now) {
    const buckets = Math.max(1, Math.round(ms / BUCKET_MS));
    const endIdx = Math.floor(now / BUCKET_MS);
    let sum = 0;
    for (let k = 0; k < buckets; k++) {
      const idx = endIdx - k;
      const slot = this._slotFor(idx);
      if (this.bucketEpoch[slot] === Math.floor(idx / BUCKET_COUNT)) sum += this.ch[c][slot];
    }
    return sum;
  }

  /** Record a depthUpdate message and the level changes we derived from it. */
  onDepthMessage(wallMsNow, levelChanges, bookChanges) {
    const slot = this._bucket(wallMsNow);
    this.ch[CH.DEPTH_MSGS][slot] += 1;
    this.ch[CH.DEPTH_LEVELS][slot] += levelChanges;
    this.ch[CH.BOOK_CHANGES][slot] += bookChanges;
    this.ch[CH.EVENTS][slot] += levelChanges;
    this.totals.depthMsgs++;
    this.totals.depthLevels += levelChanges;
    this.totals.bookChanges += bookChanges;
    this.totals.events += levelChanges;
  }

  onBookTicker(wallMsNow) {
    const slot = this._bucket(wallMsNow);
    this.ch[CH.BOOK_TICKERS][slot] += 1;
    this.ch[CH.EVENTS][slot] += 1;
    this.totals.bookTickers++;
    this.totals.events++;
  }

  onTrade(wallMsNow, price, qty, isBuyerMaker) {
    if (!Number.isFinite(price) || !Number.isFinite(qty) || price <= 0 || qty <= 0) {
      this.onMarker();
      return false;
    }
    const slot = this._bucket(wallMsNow);
    const notional = price * qty;
    this.ch[CH.TRADES][slot] += 1;
    this.ch[CH.BTC][slot] += qty;
    this.ch[CH.NOTIONAL][slot] += notional;
    this.ch[CH.EVENTS][slot] += 1;
    this.totals.trades++;
    this.totals.btc += qty;
    this.totals.notional += notional;
    this.totals.events++;
    if (isBuyerMaker) this.totals.sellTrades++;
    else this.totals.buyTrades++;
    if (qty > this.totals.largestTradeBtc) this.totals.largestTradeBtc = qty;
    this.lastPrice = price;
    return true;
  }

  /** Best available reference price: a real trade, else the book mid. */
  referencePrice() {
    if (Number.isFinite(this.lastPrice) && this.lastPrice > 0) return this.lastPrice;
    if (Number.isFinite(this.lastBook.mid) && this.lastBook.mid > 0) return this.lastBook.mid;
    return null;
  }

  /**
   * A normalized event that carries no book state (mark price, unknown type).
   * Raw socket bytes are counted once per message in onRawMessage(), never
   * here, so "WS BYTES/S" cannot double count.
   */
  /** A trade-shaped message with no economic content (price/qty zero). */
  onMarker() {
    this.totals.markers++;
  }

  onOtherEvent(wallMsNow) {
    const slot = this._bucket(wallMsNow);
    this.ch[CH.EVENTS][slot] += 1;
    this.totals.events++;
  }

  /** One raw WebSocket message: the true ingress process. */
  onRawMessage(bytes, monoUsNow, wallMsNow) {
    this.msgInterArrival.push(monoUsNow);
    this.totals.wsMsgs++;
    this.totals.wsBytes += bytes;
    const slot = this._bucket(wallMsNow);
    this.ch[CH.WS_BYTES][slot] += bytes;
    this.ch[CH.WS_MSGS][slot] += 1;
  }

  /** One normalized event emitted into the firehose. */
  onNormalizedEvent(monoUsNow) {
    this.eventInterArrival.push(monoUsNow);
  }

  onLatency(exchangeToLocalMs, processUs) {
    if (Number.isFinite(exchangeToLocalMs) && exchangeToLocalMs > -5000 && exchangeToLocalMs < 60000) {
      this.latencyExch.push(exchangeToLocalMs);
    }
    if (Number.isFinite(processUs) && processUs >= 0 && processUs < 1e7) {
      this.latencyProc.push(processUs);
    }
  }

  onBookState({ bid, ask, synced, levels, lastUpdateId }) {
    this.integrity.bookSynced = synced;
    this.integrity.bookLevels = levels;
    this.integrity.lastUpdateId = lastUpdateId;
    if (bid !== null && ask !== null) {
      this.lastBook = { bid, ask, spread: ask - bid, mid: (ask + bid) / 2 };
    }
  }

  /** Must be called about once per 50 ms; rolls the 1 s series and detection. */
  tick(wallMsNow) {
    const sec = Math.floor(wallMsNow / 1000) * 1000;
    if (sec - this._lastSecondWall < 1000) return;
    this._lastSecondWall = sec;

    const ev1s = this.windowSum(CH.EVENTS, 1000, wallMsNow);
    const dl1s = this.windowSum(CH.DEPTH_LEVELS, 1000, wallMsNow);
    const tr1s = this.windowSum(CH.TRADES, 1000, wallMsNow);
    const nt1s = this.windowSum(CH.NOTIONAL, 1000, wallMsNow);
    const by1s = this.windowSum(CH.WS_BYTES, 1000, wallMsNow);

    push1(this.series.events, ev1s);
    push1(this.series.depthLevels, dl1s);
    push1(this.series.trades, tr1s);
    push1(this.series.notional, nt1s);
    push1(this.series.wsBytes, by1s);
    push1(this.series.spread, this.lastBook.spread);
    push1(this.series.mid, this.lastBook.mid);
    push1(this.series.latencyP50, this.latencyProc.stats(wallMsNow).p50);

    if (ev1s > this.peak.events1s) {
      this.peak.events1s = ev1s;
      this.peak.events1sWallMs = wallMsNow;
    }
    if (dl1s > this.peak.depthLevels1s) this.peak.depthLevels1s = dl1s;
    if (tr1s > this.peak.trades1s) this.peak.trades1s = tr1s;
    if (nt1s > this.peak.notional1s) this.peak.notional1s = nt1s;

    // Baseline: median of the last 120 one-second samples, excluding the most
    // recent 10 s so that a sustained burst cannot inflate its own baseline.
    const hist = this.series.events;
    if (hist.length >= 30) {
      const upto = Math.max(0, hist.length - 10);
      const base = hist.slice(0, upto).filter((v) => Number.isFinite(v));
      if (base.length >= 10) {
        base.sort((a, b) => a - b);
        this.baseline.rate1s = base[Math.floor(base.length / 2)];
        this.baseline.samples = base.length;
        this.baseline.updatedWallMs = wallMsNow;
      }
    }

    this._detectBurst(wallMsNow, ev1s, dl1s, tr1s);
  }

  /**
   * Burst detection.
   *
   * A burst is a genuine regime change in the *observed* event rate, not a
   * cosmetic alert: the current 1 s rate must exceed 2.5x the trailing median
   * baseline and clear an absolute floor so an idle market cannot trigger it.
   * The recorded burst carries the price movement, traded volume and book
   * update intensity measured over the same interval, all from real events.
   */
  _detectBurst(wallMsNow, ev1s, dl1s, tr1s) {
    const base = this.baseline.rate1s;
    const floor = 200;
    const RATIO = 2.5;
    const EXIT_RATIO = 1.5;

    if (!this.activeBurst) {
      if (base && base > 0 && ev1s >= floor && ev1s >= base * RATIO) {
        this.activeBurst = {
          id: ++this._burstSeq,
          startWallMs: wallMsNow,
          endWallMs: null,
          peakRate: ev1s,
          baseline: base,
          ratio: ev1s / base,
          peakDepthRate: dl1s,
          peakTradeRate: tr1s,
          priceStart: this.referencePrice(),
          priceEnd: this.referencePrice(),
          btcStart: this.totals.btc,
          tradesStart: this.totals.trades,
          levelsStart: this.totals.depthLevels,
          notionalStart: this.totals.notional,
          ongoing: true,
        };
      }
      return;
    }

    const b = this.activeBurst;
    if (ev1s > b.peakRate) {
      b.peakRate = ev1s;
      b.ratio = base ? ev1s / base : b.ratio;
    }
    if (dl1s > b.peakDepthRate) b.peakDepthRate = dl1s;
    if (tr1s > b.peakTradeRate) b.peakTradeRate = tr1s;
    b.priceEnd = this.referencePrice();

    if (!base || ev1s < base * EXIT_RATIO) {
      b.ongoing = false;
      b.endWallMs = wallMsNow;
      b.durationMs = b.endWallMs - b.startWallMs;
      b.btc = this.totals.btc - b.btcStart;
      b.trades = this.totals.trades - b.tradesStart;
      b.levels = this.totals.depthLevels - b.levelsStart;
      b.notional = this.totals.notional - (b.notionalStart || 0);
      b.priceMovePct =
        b.priceStart && b.priceEnd ? ((b.priceEnd - b.priceStart) / b.priceStart) * 100 : 0;
      this.bursts.unshift(b);
      if (this.bursts.length > 40) this.bursts.pop();
      this.activeBurst = null;
    }
  }

  sessionMs(wallMsNow) {
    return wallMsNow - this.sessionStartWallMs;
  }

  /** Build the JSON metrics payload streamed to clients. */
  snapshot(wallMsNow) {
    const W = [100, 1000, 10000];
    const rate = (c, ms) => this.windowSum(c, ms, wallMsNow) / (ms / 1000);
    const rates = (c) => ({ w100ms: rate(c, W[0]), w1s: rate(c, W[1]), w10s: rate(c, W[2]) });
    const sessionMs = this.sessionMs(wallMsNow);

    const exchStats = this.latencyExch.stats(wallMsNow);
    const procStats = this.latencyProc.stats(wallMsNow + 90);

    return {
      t: wallMsNow,
      sessionMs,
      counters: {
        events: this.totals.events,
        depthMsgs: this.totals.depthMsgs,
        depthLevels: this.totals.depthLevels,
        bookChanges: this.totals.bookChanges,
        trades: this.totals.trades,
        bookTickers: this.totals.bookTickers,
        wsMsgs: this.totals.wsMsgs,
        wsBytes: this.totals.wsBytes,
        btc: this.totals.btc,
        notional: this.totals.notional,
        buyTrades: this.totals.buyTrades,
        sellTrades: this.totals.sellTrades,
        largestTradeBtc: this.totals.largestTradeBtc,
        markers: this.totals.markers,
      },
      avgEventsPerSec: sessionMs > 0 ? this.totals.events / (sessionMs / 1000) : 0,
      peak: this.peak,
      rates: {
        events: rates(CH.EVENTS),
        depthMsgs: rates(CH.DEPTH_MSGS),
        depthLevels: rates(CH.DEPTH_LEVELS),
        bookChanges: rates(CH.BOOK_CHANGES),
        trades: rates(CH.TRADES),
        bookTickers: rates(CH.BOOK_TICKERS),
        btc: rates(CH.BTC),
        notional: rates(CH.NOTIONAL),
        wsBytes: rates(CH.WS_BYTES),
        wsMsgs: rates(CH.WS_MSGS),
      },
      book: {
        ...this.lastBook,
        ...this.integrity,
      },
      price: this.referencePrice(),
      interArrival: {
        // The raw ingress process: one sample per WebSocket message.
        message: this.msgInterArrival.snapshot(wallMsNow),
        // The derived event process: includes per-level depth expansion.
        event: this.eventInterArrival.snapshot(wallMsNow + 90),
      },
      latency: {
        exchangeToLocal: exchStats,
        process: procStats,
      },
      baseline: { rate1s: this.baseline.rate1s, samples: this.baseline.samples },
      bursts: this.bursts.slice(0, 12),
      activeBurst: this.activeBurst,
      series: {
        events: this.series.events,
        depthLevels: this.series.depthLevels,
        trades: this.series.trades,
        notional: this.series.notional,
        wsBytes: this.series.wsBytes,
        spread: this.series.spread,
        mid: this.series.mid,
        latencyP50: this.series.latencyP50,
      },
      windowMs: BUCKET_MS,
      seriesLen: SERIES_LEN,
    };
  }

  markGap() {
    this.integrity.seqGaps++;
  }

  markResync() {
    this.integrity.resyncs++;
  }

  markDupe() {
    this.integrity.dupes++;
  }
}

function push1(arr, v) {
  arr.push(Number.isFinite(v) ? v : null);
  if (arr.length > SERIES_LEN) arr.shift();
}
