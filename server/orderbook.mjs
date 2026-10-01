/**
 * BTCUSDT USDⓈ-M futures L2 order book state machine.
 *
 * Correctness is anchored on Binance's `pu` field (previous update id), which
 * forms a strict hash chain across depthUpdate messages:
 *
 *     event[n].pu === event[n-1].u
 *
 * That property is what makes it possible to *prove* the local book is
 * consistent with the exchange instead of hoping it is.  Any break in the
 * chain is a real, detected data-integrity event and triggers a resync; the
 * gap counter is surfaced in the UI rather than hidden.
 *
 * Synchronization procedure (matches the USDⓈ-M futures documentation):
 *   1. start buffering depthUpdate events
 *   2. fetch the REST depth snapshot (lastUpdateId)
 *   3. drop buffered events with u < lastUpdateId
 *   4. apply the first buffered event with pu <= lastUpdateId <= u
 *   5. from then on require pu === previous u
 */

export class OrderBook {
  constructor({ maxLevels = 5000 } = {}) {
    /** @type {Map<number, number>} price -> quantity */
    this.bids = new Map();
    /** @type {Map<number, number>} price -> quantity */
    this.asks = new Map();
    this.lastUpdateId = 0;
    this.synced = false;
    this.maxLevels = maxLevels;

    this.stats = {
      applied: 0,
      discardedStale: 0,
      gaps: 0,
      holes: 0,
      resyncs: 0,
      levelChanges: 0,
      insertions: 0,
      deletions: 0,
      snapshots: 0,
    };

    /** Reusable output array for changed levels of a single diff. */
    this._changes = [];
    this._pendingKey = null;
  }

  get size() {
    return this.bids.size + this.asks.size;
  }

  /**
   * Install a REST snapshot.  The book is not considered usable until the
   * first matching stream event has been applied, because between the REST
   * response and the stream there is an unknown gap.
   */
  loadSnapshot(snapshot) {
    this.bids.clear();
    this.asks.clear();
    for (const [p, q] of snapshot.bids) {
      const price = Number(p);
      const qty = Number(q);
      if (qty > 0) this.bids.set(price, qty);
    }
    for (const [p, q] of snapshot.asks) {
      const price = Number(p);
      const qty = Number(q);
      if (qty > 0) this.asks.set(price, qty);
    }
    this.lastUpdateId = snapshot.lastUpdateId;
    this.synced = false;
    this.stats.snapshots++;
    return this;
  }

  /** Force the book back into the "waiting for a bridging event" state. */
  invalidate(reason = 'resync') {
    this.synced = false;
    this.stats.resyncs++;
    this.lastInvalidateReason = reason;
  }

  /**
   * Apply one depthUpdate event.
   *
   * @param {{U:number,u:number,pu:number,b:Array<[string,string]>,a:Array<[string,string]>}} ev
   * @returns {{status:'applied'|'discarded'|'bridged'|'gap'|'hole', changes:Array}}
   */
  applyDiff(ev) {
    const { U, u, pu } = ev;
    this._changes.length = 0;

    if (!this.synced) {
      // Stale: entirely at or before the snapshot.
      if (u < this.lastUpdateId) {
        this.stats.discardedStale++;
        return { status: 'discarded', changes: this._changes };
      }
      if (pu <= this.lastUpdateId && u >= this.lastUpdateId) {
        // The snapshot point falls inside this event, or this event is exactly
        // the one that follows it.  Either way the chain is continuous.
        this.synced = true;
        this.applyLevels(ev);
        this.lastUpdateId = u;
        this.stats.applied++;
        return { status: 'bridged', changes: this._changes };
      }
      // pu > lastUpdateId: the event stream has already moved past the
      // snapshot, so the event that bridges it was never seen.  This must be
      // reported as a hole; treating it as "not yet" would silently ignore
      // every subsequent event forever, because pu only ever increases.
      this.stats.holes++;
      this.holeDetail = { snapshotId: this.lastUpdateId, U, u, pu };
      return { status: 'hole', changes: this._changes };
    }

    // Step 5: strict chain.
    if (pu === this.lastUpdateId) {
      this.applyLevels(ev);
      this.lastUpdateId = u;
      this.stats.applied++;
      return { status: 'applied', changes: this._changes };
    }
    if (u <= this.lastUpdateId) {
      this.stats.discardedStale++;
      return { status: 'discarded', changes: this._changes };
    }
    // pu > lastUpdateId means events were lost between the two.
    this.stats.gaps++;
    this.gapDetail = { expectedPu: this.lastUpdateId, gotPu: pu, U, u };
    this.synced = false;
    return { status: 'gap', changes: this._changes };
  }

  /** Apply the b[]/a[] level arrays of a diff to the maps. */
  applyLevels(ev) {
    const changes = this._changes;
    if (ev.b) {
      for (let i = 0; i < ev.b.length; i++) {
        this._applyLevel(0, ev.b[i][0], ev.b[i][1], changes);
      }
    }
    if (ev.a) {
      for (let i = 0; i < ev.a.length; i++) {
        this._applyLevel(1, ev.a[i][0], ev.a[i][1], changes);
      }
    }
    this.stats.levelChanges += changes.length;
  }

  _applyLevel(side, rawPrice, rawQty, changes) {
    const price = Number(rawPrice);
    const qty = Number(rawQty);
    const map = side === 0 ? this.bids : this.asks;
    const prev = map.get(price);
    const delta = qty - (prev === undefined ? 0 : prev);

    if (qty === 0) {
      if (prev !== undefined) {
        map.delete(price);
        this.stats.deletions++;
      } else {
        // Deleting a level we never had: legal (it was outside our snapshot
        // depth).  Nothing changes.
        return;
      }
    } else {
      if (prev === undefined) this.stats.insertions++;
      map.set(price, qty);
      if (map.size > this.maxLevels) this._trim(side, map);
    }

    changes.push({
      side,
      price,
      qty,
      delta,
      wasNew: prev === undefined,
    });
  }

  /** Drop the deepest levels if a venue ever sends us an absurd book. */
  _trim(side, map) {
    const prices = [...map.keys()].sort((x, y) => (side === 0 ? y - x : x - y));
    for (let i = this.maxLevels; i < prices.length; i++) map.delete(prices[i]);
  }

  /** Best bid (highest) and best ask (lowest). */
  best() {
    let bid = -Infinity;
    for (const p of this.bids.keys()) if (p > bid) bid = p;
    let ask = Infinity;
    for (const p of this.asks.keys()) if (p < ask) ask = p;
    return {
      bid: Number.isFinite(bid) ? bid : null,
      ask: Number.isFinite(ask) ? ask : null,
    };
  }

  /**
   * Top `n` levels of one side, sorted, with cumulative quantity.
   * Linear selection is used rather than a full sort: with a few thousand
   * levels and a 5 Hz integrity check this is far cheaper.
   */
  top(side, n) {
    const map = side === 0 ? this.bids : this.asks;
    const prices = new Float64Array(map.size);
    let i = 0;
    for (const p of map.keys()) prices[i++] = p;
    const arr = prices.subarray(0, i);
    arr.sort();
    const out = [];
    let cum = 0;
    const count = Math.min(n, arr.length);
    for (let k = 0; k < count; k++) {
      // bids descend from the highest price, asks ascend from the lowest
      const price = side === 0 ? arr[arr.length - 1 - k] : arr[k];
      const qty = map.get(price);
      cum += qty;
      out.push({ price, qty, cum });
    }
    return out;
  }

  /** Full depth dump used when (re)seeding a client. */
  dump(depth = 1000) {
    return {
      lastUpdateId: this.lastUpdateId,
      synced: this.synced,
      bids: this.top(0, depth),
      asks: this.top(1, depth),
    };
  }
}
