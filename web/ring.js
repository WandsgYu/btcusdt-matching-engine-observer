/**
 * Typed-array event ring.
 *
 * The browser side of the pipeline stores every normalized event in one flat
 * byte ring.  No per-event objects, no per-event DOM, no garbage: a 1M-event
 * buffer is a single allocation, and the renderer addresses it by index.
 *
 * When the page is cross-origin isolated (the server sends COOP/COEP) the ring
 * lives in a SharedArrayBuffer and the ingest worker is the single writer while
 * the render loop is the single reader, with the write cursor published through
 * Atomics.  Otherwise the same class wraps a plain ArrayBuffer and the main
 * thread owns the writes.
 */

/** One normalized event is a fixed 88-byte record. Keep in sync with the server. */
export const RECORD_BYTES = 88;
export const HEADER_BYTES = 128;
export const DEFAULT_CAPACITY = 1 << 20; // 1,048,576 events

export class EventRing {
  /**
   * @param {number} capacity number of records
   * @param {{shared?: boolean, buffer?: ArrayBufferLike}} [opts]
   */
  constructor(capacity = DEFAULT_CAPACITY, opts = {}) {
    const shared = opts.shared === true;
    const total = HEADER_BYTES + capacity * RECORD_BYTES;
    this.buffer = opts.buffer || (shared ? new SharedArrayBuffer(total) : new ArrayBuffer(total));
    this.capacity = capacity;
    this.shared = shared;
    this.header = new BigInt64Array(this.buffer, 0, 16);
    this.bytes = new Uint8Array(this.buffer, HEADER_BYTES);
    this.dv = new DataView(this.buffer, HEADER_BYTES);
    this.totalBytes = total;
    if (!opts.buffer) this.writeIndex = 0;
  }

  get writeIndex() {
    return Number(this.shared ? Atomics.load(this.header, 0) : this.header[0]);
  }

  set writeIndex(v) {
    if (this.shared) Atomics.store(this.header, 0, BigInt(v));
    else this.header[0] = BigInt(v);
  }

  /** Byte offset of record `i` inside `this.bytes`. */
  offsetOf(i) {
    return (i % this.capacity) * RECORD_BYTES;
  }

  /**
   * Append `count` raw records from `src` starting at `srcOffset`.
   * Copying contiguous runs means at most two `set` calls per frame, so a
   * 500-record frame is two memcpys rather than 500.
   */
  appendRecords(src, srcOffset, count) {
    if (count <= 0) return;
    const start = this.writeIndex;
    let done = 0;
    while (done < count) {
      const slot = (start + done) % this.capacity;
      const run = Math.min(count - done, this.capacity - slot);
      const from = srcOffset + done * RECORD_BYTES;
      this.bytes.set(src.subarray(from, from + run * RECORD_BYTES), slot * RECORD_BYTES);
      done += run;
    }
    this.writeIndex = start + count;
  }

  /** Read one record into `out` (reused object, no allocation). */
  read(i, out) {
    const off = this.offsetOf(i);
    const dv = this.dv;
    out.kind = dv.getUint8(off + 0);
    out.side = dv.getUint8(off + 1);
    out.flags = dv.getUint16(off + 2, true);
    out.rawBytes = dv.getUint32(off + 4, true);
    out.seq = dv.getFloat64(off + 8, true);
    out.updateId = dv.getFloat64(off + 16, true);
    out.exchangeTs = dv.getFloat64(off + 24, true);
    out.localRecvMs = dv.getFloat64(off + 32, true);
    out.localMonoUs = dv.getFloat64(off + 40, true);
    out.latencyUs = dv.getFloat64(off + 48, true);
    out.price = dv.getFloat64(off + 56, true);
    out.qty = dv.getFloat64(off + 64, true);
    out.delta = dv.getFloat64(off + 72, true);
    out.extra = dv.getFloat64(off + 80, true);
    return out;
  }
}

