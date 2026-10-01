/**
 * Wire protocol shared by the Node ingestion server and the browser client.
 *
 * Two layers:
 *
 *  1. FRAME  - the transport unit written to the HTTP chunked stream and to
 *              the .binlog recording file.  Layout:
 *
 *                u8   frameType        (FRAME.EVENTS | FRAME.CONTROL)
 *                u32  payloadLength    little endian
 *                ...  payload
 *
 *              FRAME.EVENTS payload:  u32 count, then count * RECORD_BYTES
 *              FRAME.CONTROL payload: UTF-8 JSON
 *
 *  2. RECORD - one normalized market event, fixed 88 bytes so that the browser
 *              can address the ring buffer by index with zero parsing.
 *
 * Every field is little endian.  Float64 is used for ids and prices: Binance
 * update ids are ~1.17e13 (< 2^53) so they are represented exactly, and f64
 * gives ~0.5 microsecond resolution on millisecond timestamps near 1.79e12,
 * which is why high precision timing uses the monotonic microsecond field.
 */

export const RECORD_BYTES = 88;
/** Number of f64 slots in a record (offset 8 .. 88). */
export const RECORD_F64 = 10;

/** Byte offset of the first f64 field inside a record. */
export const REC_F64_OFFSET = 8;

/**
 * Normalized event kinds.  These are derived from real Binance USDⓈ-M futures
 * streams; DEPTH events are expanded one-per-changed-price-level so that the
 * firehose shows the true level-change rate rather than the message rate.
 */
export const KIND = Object.freeze({
  DEPTH: 1,
  TRADE: 2,
  BOOK_TICKER: 3,
  MARK_PRICE: 4,
  LIQUIDATION: 5,
  SNAPSHOT_BEGIN: 6,
  SNAPSHOT_LEVEL: 7,
  SNAPSHOT_END: 8,
  GAP: 9,
  RESET: 10,
  RAW_MSG: 11,
});

export const KIND_LABEL = Object.freeze([
  '', 'DEPTH', 'TRADE', 'BOOKTICKER', 'MARK', 'LIQUIDATION',
  'SNAP-BEGIN', 'SNAP-LVL', 'SNAP-END', 'GAP', 'RESET', 'RAW',
]);

/** Short labels used by the firehose, padded to a fixed column width. */
export const KIND_FIREHOSE = Object.freeze([
  '', 'DEPTH', 'TRADE', 'BOOKTKR', 'MARK', 'LIQUID',
  'SNAP', 'SNAPLVL', 'SNAPEND', 'GAP', 'RESET', 'RAW',
]);

export const SIDE = Object.freeze({ BID: 0, ASK: 1, NONE: 2 });
export const SIDE_LABEL = Object.freeze(['BID', 'ASK', '--']);

export const FLAG = Object.freeze({
  /** First level-change derived from a given WebSocket message. */
  MSG_FIRST: 1,
  /** Record came from a recording being replayed, not from the live socket. */
  REPLAY: 2,
  /** Value produced by a model, not observed. Must never be shown as real. */
  SYNTHETIC: 4,
  /** Stale / duplicate update that was correctly discarded. */
  STALE: 8,
  /** Emitted as part of a resynchronization. */
  RESYNC: 16,
  /** Trade where the buyer was the maker (Binance `m` field). */
  TRADE_BUYER_MAKER: 32,
});

export const FRAME = Object.freeze({ EVENTS: 1, CONTROL: 2 });

/** FRAME.CONTROL message types. */
export const CONTROL = Object.freeze({
  HELLO: 'hello',
  METRICS: 'metrics',
  HEALTH: 'health',
  REPLAY_START: 'replay-start',
  REPLAY_END: 'replay-end',
  RECORD_STATE: 'record-state',
  SERVER_NOTE: 'note',
});

/** Reasons a RESET record may be emitted. */
export const RESET_REASON = Object.freeze({
  SNAPSHOT: 0,
  RESYNC: 1,
  RECONNECT: 2,
  REPLAY: 3,
});

/* ------------------------------------------------------------------ codec */

/**
 * @param {DataView} dv
 * @param {number} off
 * @param {object} r
 */
export function writeRecord(dv, off, r) {
  dv.setUint8(off + 0, r.kind | 0);
  dv.setUint8(off + 1, r.side | 0);
  dv.setUint16(off + 2, r.flags | 0, true);
  dv.setUint32(off + 4, r.rawBytes | 0, true);
  dv.setFloat64(off + 8, r.seq, true);
  dv.setFloat64(off + 16, r.updateId ?? 0, true);
  dv.setFloat64(off + 24, r.exchangeTs ?? 0, true);
  dv.setFloat64(off + 32, r.localRecvMs ?? 0, true);
  dv.setFloat64(off + 40, r.localMonoUs ?? 0, true);
  dv.setFloat64(off + 48, r.latencyUs ?? 0, true);
  dv.setFloat64(off + 56, r.price ?? 0, true);
  dv.setFloat64(off + 64, r.qty ?? 0, true);
  dv.setFloat64(off + 72, r.delta ?? 0, true);
  dv.setFloat64(off + 80, r.extra ?? 0, true);
}

/**
 * @param {DataView} dv
 * @param {number} off
 * @param {object} [out] reuse an object to avoid allocation on hot paths
 */
export function readRecord(dv, off, out = {}) {
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

/** Total bytes of a FRAME.EVENTS frame carrying `count` records. */
export function eventsFrameBytes(count) {
  return 5 + 4 + count * RECORD_BYTES;
}

/**
 * Streaming frame writer.  Owns a growable buffer; `push()` appends records,
 * `finish()` returns a subarray ready to be written to a socket or a file.
 */
export class FrameWriter {
  constructor(maxEvents = 8192) {
    this.capacity = maxEvents;
    this.buf = new Uint8Array(eventsFrameBytes(maxEvents));
    this.dv = new DataView(this.buf.buffer);
    this.reset();
  }

  reset() {
    this.count = 0;
    this.dv.setUint8(0, FRAME.EVENTS);
    // length + count are patched in finish()
    return this;
  }

  /** @param {object} r record object as accepted by writeRecord */
  push(r) {
    writeRecord(this.dv, 9 + this.count * RECORD_BYTES, r);
    this.count++;
    return this.count < this.capacity;
  }

  get full() {
    return this.count >= this.capacity;
  }

  finish() {
    const len = 4 + this.count * RECORD_BYTES;
    this.dv.setUint32(1, len, true);
    this.dv.setUint32(5, this.count, true);
    return this.buf.subarray(0, 5 + len);
  }
}

const textEncoder = new TextEncoder();

/** Encode a FRAME.CONTROL JSON message. */
export function encodeControl(type, payload = {}) {
  const json = JSON.stringify({ type, ...payload });
  const body = textEncoder.encode(json);
  const out = new Uint8Array(5 + body.length);
  const dv = new DataView(out.buffer);
  dv.setUint8(0, FRAME.CONTROL);
  dv.setUint32(1, body.length, true);
  out.set(body, 5);
  return out;
}
