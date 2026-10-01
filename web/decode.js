/**
 * Stream framing decoder, shared by the ingest worker (shared-memory mode) and
 * the main thread (fallback mode).
 *
 * The server writes [u8 frameType][u32 length][payload] and payloads arrive
 * split across arbitrary chunk boundaries, so the decoder reports how many
 * bytes it consumed and lets the caller keep the remainder.
 */

import { FRAME, RECORD_BYTES } from '/shared/protocol.js';

const decoder = new TextDecoder();

/**
 * Decode as many whole frames as possible from `u8`.
 *
 * @returns {number} bytes consumed; the caller must retain `u8.subarray(n)`.
 */
export function decodeChunk(u8, ring, onControl, stats) {
  let off = 0;
  const n = u8.length;
  while (n - off >= 5) {
    const view = new DataView(u8.buffer, u8.byteOffset + off, 5);
    const type = view.getUint8(0);
    const len = view.getUint32(1, true);
    if (len < 0 || n - off < 5 + len) break;
    const payloadStart = off + 5;
    if (type === FRAME.EVENTS) {
      const payload = u8.subarray(payloadStart, payloadStart + len);
      const pdv = new DataView(payload.buffer, payload.byteOffset, len);
      const count = pdv.getUint32(0, true);
      if (len === 4 + count * RECORD_BYTES) {
        ring.appendRecords(payload, 4, count);
        stats.records += count;
      } else {
        stats.badFrames++;
      }
      stats.frames++;
    } else if (type === FRAME.CONTROL) {
      try {
        onControl(JSON.parse(decoder.decode(u8.subarray(payloadStart, payloadStart + len))));
      } catch {
        stats.badFrames++;
      }
    } else {
      stats.badFrames++;
    }
    off = payloadStart + len;
  }
  return off;
}

/** Concatenate a retained remainder with a newly arrived chunk. */
export function joinChunks(rest, chunk) {
  if (!rest || rest.length === 0) return chunk;
  const merged = new Uint8Array(rest.length + chunk.length);
  merged.set(rest, 0);
  merged.set(chunk, rest.length);
  return merged;
}
