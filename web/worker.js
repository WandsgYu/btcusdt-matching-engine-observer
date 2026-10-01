/**
 * Ingest worker.
 *
 * Owns the network read and the binary decode so that neither can ever be
 * delayed by rendering.  Two modes:
 *
 *   shared    the page is cross-origin isolated, so the worker writes records
 *             straight into a SharedArrayBuffer ring that the render loop reads
 *             with no copying and no message passing on the hot path.
 *   fallback  no SharedArrayBuffer; the worker forwards raw chunks to the main
 *             thread, which decodes into its own ring.  Same decoder, same
 *             record layout; only buffer ownership differs.
 *
 * Nothing here paces, throttles, samples or smooths the feed.  Whatever the
 * socket delivers is decoded and stored, at whatever rate it arrives.
 */

import { EventRing, DEFAULT_CAPACITY } from './ring.js';
import { decodeChunk, joinChunks } from './decode.js';

const canShare =
  typeof SharedArrayBuffer !== 'undefined' &&
  typeof crossOriginIsolated !== 'undefined' &&
  crossOriginIsolated === true;

const params = new URLSearchParams(self.location.search);
const capacity = Math.max(1 << 14, Number(params.get('ring')) || DEFAULT_CAPACITY);

const stats = {
  frames: 0,
  records: 0,
  badFrames: 0,
  chunks: 0,
  bytes: 0,
  controls: 0,
  reconnects: 0,
};

const EMPTY = new Uint8Array(0);

// Only allocate the ring where the worker actually writes into it.
const ring = canShare ? new EventRing(capacity, { shared: true }) : null;

postMessage({
  type: 'ready',
  shared: canShare,
  capacity,
  buffer: canShare ? ring.buffer : null,
});

let rest = EMPTY;
let controller = null;
let aborted = false;

function onControl(msg) {
  stats.controls++;
  postMessage({ type: 'control', msg });
}

async function connect() {
  aborted = false;
  controller = new AbortController();
  while (!aborted) {
    try {
      const res = await fetch('/api/stream', { signal: controller.signal, cache: 'no-store' });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      postMessage({ type: 'status', state: 'streaming' });
      const reader = res.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        stats.chunks++;
        stats.bytes += value.length;
        const chunk = joinChunks(rest, value);
        if (canShare) {
          const used = decodeChunk(chunk, ring, onControl, stats);
          rest = used < chunk.length ? chunk.subarray(used) : EMPTY;
        } else {
          // Hand the bytes to the thread that owns the ring. slice() gives an
          // exactly-sized, zero-offset buffer that can be transferred.
          rest = EMPTY;
          const copy = chunk.slice();
          postMessage({ type: 'chunk', buf: copy.buffer }, [copy.buffer]);
        }
        if (stats.chunks % 30 === 0) postMessage({ type: 'stats', stats: { ...stats } });
      }
      postMessage({ type: 'stats', stats: { ...stats } });
      if (aborted) break;
      stats.reconnects++;
      postMessage({ type: 'status', state: 'reconnecting' });
    } catch (err) {
      if (aborted || err.name === 'AbortError') break;
      stats.reconnects++;
      postMessage({ type: 'status', state: 'error', message: String(err.message || err) });
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
}

self.onmessage = (ev) => {
  if (ev.data && ev.data.type === 'reconnect') {
    aborted = true;
    if (controller) controller.abort();
    setTimeout(() => connect(), 60);
  }
};

connect();
