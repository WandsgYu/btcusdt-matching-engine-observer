/**
 * Ingest worker.
 *
 * Owns the network read and the binary decode so that neither can ever be
 * delayed by rendering.  It runs in one of two modes, chosen by the page:
 *
 *   mode=server      (default) reads the byte stream from the observer's Node
 *                    process over `GET /api/stream`.
 *   mode=standalone  runs the *entire* ingestion pipeline in this worker -
 *                    connecting to Binance directly and fetching the depth
 *                    snapshot from the public REST API - so the observer works
 *                    as a static site with no backend.  It emits exactly the
 *                    same byte stream the server would have emitted, which is
 *                    why the render thread cannot tell the difference.
 *
 * Two ring strategies, forced by the browser:
 *
 *   shared    the page is cross-origin isolated (the Node server sends
 *             COOP/COEP), so this worker writes records straight into a
 *             SharedArrayBuffer the render loop reads with no copying.
 *   fallback  no SharedArrayBuffer.  Raw chunks are transferred to the render
 *             thread, which decodes into its own ring with the same decoder.
 *
 * Nothing here paces, throttles, samples or smooths the feed.  Whatever the
 * socket delivers is decoded and stored, at whatever rate it arrives.
 */

import { EventRing, DEFAULT_CAPACITY } from './ring.js';
import { decodeChunk, joinChunks } from './decode.js';
import { CONTROL, encodeControl } from './shared/protocol.js';
import { measureTimerResolutionUs } from './shared/runtime.js';

const params = new URLSearchParams(self.location.search);
const STANDALONE = params.get('mode') === 'standalone';
const capacity = Math.max(1 << 14, Number(params.get('ring')) || DEFAULT_CAPACITY);

// SharedArrayBuffer needs cross-origin isolation. In the static build that is
// arranged by coi-serviceworker.js (Pages cannot send COOP/COEP itself); if the
// browser refuses, this falls back to transferring chunks and the render thread
// decodes with the same decoder.
const canShare =
  typeof SharedArrayBuffer !== 'undefined' &&
  typeof crossOriginIsolated !== 'undefined' &&
  crossOriginIsolated === true;

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
const ring = canShare ? new EventRing(capacity, { shared: true }) : null;

postMessage({
  type: 'ready',
  shared: canShare,
  capacity,
  standalone: STANDALONE,
  buffer: canShare ? ring.buffer : null,
});

/* ══════════════════════════ delivery to render thread ═══════════════════ */

let rest = EMPTY;

function onControl(msg) {
  stats.controls++;
  postMessage({ type: 'control', msg, ringIndex: ring ? ring.writeIndex : -1 });
}

/**
 * Push one encoded frame towards the render thread.
 *
 * In shared mode the frame is decoded here, synchronously, while the pipeline's
 * reusable frame buffer is still valid.  In fallback mode it has to be copied
 * before transfer, both because the pipeline reuses its buffer and because a
 * transferred ArrayBuffer would be detached from under it.
 */
function deliver(frame) {
  if (!frame || frame.length === 0) return;
  if (canShare) {
    const chunk = joinChunks(rest, frame);
    const used = decodeChunk(chunk, ring, onControl, stats);
    rest = used < chunk.length ? chunk.subarray(used) : EMPTY;
  } else {
    const copy = frame.slice();
    postMessage({ type: 'chunk', buf: copy.buffer }, [copy.buffer]);
  }
}

/* ═════════════════════════════ mode: server stream ══════════════════════ */

let controller = null;
let aborted = false;

async function connectToServer() {
  aborted = false;
  controller = new AbortController();
  while (!aborted) {
    try {
      const res = await fetch('./api/stream', { signal: controller.signal, cache: 'no-store' });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      postMessage({ type: 'status', state: 'streaming' });
      const reader = res.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        stats.chunks++;
        stats.bytes += value.length;
        deliver(value);
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

/* ══════════════════════════ mode: standalone ════════════════════════════ */

/** In-memory recording, so REC/REPLAY work without a filesystem. */
const memRecorder = {
  active: null,
  last: null,
  list: [],
  state() {
    return {
      recording: !!this.active,
      file: this.active ? this.active.name : null,
      startedAt: this.active ? this.active.startedAt : null,
      endsAt: this.active ? this.active.startedAt + this.active.limitMs : null,
      limitMs: this.active ? this.active.limitMs : null,
      bytes: this.active ? this.active.bytes : 0,
      frames: this.active ? this.active.frames : 0,
      lastFile: this.last,
      directory: 'in-memory (static build: no filesystem)',
    };
  },
};

let pipeline = null;
let metricsSeq = 0;
let metricsTimer = null;
let replaying = false;
const log = [];

function pushLog(level, text) {
  log.unshift({ level, text, wallMs: Date.now() });
  if (log.length > 200) log.pop();
}

async function startStandalone() {
  // The clock this build timestamps everything with, and how fine it actually is.
  const timingResolutionUs = measureTimerResolutionUs();
  const [{ Pipeline }, { buildMetricsPayload }] = await Promise.all([
    import('./core/pipeline.mjs'),
    import('./shared/metrics-payload.js'),
  ]);

  pipeline = new Pipeline({});

  pipeline.on('note', (n) => {
    pushLog(n.level, n.text);
    if (n.level !== 'info') postMessage({ type: 'control', msg: { type: 'note', ...n }, ringIndex: -1 });
  });
  pipeline.on('feed-state', (s) => {
    pushLog('info', `feed ${s.state}${s.detail ? ` (${s.detail})` : ''}`);
    postMessage({ type: 'control', msg: { type: 'feed-state', ...s }, ringIndex: -1 });
  });
  pipeline.on('feed-error', (e) => pushLog('warn', `feed error: ${e}`));

  // The pipeline emits the same encoded frames the server would have sent.
  pipeline.on('frames', (frame) => {
    if (memRecorder.active) writeFrame(frame);
    if (replaying) return;
    deliver(frame);
  });

  const snapshotFrames = () => pipeline.buildClientSnapshotFrame();

  function writeFrame(frame) {
    const a = memRecorder.active;
    if (!a) return;
    const copy = frame.slice();
    a.frames.push(copy);
    a.bytes += copy.length;
  }

  function startRecording(seconds) {
    stopRecording();
    const limitMs = Math.max(1000, Math.min(3600, Number(seconds) || 30) * 1000);
    const startedAt = Date.now();
    const name = `btcusdt-${new Date(startedAt).toISOString().replace(/[-:]/g, '').replace(/\..+/, '').replace('T', '-')}.binlog`;
    memRecorder.active = { name, startedAt, limitMs, frames: [], bytes: 0, timer: null };
    writeFrame(
      encodeControl('recording-header', {
        symbol: pipeline.symbol,
        startedAt,
        limitMs,
        streams: pipeline.feed.streams,
        depthLimit: pipeline.depthLimit,
        serverNote:
          'Frames are the unmodified live stream captured in the browser. Timestamps are original.',
      }),
    );
    writeFrame(snapshotFrames());
    memRecorder.active.timer = setTimeout(() => stopRecording(), limitMs);
    pushLog('info', `recording started -> ${name} (${limitMs / 1000}s, in memory)`);
    broadcastRecordState();
    return memRecorder.state();
  }

  function stopRecording() {
    const a = memRecorder.active;
    if (!a) return null;
    if (a.timer) clearTimeout(a.timer);
    memRecorder.active = null;
    const info = {
      name: a.name,
      bytes: a.bytes,
      frames: a.frames.length,
      startedAt: a.startedAt,
      durationMs: Date.now() - a.startedAt,
      events: countEvents(a.frames),
    };
    memRecorder.last = info;
    memRecorder.list.unshift(info);
    if (memRecorder.list.length > 20) memRecorder.list.pop();
    memRecorder.captured = a;
    pushLog('info', `recording stopped: ${info.name} (${info.frames} frames, ${info.bytes} bytes)`);
    broadcastRecordState();
    postRecordings();
    return info;
  }

  function countEvents(frames) {
    let n = 0;
    for (const f of frames) {
      if (f[0] !== 1) continue; // FRAME.EVENTS
      n += (f[5] | (f[6] << 8) | (f[7] << 16) | (f[8] << 24)) >>> 0;
    }
    return n;
  }

  function broadcastRecordState() {
    deliver(encodeControl(CONTROL.RECORD_STATE, memRecorder.state()));
  }

  function postRecordings() {
    postMessage({ type: 'recordings', list: memRecorder.list, state: memRecorder.state() });
  }

  async function replay(name) {
    const captured = memRecorder.captured;
    if (!captured || captured.name !== name) {
      deliver(
        encodeControl(CONTROL.SERVER_NOTE, {
          level: 'error',
          text: `replay failed: ${name} is not in memory (the static build keeps recordings in memory only)`,
        }),
      );
      return;
    }
    replaying = true;
    deliver(
      encodeControl(CONTROL.REPLAY_START, {
        file: name,
        frames: captured.frames.length,
        bytes: captured.bytes,
        serverNote:
          'Original captured frames, byte for byte, replayed from memory. Timestamps and measured latencies are the values recorded at capture time.',
      }),
    );
    for (const f of captured.frames) deliver(f);
    deliver(encodeControl(CONTROL.REPLAY_END, { file: name, frames: captured.frames.length }));
    replaying = false;
    deliver(snapshotFrames());
    deliver(
      encodeControl(CONTROL.SERVER_NOTE, {
        level: 'warn',
        text: 'live stream resumed with a fresh book snapshot; events that arrived during replay were skipped for this display',
      }),
    );
    pushLog('info', `replay of ${name} delivered (${captured.frames.length} frames)`);
  }

  self.addEventListener('message', (ev) => {
    const m = ev.data || {};
    if (m.type === 'record') startRecording(m.seconds || 30);
    else if (m.type === 'record-stop') stopRecording();
    else if (m.type === 'replay') replay(m.file);
    else if (m.type === 'list-recordings') postRecordings();
  });

  pipeline.start();

  deliver(
    encodeControl(CONTROL.HELLO, {
      clientId: 1,
      symbol: pipeline.symbol,
      streams: pipeline.feed.streams,
      depthLimit: pipeline.depthLimit,
      serverStartedAt: Date.now(),
      recordBytes: 88,
      protocolVersion: 1,
      standalone: true,
      timingSource: 'performance.now() in this browser',
      timingResolutionUs,
      serverNote:
        'STATIC BUILD: the whole ingestion pipeline is running in this browser worker, connected directly to Binance. Event timing is measured here with performance.now(), not on a server.',
    }),
  );
  deliver(snapshotFrames());

  const emitMetrics = () => {
    metricsSeq++;
    deliver(
      encodeControl(
        CONTROL.METRICS,
        buildMetricsPayload(pipeline, {
          seq: metricsSeq,
          recorder: memRecorder.state(),
          clients: [],
          log,
        }),
      ),
    );
  };
  emitMetrics();
  metricsTimer = setInterval(emitMetrics, 100);
  setInterval(() => postMessage({ type: 'stats', stats: { ...stats } }), 500);
  postRecordings();
  pushLog('info', 'standalone pipeline started inside the browser worker; connecting to Binance directly');
}

/* ══════════════════════════════════ boot ════════════════════════════════ */

if (STANDALONE) {
  startStandalone().catch((err) => {
    postMessage({ type: 'status', state: 'error', message: String((err && err.message) || err) });
    deliver(
      encodeControl(CONTROL.SERVER_NOTE, {
        level: 'error',
        text: `standalone pipeline failed to start: ${(err && err.message) || err}`,
      }),
    );
  });
} else {
  self.addEventListener('message', (ev) => {
    if (ev.data && ev.data.type === 'reconnect') {
      aborted = true;
      if (controller) controller.abort();
      setTimeout(() => connectToServer(), 60);
    }
  });
  connectToServer();
}
