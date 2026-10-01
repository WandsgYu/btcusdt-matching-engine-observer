/**
 * The JSON metrics payload pushed to clients ~10 times a second.
 *
 * Lives in `shared/` because both hosts produce it: the Node server streams it
 * over the HTTP stream, and the static build produces it inside the browser
 * worker.  Identical numbers from identical code, whichever way the app runs.
 */

/** Round numbers so the 10 Hz JSON payload stays small. */
export function tidy(v, depth = 0) {
  if (v === null || v === undefined) return v;
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) return null;
    const r = Math.round(v * 1000) / 1000;
    return Object.is(r, -0) ? 0 : r;
  }
  if (Array.isArray(v)) {
    if (depth > 3) return v.length;
    return v.map((x) => tidy(x, depth + 1));
  }
  if (typeof v === 'object') {
    if (depth > 3) return undefined;
    const out = {};
    for (const k in v) {
      const t = tidy(v[k], depth + 1);
      if (t !== undefined) out[k] = t;
    }
    return out;
  }
  return v;
}

/**
 * @param {import('../server/pipeline.mjs').Pipeline} pipeline
 * @param {{seq:number, recorder?:object, clients?:Array, log?:Array}} opts
 */
export function buildMetricsPayload(pipeline, opts = {}) {
  const st = pipeline.status();
  return tidy({
    seq: opts.seq ?? 0,
    serverWallMs: Date.now(),
    ...st.metrics,
    feed: {
      state: st.feed.state,
      reconnects: st.feed.reconnects,
      attempts: st.feed.attempts,
      lastError: st.feed.lastError,
      streams: st.feed.streams,
    },
    integrity: {
      armed: st.armed,
      bookSynced: st.synced,
      bookBids: st.book.bids,
      bookAsks: st.book.asks,
      lastUpdateId: st.book.lastUpdateId,
      applied: st.book.stats.applied,
      discardedStale: st.book.stats.discardedStale,
      gaps: st.book.stats.gaps,
      holes: st.book.stats.holes,
      resyncs: st.book.stats.resyncs,
      insertions: st.book.stats.insertions,
      deletions: st.book.stats.deletions,
      bufferedFrames: st.sequencer.bufferedFrames,
      depthDiscards: st.sequencer.depthDiscards,
      unknownMessages: st.sequencer.unknownMessages,
      markersFiltered: st.metrics.counters.markers,
      snapshotPending: st.snapshot.pending,
      snapshotAttempts: st.snapshot.attempts,
      snapshotResyncs: st.snapshot.resyncs,
      snapshotRttMs: st.snapshot.lastRttMs,
      snapshotError: st.snapshot.lastError,
      syncMisses: st.sequencer.syncMisses,
      replayedFrames: st.sequencer.replayedFrames,
    },
    clock: st.clock,
    loopLag: st.loopLag,
    recorder: opts.recorder || { recording: false, file: null, directory: null, lastFile: null },
    sequencer: { seq: st.sequencer.seq, framesSent: st.framesSent, recordsSent: st.recordsSent },
    clients: (opts.clients || []).map((c) => ({
      id: c.id,
      mode: c.mode,
      queueBytes: c.queueBytes,
      droppedFrames: c.droppedFrames,
    })),
    log: (opts.log || []).slice(0, 6),
  });
}
