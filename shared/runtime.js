/**
 * The handful of host differences the pipeline has to care about.
 *
 * The ingestion pipeline is written once and runs in two places: as a Node
 * process, and inside a browser worker (so the observer works as a static site
 * with no backend).  Everything else in the pipeline is plain JS; only the
 * monotonic clock and the "run this soon, but batched" primitive differ.
 */

const HAS_HRTIME =
  typeof process !== 'undefined' &&
  process.hrtime &&
  typeof process.hrtime.bigint === 'function';

const HAS_PERF =
  typeof performance !== 'undefined' && typeof performance.now === 'function';

export const isNode = HAS_HRTIME && typeof window === 'undefined' && typeof self === 'undefined';

/**
 * Monotonic nanoseconds.
 *
 * Durations are only ever computed by subtracting two values from this
 * function, so the absolute origin does not matter - what matters is that it
 * never goes backwards.
 */
export function nowNs() {
  if (HAS_HRTIME) return process.hrtime.bigint();
  if (HAS_PERF) return BigInt(Math.round(performance.now() * 1e6));
  return BigInt(Date.now()) * 1000000n;
}

/** True when `performance.now()` itself is a fine-grained monotonic source. */
export function hasHighResPerf() {
  return HAS_PERF;
}

/**
 * Empirically measure the resolution of the monotonic clock, in microseconds.
 *
 * This matters most in the static build, where the pipeline's timestamps come
 * from the browser's `performance.now()`.  Browsers coarsen that clock to blunt
 * Spectre-style timing attacks, and the clamp differs by browser and by whether
 * the page is cross-origin isolated.  If the clock only ticks every 100 us then
 * the sub-100 us bins of the inter-arrival histogram are measuring the timer,
 * not the market - so the number is measured here and shown, rather than
 * assumed to be fine.
 *
 * @returns {number} microseconds (0.001 for process.hrtime, which is ns-based)
 */
export function measureTimerResolutionUs() {
  if (HAS_HRTIME) return 0.001;
  if (!HAS_PERF) return 1000;
  let min = Infinity;
  let prev = performance.now();
  for (let i = 0; i < 200000; i++) {
    const now = performance.now();
    const d = now - prev;
    if (d > 0 && d < min) min = d;
    prev = now;
  }
  return Number.isFinite(min) ? min * 1000 : 1000;
}

/**
 * Schedule work as soon as the current batch of input is drained.
 *
 * The pipeline uses this to coalesce records into one frame per turn of the
 * host loop: everything that arrived during one websocket callback burst
 * becomes a single frame instead of one frame per record.  In Node that is
 * exactly `setImmediate`.  In a browser `setImmediate` does not exist, and
 * `setTimeout(fn, 0)` is clamped to ~4 ms once nested - which is still a fine
 * frame cadence here (~250 frames/s), and far better than flushing per record.
 */
export function scheduleBatch(fn) {
  if (typeof setImmediate === 'function') {
    setImmediate(fn);
  } else {
    setTimeout(fn, 0);
  }
}

/** A timeout signal that is an AbortSignal on both platforms. */
export function timeoutSignal(ms) {
  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function') {
    return AbortSignal.timeout(ms);
  }
  const ctrl = new AbortController();
  setTimeout(() => ctrl.abort(new Error('timeout')), ms);
  return ctrl.signal;
}
