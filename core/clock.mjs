/**
 * Clock services.
 *
 * Two distinct time bases are used, and mixing them up would quietly destroy
 * the meaning of every latency number in the UI:
 *
 *  - MONOTONIC is the only source used for durations: inter-arrival gaps,
 *    processing latency.  It can never jump backwards.  Backed by
 *    `process.hrtime.bigint()` in Node and `performance.now()` in a browser.
 *
 *  - WALL (Date.now) is only used to express "when did this happen in civil
 *    time" so that it can be compared with Binance's exchange timestamp.
 *    Date.now() is coarse (~1 ms), so it is *interpolated* with the monotonic
 *    clock to recover sub-millisecond resolution while staying monotonic.
 *
 * The interpolation anchor is periodically re-synced against Date.now() so the
 * derived wall clock cannot drift away from civil time over a long session.
 */

import { nowNs, timeoutSignal } from '../shared/runtime.js';

const T0_MONO_NS = nowNs();

/** Nanoseconds from a monotonic clock. */
export function monoNs() {
  return nowNs();
}

/** Microseconds since start (monotonic, sub-microsecond float). */
export function monoUs() {
  return Number(nowNs() - T0_MONO_NS) / 1000;
}

let anchorMonoNs = nowNs();
let anchorWallMs = Date.now();

/**
 * Millisecond wall clock with sub-millisecond resolution, monotonic between
 * re-syncs.  Never returns a value lower than a previously returned value.
 */
let lastWallMs = 0;
export function wallMsHiRes() {
  const nowMono = nowNs();
  let v = anchorWallMs + Number(nowMono - anchorMonoNs) / 1e6;
  if (v < lastWallMs) v = lastWallMs;
  lastWallMs = v;
  return v;
}

/** Re-pin the interpolation anchor to civil time. Returns the correction (ms). */
export function resyncWallAnchor() {
  const before = wallMsHiRes();
  anchorWallMs = Date.now();
  anchorMonoNs = nowNs();
  lastWallMs = 0;
  return anchorWallMs - before;
}

/** Wall clock milliseconds, integer. */
export function wallMs() {
  return Date.now();
}

export function startMonoNs() {
  return T0_MONO_NS;
}

/* --------------------------------------------------- exchange clock offset */

/**
 * Estimated offset between the local civil clock and Binance's server clock.
 *
 * This is a *measurement of our own clock*, not of Binance's internals.  It is
 * needed because `exchange -> local` latency computed from timestamps is
 * contaminated by clock skew: if our clock is 40 ms behind, every latency
 * reading is inflated by 40 ms.  Reporting the estimate - and its uncertainty,
 * which is half the round trip - lets the reader judge the number instead of
 * trusting it blindly.
 *
 * Uses the NTP-style midpoint estimator over a REST round trip:
 *     offset ~= serverTime - (t_send + t_recv) / 2
 * and keeps the sample from the lowest-RTT round trip seen so far, which is
 * the standard way to reduce the influence of queueing delay.
 */
export const clockOffset = {
  /** serverTime - localTime, milliseconds. NaN until first successful probe. */
  offsetMs: NaN,
  /** Round trip time of the best sample, milliseconds. */
  rttMs: NaN,
  attempts: 0,
  successes: 0,
  lastProbeWallMs: 0,
  lastError: null,
  history: [],
};

export async function probeClockOffset(fetchImpl = fetch, base = 'https://fapi.binance.com') {
  clockOffset.attempts++;
  const t0 = wallMsHiRes();
  try {
    const res = await fetchImpl(`${base}/fapi/v1/time`, { signal: timeoutSignal(5000) });
    const t1 = wallMsHiRes();
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    const serverTime = Number(body.serverTime);
    if (!Number.isFinite(serverTime)) throw new Error('bad serverTime');
    const rtt = t1 - t0;
    const offset = serverTime - (t0 + t1) / 2;
    clockOffset.successes++;
    clockOffset.lastProbeWallMs = Date.now();
    clockOffset.lastError = null;
    clockOffset.history.push({ rtt, offset });
    if (clockOffset.history.length > 32) clockOffset.history.shift();
    if (!Number.isFinite(clockOffset.rttMs) || rtt <= clockOffset.rttMs) {
      clockOffset.offsetMs = offset;
      clockOffset.rttMs = rtt;
    }
    return { rtt, offset };
  } catch (err) {
    clockOffset.lastError = String(err && err.message ? err.message : err);
    clockOffset.lastProbeWallMs = Date.now();
    return null;
  }
}
