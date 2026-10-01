/**
 * Focused check of the sliding-window bucketing.
 *
 * The bucket ring is stamped with a *generation* (bucket index / ring length)
 * rather than the raw bucket index, because the raw index is ~3.6e10 and would
 * silently overflow the Int32Array the stamps live in - every comparison would
 * fail and every window sum would reset the buckets it read.
 */
import { Metrics } from '../core/metrics.mjs';

const m = new Metrics();
const t0 = 1790837000000;
let t = t0;
for (let i = 0; i < 80; i++) {
  t += 38;
  m.onDepthMessage(t, 5, 5);
}
const snap = m.snapshot(t);
let fails = 0;
const check = (name, ok, detail) => {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  ' + detail : ''}`);
  if (!ok) fails++;
};

check('100 ms window is non-zero under continuous input', snap.rates.events.w100ms > 0, `w100ms=${snap.rates.events.w100ms}`);
check('windows are monotonically ordered', snap.rates.events.w10s <= snap.rates.events.w1s && snap.rates.events.w1s <= snap.rates.events.w100ms,
  `100ms=${snap.rates.events.w100ms} 1s=${snap.rates.events.w1s} 10s=${snap.rates.events.w10s}`);
check('windowSum(100ms) counts exactly the last two buckets', m.windowSum(0, 100, t) === 15, `got ${m.windowSum(0, 100, t)}`);
check('windowSum(1s) counts the last twenty buckets', m.windowSum(0, 1000, t) === 135, `got ${m.windowSum(0, 1000, t)}`);
check('a stale slot is not counted after the ring wraps', (() => {
  // Advance 65 s so every slot has been recycled, then write one event.
  const later = t + 65000;
  m.onDepthMessage(later, 3, 3);
  return m.windowSum(0, 100, later) === 3;
})(), 'slot recycling');
process.exit(fails ? 1 : 0);
