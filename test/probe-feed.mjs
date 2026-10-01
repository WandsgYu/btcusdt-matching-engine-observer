/**
 * Standalone probe of the Binance USDⓈ-M futures WebSocket: which streams
 * actually deliver, at what rate, and how bursty the arrival process is.
 *
 * Run before trusting any throughput claim: `npm run probe`.
 */
const STREAMS = [
  'btcusdt@depth@0ms',
  'btcusdt@depth@100ms',
  'btcusdt@trade',
  'btcusdt@aggTrade',
  'btcusdt@bookTicker',
  'btcusdt@markPrice@1s',
  '!forceOrder@arr',
  'btcusdt@depth20@100ms',
];
const SECONDS = Number(process.argv[2] || 10);

for (const name of STREAMS) {
  await new Promise((resolve) => {
    const url = `wss://fstream.binance.com/ws/${name}`;
    let msgs = 0;
    let bytes = 0;
    let levels = 0;
    const gaps = [];
    let last = process.hrtime.bigint();
    let err = null;
    const ws = new WebSocket(url);
    ws.onerror = (e) => (err = e.message || e.type);
    ws.onmessage = (ev) => {
      const now = process.hrtime.bigint();
      gaps.push(Number(now - last) / 1000);
      last = now;
      msgs++;
      const s = String(ev.data);
      bytes += s.length;
      try {
        const j = JSON.parse(s);
        if (j.b) levels += j.b.length;
        if (j.a) levels += j.a.length;
      } catch { /* ignore */ }
    };
    setTimeout(() => {
      try { ws.close(); } catch { /* ignore */ }
      gaps.sort((a, b) => a - b);
      const q = (p) => (gaps.length ? gaps[Math.floor(gaps.length * p)].toFixed(0) : '-');
      console.log(
        `${name.padEnd(20)} msg=${String(msgs).padStart(6)} (${(msgs / SECONDS).toFixed(1)}/s)  ` +
        `levels=${String(levels).padStart(7)} (${(levels / SECONDS).toFixed(0)}/s)  ` +
        `${(bytes / SECONDS / 1024).toFixed(1)} KiB/s  ` +
        `gap p50=${q(0.5)}µs p99=${q(0.99)}µs max=${(gaps.at(-1) || 0).toFixed(0)}µs` +
        (err ? `  ERR=${err}` : ''),
      );
      resolve();
    }, SECONDS * 1000);
  });
}
process.exit(0);
