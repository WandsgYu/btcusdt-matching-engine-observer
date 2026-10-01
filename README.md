# BTCUSDT PERP · MATCHING ENGINE OBSERVER

A real-time observer for the Binance USDⓈ-M **BTCUSDT perpetual futures** market.

It is not a price terminal. It exists to make three things visible:

1. **How much is actually happening.** A live event firehose at the true arrival
   rate — a few thousand normalized events per second — not a sampled or
   smoothed summary.
2. **That a matching system is a deterministic serial state machine.** Pause,
   then advance one event at a time and watch the order book change
   `event N → book state N`, `event N+1 → book state N+1`.
3. **That a millisecond is a coarse unit of time.** Slow the playhead to
   1/1000 and one real millisecond of market activity takes a second to watch.

---

## Quick start

```bash
node server/index.mjs          # or: npm start
open http://127.0.0.1:8787
```

Requires Node 22+ (uses the built-in WebSocket client and `fetch`).
**Zero runtime dependencies.** Nothing to install, nothing to build.

Options (environment variables):

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8787` | HTTP port |
| `HOST` | `127.0.0.1` | bind address |
| `METRICS_HZ` | `10` | how often the server pushes aggregate metrics |

Ring size is a URL parameter: `?ring=2097152` for a 2M-event ring (176 MiB),
`?ring=262144` for 256K (22 MiB). Default is 1M events (88 MiB).

---

## Architecture

```
                    Binance USDⓈ-M futures WebSocket
             btcusdt@depth@0ms · @trade · @bookTicker · @markPrice@1s
                                  │
                         ┌────────▼────────┐
                         │  feed.mjs       │  timestamps captured as the FIRST
                         │  (ingress)      │  statement of the message callback
                         └────────┬────────┘
                                  │ raw frame + recvMonoUs + recvWallMs + bytes
                         ┌────────▼────────┐
                         │  ingress buffer │  held while the book is not provably
                         │                 │  in sync; replayed in arrival order
                         └────────┬────────┘
                         ┌────────▼────────┐
                         │  normalizer     │  JSON → 88-byte typed records
                         │                 │  depthUpdate expands per price level
                         └────────┬────────┘
                         ┌────────▼────────┐
                         │  sequencer      │  monotonic local serial per event
                         └────────┬────────┘
                         ┌────────▼────────┐
                         │  orderbook.mjs  │  L2 state machine, validated against
                         │                 │  the exchange's own `pu` hash chain
                         └────────┬────────┘
                         ┌────────▼────────┐
                         │  metrics.mjs    │  sliding windows, exact percentiles,
                         │                 │  inter-arrival histogram, bursts
                         └────────┬────────┘
                    ┌─────────────┴─────────────┐
                    │                           │
           ┌────────▼────────┐         ┌────────▼────────┐
           │  .binlog        │         │  HTTP chunked   │  binary frames,
           │  recorder       │         │  /api/stream    │  backpressure-aware
           └─────────────────┘         └────────┬────────┘
                                                │
                         ═══════ process boundary ═══════
                                                │
                                    ┌───────────▼───────────┐
                                    │  worker.js            │  fetch reader +
                                    │  (ingest worker)      │  binary decode
                                    └───────────┬───────────┘
                                                │ SharedArrayBuffer ring
                                                │ (Atomics cursor, zero copy)
                                    ┌───────────▼───────────┐
                                    │  app.js               │  playback cursor +
                                    │  (render, 60 fps)     │  canvas painting
                                    └───────────────────────┘
```

**Data processing and rendering are fully decoupled.** The server never waits on
the browser, and the browser never blocks the ingest path: the worker writes
records into a `SharedArrayBuffer` ring and the render loop reads it. A slow
frame costs a frame — it cannot cost an event.

The invariant that makes single-stepping real:

> Every `DEPTH` record that leaves the pipeline was applied to the authoritative
> book *before* it was emitted, and records leave in exactly the order they were
> applied.

The browser mirrors the same transitions, so the mirrored book at cursor `N` is
the book the server's state machine held after event `N` — during start-up,
during a resync, and during replay alike.

---

## What is measured, and how

Everything numeric on screen traces to a real WebSocket frame.

| Quantity | Source | Notes |
|---|---|---|
| Local receive time | `process.hrtime.bigint()` at socket callback entry | monotonic; **all durations** use this base |
| Wall-clock receive time | `Date.now()` interpolated against the monotonic clock | sub-ms resolution, monotonic, re-anchored every 30 s |
| Exchange event time | Binance `E` / `T` fields | never regenerated on the client |
| Processing latency | monotonic µs from callback entry to records emitted | includes `JSON.parse`, `pu`-chain validation, book application |
| Inter-arrival | differences of monotonic timestamps | two separate processes tracked: raw WS messages, and normalized events |
| Percentiles | exact sort of the last 8192 samples | not a histogram approximation |
| Window rates | 50 ms buckets, 60 s of history | windows are bucket-quantized at 50 ms |
| WS bytes | length of each frame off the socket | ASCII payloads, so length == bytes |
| Book state | REST snapshot + `pu`-chained diffs | chain breaks are detected, counted and resynchronized |

Two different inter-arrival processes are tracked because they are genuinely
different things:

* **WS MESSAGE** — one sample per message from Binance. The true arrival process
  at this machine.
* **NORMALIZED EVENT** — one sample per event after depth expansion. A single
  `depthUpdate` carrying 70 changed levels becomes 70 sub-100 µs events. Those
  tiny gaps are the *expansion*, not the network.

### What is deliberately not claimed

**Not observable from any public API**, and therefore never shown as fact:
Binance's internal sequencer queue depth, matching-engine latency, CPU
utilisation, gateway backlog, or any internal message rate.

The `SEQUENCER` panel shows this machine's local state machine and says so on
the panel. Queue depth there is *our* ingress-minus-committed depth, which is a
real, observable quantity — and it grows visibly when you slow the playhead
down, which is the entire point of the TIME DILATION experiment.

There is an optional overlay, **off by default**, that models a hypothetical
single-threaded engine. When enabled it prints its own formula and is badged
`SIMULATED — NOT BINANCE TELEMETRY`. It is never mixed into the observed panels.

`EXCHANGE → LOCAL` latency includes network transit, Binance's push mechanism
and **clock skew**. The skew is measured against Binance's server clock over
REST and displayed, along with its uncertainty (± ½ RTT), together with an
offset-corrected estimate. On a long-haul link that correction is large and the
uncertainty is large with it; the panel says so rather than picking the flattering
number.

`SERVER EVENT LOOP LAG` is displayed next to the latency figures on purpose: if
this machine stalls, every processing-latency number measured around the stall
is contaminated, and the panel says which case you are in.

---

## The three experiments

### 1 · EVENT FIREHOSE

The centre column is the live event stream, painted on canvas and batched into
`requestAnimationFrame`. At realtime it scrolls far faster than it can be read —
that is the correct outcome, and it is the honest one: roughly 1,000–4,000
normalized events per second on a normal BTCUSDT tape, spiking higher during
bursts.

Additions to the stream that would be dishonest are surfaced instead:

* `drawn` / `received` — if a catch-up frame has to skip formatting lines that
  would be off-screen anyway, the count of skipped lines is displayed.
* `scrolled past` — same counter, named for what it is.

### 2 · PAUSE + STEP — the serial state machine

Press `PAUSE`, then `STEP` (or `→`). Each click advances the sequencer by
**exactly one event**, and the order book, the `PROCESSING` panel and the
waterfall all move with it:

```
91827361 → DONE
91827362 → DONE
91827363 → PROCESSING
91827364 → WAIT
91827365 → WAIT
```

Serials beyond the ingress head are shown dimmed: the sequencer assigns serials
densely, so they are projected rather than received.

One detail worth knowing: a single `depthUpdate` message expands into ~70 level
events that all share the same exchange update id `u`. So stepping changes the
book state on nearly every click while `lastUpdateId` only moves once per
message. Both numbers are on screen so the difference is visible.

### 3 · TIME DILATION

| Control | Meaning |
|---|---|
| `REALTIME` | playhead pinned to the newest event |
| `10× / 100× / 1000× SLOWER` | market time advances at 1/10, 1/100, 1/1000 of wall time |

At `1000× SLOWER`, one real millisecond of market activity takes one second to
watch. Live data continues to arrive and be recorded at full speed the whole
time — only the playhead slows. `LAG` shows how far behind the live edge you
are, and `QUEUE` shows the local backlog growing in real time.

The playback rate and the dilation multiply: `EFFECTIVE` in the bottom bar
always shows the single resulting number, so the two controls can never be read
as contradicting each other.

---

## RECORD + REPLAY

`● REC` writes `recordings/*.binlog` — the **unmodified live frame stream**,
byte for byte, preceded by one metadata frame and a full book snapshot.

That has a useful consequence: replay is not a re-synthesis of the market. It is
the identical byte stream that was captured, carrying the original exchange
timestamps, the original local receive timestamps, and the processing latencies
that were actually measured at capture time. Verified by
`npm test` (check T6c: every replayed record is compared field by field against
the live record with the same local serial).

Because a recording begins with a book snapshot, stepping through a replay
reconstructs exactly the book states that were live at capture time.

Pick a file and press `▶ LOAD`. Live events keep being received and recorded
while the display follows the replay segment; `RETURN TO LIVE` re-seeds from a
fresh snapshot and states plainly that events arriving during the replay were
skipped for that client.

---

## Keyboard

| Key | Action |
|---|---|
| `Space` | pause / resume |
| `→` | single step |
| `1` `2` `3` | 1× / 0.1× / 0.01× playback |

The console/automation handle is at `window.__observer` (`state`, `book`,
`step`, `seekTo`, `returnToLive`, `ringInfo`).

---

## Verifying it

```bash
node server/index.mjs &        # needs a running observer
npm test                       # 13 end-to-end checks against the live feed
npm run test:metrics           # sliding-window bucketing
npm run test:recording         # composition of the newest recording
npm run probe                  # which Binance streams actually deliver, and how bursty
```

`npm test` connects to the running observer, consumes the real binary stream and
checks the properties the whole project rests on:

| Check | Property |
|---|---|
| T1 | every event frame is self-consistent |
| T2 | local sequencer serials strictly increase |
| T3 | the client's top of book **equals the server's authoritative book** at a shared update id |
| T3b | the full 20-level book is exact when the server's current id is shared |
| T4 | zero `pu`-chain breaks observed |
| T5 | event density and byte rate are those of the real venue |
| T6 | a recording replays into byte-identical records with original timestamps |

`npm run shot -- --url http://127.0.0.1:8787 --out shot.png` drives headless
Chrome over the DevTools Protocol (no dependencies) and reports any page
exception, so the UI can be checked rather than assumed.

---

## Findings

Things this tool surfaced that were not obvious going in.

**The arrival process is extremely bursty, and 100 ms is a long time.**
Messages arrive in tight clusters — the median inter-arrival gap is around
60 µs — separated by silences with a long tail: p99 ≈ 73 ms, and gaps of
**709 ms** were measured. About 1.1 % of gaps exceed 100 ms, which is why the
100 ms rate window legitimately reads zero several times a minute while the 1 s
window reads 1,500/s. That is not a dropped sample; it is the shape of the data.

**Binance emits non-economic trade messages.** BTCUSDT perp periodically sends
`{"e":"trade","p":"0","q":"0","X":"NA"}` — price zero, quantity zero. Counting
these as trades inflates TRADES/SEC and a zero poisons any reference price
derived from "last trade", which silently corrupted burst price-movement
figures before it was caught. They are now filtered and counted separately
(`non-economic trade markers` in the DATA INTEGRITY panel).

**A snapshot race can silently disable the entire depth stream.** If the REST
depth snapshot returns faster than the first `depthUpdate` arrives, every
buffered event is already *ahead* of the snapshot. Since `pu` only ever
increases, the bridging event can never arrive — and a naive implementation
ignores every depth update for the rest of the session while the socket stays
healthy and trades keep flowing. `installSnapshot()` therefore validates that
the retained buffer can actually bridge the snapshot, and keeps the buffer and
refetches when it cannot. This is what the `holes` and `stale snapshots
refetched` counters report.

---

## Layout

```
server/
  index.mjs       HTTP, static files, chunked binary stream, record/replay API
  pipeline.mjs    ingress buffer → normalizer → sequencer → book → metrics → frames
  feed.mjs        Binance WebSocket, arrival timestamps, reconnect, idle watchdog
  orderbook.mjs   L2 state machine with pu-chain validation and resync
  metrics.mjs     sliding windows, exact percentiles, burst detector, clock offset
  recorder.mjs    .binlog writer/reader (async, cached listing)
  clock.mjs       monotonic base, sub-ms wall clock, exchange clock-offset probe
shared/
  protocol.js     wire format: 88-byte records, frame layout (server + browser)
web/
  index.html      panel structure
  style.css       dark instrument styling
  app.js          playback cursor, book mirror, canvas renderers, controls
  worker.js       ingest worker: fetch reader + binary decode
  decode.js       frame decoder (shared by worker and main thread)
  ring.js         typed-array event ring (SharedArrayBuffer or ArrayBuffer)
test/
  run.mjs         end-to-end checks against the live feed
tools/
  screenshot.mjs  headless-Chrome capture + page-exception reporting
```
