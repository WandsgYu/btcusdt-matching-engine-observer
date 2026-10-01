/**
 * Binance USDⓈ-M futures public WebSocket feed.
 *
 * Responsibilities are deliberately narrow: own the socket, capture arrival
 * timestamps as the *first* statement of the message callback (anything done
 * before that would be charged to the network), and hand raw frames to the
 * pipeline.  All interpretation happens downstream.
 *
 * Timestamps captured per message:
 *   recvMonoUs - monotonic microseconds since process start. Used for every
 *                duration in the system (inter-arrival, processing latency).
 *   recvWallMs - high resolution wall clock, used only to compare against the
 *                exchange timestamp.
 */

import { EventEmitter } from 'node:events';
import { monoUs, wallMsHiRes } from './clock.mjs';

export const DEFAULT_STREAMS = Object.freeze([
  'btcusdt@depth@0ms',
  'btcusdt@trade',
  'btcusdt@bookTicker',
  'btcusdt@markPrice@1s',
]);

const WS_BASE = 'wss://fstream.binance.com/stream?streams=';

export class BinanceFeed extends EventEmitter {
  constructor({ streams = DEFAULT_STREAMS, symbol = 'BTCUSDT', idleTimeoutMs = 45000 } = {}) {
    super();
    this.streams = [...streams];
    this.symbol = symbol;
    this.idleTimeoutMs = idleTimeoutMs;
    this.ws = null;
    this.state = 'idle';
    this.attempts = 0;
    this.connectedAt = null;
    this.reconnects = 0;
    this.lastMessageWallMs = 0;
    this.messagesSinceConnect = 0;
    this.bytesSinceConnect = 0;
    this._closing = false;
    this._backoffMs = 500;
    this._watchdog = null;
    this.lastError = null;
  }

  get url() {
    return WS_BASE + this.streams.join('/');
  }

  start() {
    this._closing = false;
    this._connect();
    this._watchdog = setInterval(() => this._checkIdle(), 5000);
    return this;
  }

  stop() {
    this._closing = true;
    if (this._watchdog) clearInterval(this._watchdog);
    this._watchdog = null;
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        /* ignore */
      }
      this.ws = null;
    }
  }

  _setState(state, detail) {
    if (this.state === state) return;
    this.state = state;
    this.emit('state', { state, detail, wallMs: Date.now(), attempts: this.attempts });
  }

  _connect() {
    if (this._closing) return;
    this.attempts++;
    this._setState(this.attempts === 1 ? 'connecting' : 'reconnecting');
    let ws;
    try {
      ws = new WebSocket(this.url);
    } catch (err) {
      this.lastError = String(err);
      this._scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.onopen = () => {
      this.connectedAt = Date.now();
      this.messagesSinceConnect = 0;
      this.bytesSinceConnect = 0;
      this.lastMessageWallMs = Date.now();
      this._backoffMs = 500;
      this._setState('open');
      this.emit('open');
    };

    ws.onmessage = (ev) => {
      // Capture time before touching the payload.
      const recvMonoUs = monoUs();
      const recvWallMs = wallMsHiRes();
      const data = ev.data;
      const bytes = typeof data === 'string' ? data.length : data.byteLength || 0;
      this.lastMessageWallMs = Date.now();
      this.messagesSinceConnect++;
      this.bytesSinceConnect += bytes;
      this.emit('message', data, recvMonoUs, recvWallMs, bytes);
    };

    ws.onerror = (ev) => {
      this.lastError = ev && ev.message ? ev.message : 'websocket error';
      this.emit('error', this.lastError);
    };

    ws.onclose = (ev) => {
      this.ws = null;
      if (this._closing) {
        this._setState('closed');
        return;
      }
      this.reconnects++;
      this.emit('disconnect', { code: ev && ev.code, reason: ev && ev.reason });
      this._scheduleReconnect();
    };
  }

  _scheduleReconnect() {
    if (this._closing) return;
    const delay = Math.min(30000, this._backoffMs) * (0.5 + Math.random() * 0.5);
    this._backoffMs = Math.min(30000, this._backoffMs * 2);
    this._setState('reconnecting', `retry in ${Math.round(delay)} ms`);
    setTimeout(() => this._connect(), delay);
  }

  _checkIdle() {
    if (this.state !== 'open') return;
    const idle = Date.now() - this.lastMessageWallMs;
    if (idle > this.idleTimeoutMs) {
      this.lastError = `no data for ${idle} ms`;
      this.emit('idle-timeout', { idleMs: idle });
      this.reconnects++;
      try {
        this.ws && this.ws.close();
      } catch {
        /* ignore */
      }
    }
  }

  status() {
    return {
      state: this.state,
      url: this.url,
      streams: this.streams,
      attempts: this.attempts,
      reconnects: this.reconnects,
      connectedAt: this.connectedAt,
      lastMessageWallMs: this.lastMessageWallMs,
      messagesSinceConnect: this.messagesSinceConnect,
      bytesSinceConnect: this.bytesSinceConnect,
      lastError: this.lastError,
    };
  }
}
