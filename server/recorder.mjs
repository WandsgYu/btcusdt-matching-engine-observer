/**
 * Frame recording and replay.
 *
 * A recording is byte-for-byte the same frame stream that live clients receive,
 * with one extra leading CONTROL frame carrying metadata and a full book
 * snapshot.  Two useful consequences:
 *
 *   - replay is not a re-synthesis of the market, it is the identical byte
 *     stream that was captured, so replayed events are the original events
 *     with their original exchange timestamps, local receive timestamps and
 *     measured processing latencies;
 *   - a recording starts from a book snapshot, so stepping through a replay
 *     reconstructs exactly the book states that were live at capture time.
 */

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { FRAME, RECORD_BYTES, encodeControl } from '../shared/protocol.js';

export class Recorder {
  constructor({ dir, symbol = 'BTCUSDT' }) {
    this.dir = dir;
    this.symbol = symbol;
    fs.mkdirSync(dir, { recursive: true });
    this.active = null;
    this.lastFile = null;
    this.history = [];
    /** name -> {size, mtime, entry} so repeated listings do not re-read files. */
    this._cache = new Map();
  }

  get recording() {
    return this.active !== null;
  }

  state() {
    return {
      recording: this.recording,
      file: this.active ? this.active.name : null,
      startedAt: this.active ? this.active.startedAt : null,
      limitMs: this.active ? this.active.limitMs : null,
      endsAt: this.active ? this.active.startedAt + this.active.limitMs : null,
      bytes: this.active ? this.active.bytes : 0,
      frames: this.active ? this.active.frames : 0,
      lastFile: this.lastFile,
      directory: this.dir,
    };
  }

  /**
   * @param {object} opts
   * @param {number} opts.limitMs  auto-stop after this long (0 = manual stop)
   * @param {Uint8Array} opts.snapshotFrames frames describing the current book
   * @param {object} opts.meta     free-form metadata stored in the header
   */
  start({ limitMs = 30000, snapshotFrames = null, meta = {} } = {}) {
    if (this.active) this.stop();
    const now = new Date();
    const stamp = now.toISOString().replace(/[-:]/g, '').replace(/\..+/, '').replace('T', '-');
    const name = `${this.symbol.toLowerCase()}-${stamp}.binlog`;
    const full = path.join(this.dir, name);
    const fd = fs.openSync(full, 'w');

    const header = encodeControl('recording-header', {
      symbol: this.symbol,
      startedAt: now.getTime(),
      limitMs,
      serverNote:
        'Frames are the unmodified live stream. Timestamps are original: exchangeTs from Binance, localRecvMs/localMonoUs/latencyUs measured locally at capture time.',
      ...meta,
    });
    fs.writeSync(fd, header);

    this.active = {
      fd,
      name,
      full,
      startedAt: now.getTime(),
      limitMs,
      bytes: header.length,
      frames: 1,
      timer: null,
    };
    if (limitMs > 0) {
      this.active.timer = setTimeout(() => this.stop(), limitMs);
    }
    if (snapshotFrames) this.writeFrame(snapshotFrames);
    return this.state();
  }

  /** @param {Uint8Array} frame a complete FRAME.EVENTS / FRAME.CONTROL frame */
  writeFrame(frame) {
    const a = this.active;
    if (!a) return;
    try {
      fs.writeSync(a.fd, frame);
      a.bytes += frame.length;
      a.frames++;
    } catch (err) {
      this.lastError = String(err);
      this.stop();
    }
  }

  stop() {
    const a = this.active;
    if (!a) return null;
    if (a.timer) clearTimeout(a.timer);
    try {
      fs.closeSync(a.fd);
    } catch {
      /* ignore */
    }
    this.active = null;
    const info = {
      name: a.name,
      bytes: a.bytes,
      frames: a.frames,
      startedAt: a.startedAt,
      durationMs: Date.now() - a.startedAt,
    };
    this.lastFile = info;
    this.history.unshift(info);
    if (this.history.length > 20) this.history.pop();
    return info;
  }

  /**
   * List recordings on disk.
   *
   * Asynchronous and cached. The previous implementation walked every frame of
   * every file with one synchronous read() per field, which blocked the event
   * loop for tens of milliseconds on each call - enough to stall the websocket
   * receive path and visibly punch holes in the 100 ms metric windows. Reading
   * each file in a single async pass, and caching by (name, size, mtime), keeps
   * the ingest loop untouched.
   */
  async list() {
    let names = [];
    try {
      names = (await fsp.readdir(this.dir)).filter((n) => n.endsWith('.binlog'));
    } catch {
      return [];
    }
    const out = [];
    for (const name of names) {
      const full = path.join(this.dir, name);
      let stat;
      try {
        stat = await fsp.stat(full);
      } catch {
        continue;
      }
      const cached = this._cache.get(name);
      if (cached && cached.size === stat.size && cached.mtime === stat.mtimeMs) {
        out.push(cached.entry);
        continue;
      }
      const entry = {
        name,
        bytes: stat.size,
        mtime: stat.mtimeMs,
        startedAt: null,
        durationMs: null,
        events: 0,
        meta: null,
        corrupt: false,
      };
      try {
        const buf = await fsp.readFile(full);
        let off = 0;
        let headerDone = false;
        while (off + 5 <= buf.length) {
          const type = buf.readUInt8(off);
          const len = buf.readUInt32LE(off + 1);
          if (len < 0 || off + 5 + len > buf.length) {
            if (!headerDone) entry.corrupt = true;
            break;
          }
          if (!headerDone && type === FRAME.CONTROL) {
            try {
              const meta = JSON.parse(buf.toString('utf8', off + 5, off + 5 + len));
              entry.meta = meta;
              entry.startedAt = meta.startedAt ?? null;
              entry.durationMs = meta.limitMs ?? null;
              headerDone = true;
            } catch {
              entry.corrupt = true;
            }
          } else if (type === FRAME.EVENTS && len >= 4) {
            entry.events += buf.readUInt32LE(off + 5);
          }
          off += 5 + len;
        }
      } catch {
        entry.corrupt = true;
      }
      this._cache.set(name, { size: stat.size, mtime: stat.mtimeMs, entry });
      out.push(entry);
    }
    out.sort((a, b) => b.mtime - a.mtime);
    return out;
  }

  /** Read a whole recording into memory, split into frames. */
  readFrames(name) {
    const safe = path.basename(name);
    const full = path.join(this.dir, safe);
    if (!full.startsWith(path.resolve(this.dir))) throw new Error('path escape');
    const buf = fs.readFileSync(full);
    const frames = [];
    let off = 0;
    while (off + 5 <= buf.length) {
      const type = buf.readUInt8(off);
      const len = buf.readUInt32LE(off + 1);
      if (len < 0 || off + 5 + len > buf.length) {
        throw new Error(`truncated frame at offset ${off}`);
      }
      if (type === FRAME.EVENTS && len >= 4 && (len - 4) % RECORD_BYTES !== 0) {
        throw new Error(`bad event frame length ${len} at ${off}`);
      }
      frames.push(buf.subarray(off, off + 5 + len));
      off += 5 + len;
    }
    return { frames, bytes: buf.length, truncated: off !== buf.length };
  }
}
