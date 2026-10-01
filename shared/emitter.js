/**
 * Minimal event emitter that works identically in Node and in a browser
 * worker.  The pipeline and the websocket feed both need `on`/`emit`; pulling
 * in `node:events` would make the whole pipeline Node-only, and the point of
 * this file is that one implementation of the pipeline runs in both places.
 *
 * Deliberately not a full EventEmitter: no `prependListener`, no
 * `setMaxListeners`, no special-casing of the 'error' event (throwing from
 * inside `emit` when nothing is listening would take down a worker for an
 * error that is already being surfaced elsewhere).
 */
export class Emitter {
  constructor() {
    /** @type {Map<string, Function[]>} */
    this._listeners = new Map();
  }

  on(type, fn) {
    let arr = this._listeners.get(type);
    if (!arr) {
      arr = [];
      this._listeners.set(type, arr);
    }
    arr.push(fn);
    return this;
  }

  off(type, fn) {
    const arr = this._listeners.get(type);
    if (arr) {
      const i = arr.indexOf(fn);
      if (i >= 0) arr.splice(i, 1);
    }
    return this;
  }

  once(type, fn) {
    const wrapper = (...args) => {
      this.off(type, wrapper);
      fn(...args);
    };
    return this.on(type, wrapper);
  }

  emit(type, ...args) {
    const arr = this._listeners.get(type);
    if (!arr || arr.length === 0) return false;
    // Copy: a listener may remove itself while we iterate.
    for (const fn of arr.slice()) {
      try {
        fn(...args);
      } catch (err) {
        // A throwing listener must not silently break the ingest loop.
        if (typeof console !== 'undefined') console.error(`listener for "${type}" threw:`, err);
      }
    }
    return true;
  }

  removeAllListeners(type) {
    if (type === undefined) this._listeners.clear();
    else this._listeners.delete(type);
    return this;
  }
}
