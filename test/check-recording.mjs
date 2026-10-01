/** Composition of a recording, by record kind. */
import { fileURLToPath } from 'node:url';
import { Recorder } from '../server/recorder.mjs';
import { readRecord, RECORD_BYTES, FRAME, KIND_LABEL } from '../shared/protocol.js';
const dir = fileURLToPath(new URL('../recordings/', import.meta.url));
const rec = new Recorder({ dir });
// Default to the most recent recording so `npm run test:recording` just works.
const name = process.argv[2] || (await rec.list())[0]?.name;
if (!name) {
  console.error('no recordings found; run the observer and press REC first');
  process.exit(2);
}
const { frames, bytes } = rec.readFrames(name);
const counts = {};
let n = 0, first = null, last = null, kindsInFirst200 = {};
const dv = new DataView(new ArrayBuffer(RECORD_BYTES));
for (const f of frames) {
  const type = f[0];
  if (type !== FRAME.EVENTS) continue;
  const len = f[1] | (f[2]<<8) | (f[3]<<16) | (f[4]<<24);
  const count = f[5] | (f[6]<<8) | (f[7]<<16) | (f[8]<<24);
  for (let i = 0; i < count; i++) {
    const view = new DataView(f.buffer, f.byteOffset + 9 + i*RECORD_BYTES, RECORD_BYTES);
    const r = readRecord(view, 0);
    counts[r.kind] = (counts[r.kind]||0)+1;
    if (n < 200) kindsInFirst200[r.kind] = (kindsInFirst200[r.kind]||0)+1;
    if (!first) first = r.localRecvMs;
    last = r.localRecvMs;
    n++;
  }
}
console.log(`${name}  ${(bytes/1024).toFixed(0)} KiB  ${frames.length} frames  ${n} records  span ${((last-first)/1000).toFixed(2)}s`);
console.log('all:', Object.entries(counts).map(([k,v])=>`${KIND_LABEL[k]}=${v} (${(v/n*100).toFixed(1)}%)`).join('  '));
console.log('first 200 records:', Object.entries(kindsInFirst200).map(([k,v])=>`${KIND_LABEL[k]}=${v}`).join('  '));
