'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { WriteBatch } = require('../src/db/writeBatch');
function setup(options = {}) {
  const calls = [];
  const batch = new WriteBatch({ async query(sql, values) { calls.push({sql,values}); return {affectedRows: 1}; } }, { size: 200, delay: 1, ...options });
  return { calls, batch };
}
test('concurrent insert calls become bounded SQL batches and await persistence', async () => {
  const {calls,batch} = setup({size:3});
  const sql = 'INSERT INTO alerts (vehicle, c_date) VALUES (?, NOW())';
  const promises = Array.from({length:8}, (_,i) => batch.insert(sql,[String(i)]));
  await Promise.all(promises); await batch.close();
  assert.deepEqual(calls.map(c=>c.values.length),[3,3,2]);
  assert.deepEqual(calls.flatMap(c=>c.values), ['0','1','2','3','4','5','6','7']);
});
test('updates batch distinct IDs while repeated IDs retain order', async () => {
  const {calls,batch} = setup();
  const sql = 'UPDATE events SET duration = ?, path = JSON_ARRAY_APPEND(path, \'$\', ?) WHERE id = ? AND status = 1 AND end_date_time IS NULL';
  await Promise.all([[1,'a',7],[2,'b',8],[3,'c',7]].map(row=>batch.update(sql,row)));
  await batch.close();
  assert.equal(calls.length,2);
  assert.match(calls[0].sql,/UNION ALL/);
  assert.match(calls[0].sql,/duration = batch.v0/);
  assert.match(calls[0].sql,/events.id = batch.v2/);
  assert.match(calls[0].sql,/AND status = 1 AND end_date_time IS NULL/);
  assert.deepEqual(calls[0].values,[1,'a',7,2,'b',8]);
  assert.deepEqual(calls[1].values,[3,'c',7]);
});
test('different templates are never reordered across close/progress writes', async () => {
  const {calls,batch} = setup();
  await Promise.all([
    batch.update('UPDATE events SET duration = ? WHERE id = ?', [1,7]),
    batch.update('UPDATE events SET closed = ? WHERE id = ?', [1,7]),
    batch.update('UPDATE events SET duration = ? WHERE id = ?', [2,8])
  ]);
  await batch.close();
  assert.equal(calls.length,3); assert.match(calls[1].sql,/closed/);
});
test('failures reject all batch callers without automatic replay', async () => {
  let count = 0;
  const batch = new WriteBatch({async query() { count++; throw new Error('lost connection'); }}, {size:10,delay:1,limit:10});
  const results = await Promise.allSettled([batch.insert('INSERT INTO logs (v) VALUES (?)',[1]),batch.insert('INSERT INTO logs (v) VALUES (?)',[2])]);
  assert.ok(results.every(r=>r.status==='rejected')); assert.equal(count,1); assert.equal(batch.pending,0);
  await batch.close();
});
test('queue limits reject overload and shutdown drains pending writes', async () => {
  const {calls,batch} = setup({limit:1,delay:100});
  const p = batch.insert('INSERT INTO logs (v) VALUES (?)',[1]);
  await assert.rejects(batch.insert('INSERT INTO logs (v) VALUES (?)',[2]), /queue full/);
  await batch.close(); await p;
  assert.equal(calls.length,1);
  await assert.rejects(batch.insert('INSERT INTO logs (v) VALUES (?)',[3]), /closed/);
});
test('timer flushes low traffic and rejects malformed templates', async () => {
  const {batch} = setup();
  await assert.rejects(batch.update('UPDATE events SET v=? WHERE vehicle=?',[1,'x']), /Unsupported/);
  await assert.rejects(batch.insert('INSERT INTO x (a) VALUES (?)',[]), /count mismatch/);
  await batch.close();
});
