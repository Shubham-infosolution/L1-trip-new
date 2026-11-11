'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
process.env.DB_QUEUE_LIMIT = '200';
const originalLoad = Module._load;
let created = 0, poolCalls = [], connections = [], failBegin = false, failCommit = false, failRollback = false;
const fakePool = {
  on() {},
  query(sql, values, cb) { poolCalls.push({ sql, values }); cb(sql === 'FAIL' ? new Error('SQL failure') : null, { affectedRows: 1 }); },
  getConnection(cb) {
    const conn = {
      queries: [], released: 0, destroyed: 0, committed: 0, rolledBack: 0,
      beginTransaction(done) { done(failBegin ? new Error('begin failure') : null); },
      query(sql, values, done) { this.queries.push(sql); setImmediate(() => done(sql === 'FAIL' ? new Error('SQL failure') : null, { affectedRows: 1 })); },
      commit(done) { this.committed++; done(failCommit ? new Error('commit failure') : null); },
      rollback(done) { this.rolledBack++; done(failRollback ? new Error('rollback failure') : null); },
      release() { this.released++; }, destroy() { this.destroyed++; }
    };
    connections.push(conn); cb(null, conn);
  },
  end(cb) { cb(); }
};
Module._load = function(name, ...args) {
  if (name === 'mysql2') return { createPool(config) { created++; assert.equal(config.connectionLimit, 10); assert.equal(config.maxIdle, 5); assert.equal(config.queueLimit, 200); return fakePool; } };
  return originalLoad.call(this, name, ...args);
};
const { Database } = require('../src/model/dbquery');
Module._load = originalLoad;
const db = new Database();
test('one pool across repository instances; pool.query handles ordinary reads and errors', async () => {
  await Promise.all(Array.from({ length: 11 }, () => new Database().query('SELECT 1')));
  assert.equal(created, 1);
  await assert.rejects(db.query('FAIL'), /SQL failure/);
  assert.equal(connections.length, 0);
});
test('concurrent transaction scopes stay isolated and release exactly once', async () => {
  const start = connections.length;
  await Promise.all(['A','B'].map(label => db.withTransaction(async () => {
    await new Database().query(label);
    await db.withTransaction(() => db.query(label));
  })));
  for (const [i,label] of ['A','B'].entries()) {
    const conn = connections[start+i];
    assert.deepEqual(conn.queries, [label,label]); assert.equal(conn.committed,1); assert.equal(conn.released,1);
  }
});
test('SQL and begin/commit failures roll back and release; rollback failure destroys', async () => {
  await assert.rejects(db.withTransaction(() => db.query('FAIL')), error => error.message === 'SQL failure' && error.transactionRolledBack === true);
  assert.equal(connections.at(-1).rolledBack,1); assert.equal(connections.at(-1).released,1);
  failBegin = true;
  await assert.rejects(db.withTransaction(() => db.query('A')), /begin failure/); failBegin = false;
  assert.equal(connections.at(-1).released,1);
  failCommit = true;
  await assert.rejects(db.withTransaction(() => db.query('A')), error => error.message === 'commit failure' && error.transactionRolledBack === false); failCommit = false;
  assert.equal(connections.at(-1).released,1);
  failRollback = true;
  await assert.rejects(db.withTransaction(() => db.query('FAIL')), error => error.message === 'SQL failure' && error.transactionRolledBack === false); failRollback = false;
  assert.equal(connections.at(-1).destroyed,1); assert.equal(connections.at(-1).released,0);
});
test('bulk chunks share one transaction and a later failure rolls everything back', async () => {
  const wrap = require('../src/db/chunkRepository');
  process.env.DB_BATCH_SIZE = '2';
  const calls = [];
  const repo = { async bulk(rows) { calls.push(rows); return db.query(rows.includes(5) ? 'FAIL' : 'INSERT', rows); } };
  wrap(repo, ['bulk']);
  await assert.rejects(repo.bulk([1,2,3,4,5]), /SQL failure/);
  assert.deepEqual(calls, [[1,2],[3,4],[5]]);
  assert.equal(connections.at(-1).committed,0); assert.equal(connections.at(-1).rolledBack,1);
  assert.equal(connections.at(-1).released,1);
  delete process.env.DB_BATCH_SIZE;
});
