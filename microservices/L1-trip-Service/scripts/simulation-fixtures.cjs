'use strict';
const assert = require('node:assert/strict');
const { Repository, START, END } = require('../src/repository');
const { sqlTime } = require('../src/domain');
const format = ms => sqlTime(ms, '+05:30');
function utcText(ms) {
  const s = new Date(ms).toISOString();
  return s.slice(8,10)+s.slice(5,7)+s.slice(0,4)+' '+s.slice(11,19).replaceAll(':','');
}
function dbFixture(rows) {
  const state = { rows: structuredClone(rows), writes: [], unavailable: false, loseCommit: false };
  const db = {
    async withTransaction(fn) {
      if (state.unavailable) throw Object.assign(new Error('Simulated MySQL outage'), { code: 'ECONNRESET' });
      const before = structuredClone(state.rows), beforeWrites = state.writes.length;
      let result;
      try { result = await fn(db); }
      catch (e) { state.rows = before; state.writes.length = beforeWrites; throw e; }
      if (state.loseCommit) { state.loseCommit = false; throw Object.assign(new Error('Commit response lost'), { code: 'ECONNRESET' }); }
      return result;
    },
    async writeExecute(sql, args) {
      if (sql.includes('FOR UPDATE')) return state.rows.map(t => ({ id: t.id }));
      if (sql.includes('MAX(completion_date)')) return [{ last_end: state.rows.filter(t => t.trip_status === 2)
        .map(t => t.completion_date).filter(Boolean).sort().at(-1) || null }];
      if (sql === START || sql === END) {
        const start = sql === START;
        const t = state.rows.find(t => t.id === args[start ? 5 : 3]);
        if (!t || t.trip_status !== (start ? 0 : 1)) return { affectedRows: 0 };
        if (start) {
          assert.ok(args[0] > t.lifting_date);
          Object.assign(t, { trip_status: 1, trip_date: args[0], start_lat: args[3], start_lng: args[4],
            expected_arrival_time: args[2] === null ? null : format(Date.parse(args[0].replace(' ','T')+'+05:30') + args[2]*60000) });
        } else {
          assert.ok(args[2] > t.trip_date);
          Object.assign(t, { trip_status: 2, completion_date: args[2], end_lat: args[0], end_lng: args[1] });
        }
        state.writes.push({ kind: start ? 'start' : 'end', trip_id: t.id, saved_datetime_ist: args[start ? 0 : 2] });
        return { affectedRows: 1 };
      }
      assert.ok(sql.startsWith('SELECT'));
      return structuredClone(state.rows.filter(t => t.trip_status < 2 || (sql.includes('OR tp.id IN') && args.slice(1).includes(t.id))));
    }
  };
  return { state, repo: new Repository(db) };
}
module.exports = { format, utcText, dbFixture };
