'use strict';
const Database = require('../model/dbquery');
const { integer } = require('./settings');
// Explicit opt-in only. No generated-ID inserts or affectedRows-dependent transitions.
class WriteBatch {
  constructor(db =  Database, options = {}) {
    this.db = db;
    this.size = options.size ?? integer('DB_BATCH_SIZE', 200, 1, 1000);
    this.delay = options.delay ?? integer('DB_BATCH_INTERVAL_MS', 25, 1, 1000);
    this.limit = options.limit ?? integer('DB_WRITE_QUEUE_LIMIT', 5000);
    this.queue = []; this.pending = 0; this.running = null; this.timer = null; this.closed = false;
  }
  insert(sql, values) { return this.enqueue('insert', sql, values); }
  update(sql, values) { return this.enqueue('update', sql, values); }
  enqueue(kind, sql, values) {
    if (this.closed) return Promise.reject(new Error('Write queue is closed'));
    if (this.pending >= this.limit) return Promise.reject(new Error('DB write queue full'));
    // Templates are fixed repository SQL, never user-provided SQL.
    if (!Array.isArray(values) || (sql.match(/\?/g) || []).length !== values.length) {
      return Promise.reject(new Error('Batch parameter count mismatch'));
    }
    this.pending++;
    const result = new Promise((resolve, reject) => this.queue.push({ kind, sql, values, resolve, reject }));
    if (this.queue.length >= this.size) this.flush();
    else if (!this.timer && !this.running) this.timer = setTimeout(() => this.flush(), this.delay);
    return result;
  }
  compile(items) {
    const first = items[0];
    // Distinct update IDs may be sorted; repeated IDs and template transitions
    // already form separate batches and retain enqueue order.
    if (first.kind === 'update') items = [...items].sort((a,b) =>
      String(a.values.at(-1)).localeCompare(String(b.values.at(-1)), 'en', {numeric:true}));
    const values = items.flatMap(item => item.values);
    if (first.kind === 'insert') {
      const match = first.sql.match(/^(\s*INSERT\s+INTO\s+\w+\s*\([^]*?\)\s*VALUES\s*)(\([^]*\))\s*$/i);
      if (!match) throw new Error('Unsupported insert batch template');
      return [match[1] + items.map(() => match[2]).join(','), values];
    }
    const match = first.sql.match(/^\s*UPDATE\s+(\w+)\s+SET\s+([^]*?)\s+WHERE\s+id\s*=\s*\?([^]*)$/i);
    if (!match || (match[3].match(/\?/g) || []).length) throw new Error('Unsupported update batch template');
    // Each distinct ID joins exactly one derived row. Repeated IDs flush in order.
    const n = first.values.length;
    let i = 0;
    const set = match[2].replace(/\?/g, () => `batch.v${i++}`);
    if (i !== n - 1) throw new Error('Invalid update batch key');
    const select = first.values.map((_, j) => `? AS v${j}`).join(',');
    const derived = items.map(() => `SELECT ${select}`).join(' UNION ALL ');
    return [`UPDATE ${match[1]} JOIN (${derived}) AS batch ON ${match[1]}.id = batch.v${n - 1} SET ${set} WHERE ${match[1]}.id = batch.v${n - 1} ${match[3]}`, values];
  }
  flush() {
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    if (this.running) return this.running;
    this.running = this.drain().finally(() => {
      this.running = null;
      if (this.queue.length) this.flush();
    });
    return this.running;
  }
  async drain() {
    while (this.queue.length) {
      const first = this.queue[0], items = [], keys = new Set();
      // Contiguous groups preserve ordering even between different update templates.
      while (this.queue.length && items.length < this.size) {
        const item = this.queue[0];
        if (item.kind !== first.kind || item.sql !== first.sql) break;
        const key = String(item.values[item.values.length - 1]);
        if (item.kind === 'update' && keys.has(key)) break;
        keys.add(key); items.push(this.queue.shift());
      }
      try {
        const [sql, values] = this.compile(items);
        const result = await this.db.query(sql, values, {retryDeadlocks: first.kind === 'update'});
        // Aggregate result only; callers opting in must not use per-row insertId/counts.
        for (const item of items) item.resolve({ affectedRows: result.affectedRows, batchSize: items.length });
      } catch (error) {
        // Never retry non-idempotent writes after an ambiguous connection failure.
        for (const item of items) item.reject(error);
      } finally { this.pending -= items.length; }
    }
  }
  async close() { this.closed = true; await this.flush(); }
}
module.exports = new WriteBatch();
module.exports.WriteBatch = WriteBatch;
