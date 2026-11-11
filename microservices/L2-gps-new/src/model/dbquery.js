'use strict';
const mysql = require('mysql2');
const { AsyncLocalStorage } = require('node:async_hooks');
const { integer } = require('../db/settings');
const transactions = new AsyncLocalStorage();
let shared;
let closing;
const metrics = { acquired: 0, released: 0, enqueued: 0, queries: 0, errors: 0 };
function identifier(value) {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) throw new Error('Invalid SQL identifier');
  return `\`${value}\``;
}
function callbackCall(target, method, ...args) {
  return new Promise((resolve, reject) => {
    target[method](...args, (err, result) => err ? reject(err) : resolve(result));
  });
}
function pool() {
  if (closing) throw new Error('Database pool is closing');
  if (!shared) {
    const connectionLimit = integer('DB_POOL_LIMIT', 10, 1, 1000);
    const timezone = process.env.APP_TIMEZONE || 'Asia/Kolkata';
    const part = new Intl.DateTimeFormat('en-US', { timeZone: timezone,
      timeZoneName: 'longOffset' }).formatToParts(new Date()).find(p => p.type === 'timeZoneName');
    const offset = part.value === 'GMT' ? '+00:00' : part.value.replace('GMT', '');
    shared = mysql.createPool({
      // Use the live MySQL Router primary endpoint, including transaction reads.
      host: process.env.DB_WRITE_HOST || process.env.DB_HOST || '127.0.0.1',
      port: integer('DB_WRITE_PORT', integer('DB_PORT', 6446, 1, 65535), 1, 65535),
      user: 'Jsfc_Amazon', 
      password: 'Jsfc_Amazon@2026' ,
      database: process.env.DB_NAME || 'jsfc_vts', connectionLimit,
      maxIdle: integer('DB_MAX_IDLE', Math.min(5, connectionLimit), 0, connectionLimit),
      idleTimeout: integer('DB_IDLE_TIMEOUT_MS', 60000),
      waitForConnections: true, queueLimit: integer('DB_QUEUE_LIMIT',  0, 0, 100000),
      connectTimeout: integer('DB_CONNECT_TIMEOUT_MS', 10000),
      enableKeepAlive: true, keepAliveInitialDelay: 10000,
      multipleStatements: false, timezone: offset
    });
    shared.on('acquire', () => {
      metrics.acquired++;
      // console.log(`[DB ACQUIRE] acquired=${metrics.acquired} released=${metrics.released} active=${metrics.acquired - metrics.released} total=${shared._allConnections?.length || 0} free=${shared._freeConnections?.length || 0} waiting=${shared._connectionQueue?.length || 0}`);
    });

    shared.on('release', () => {
        metrics.released++;
        // console.log(`[DB RELEASE] acquired=${metrics.acquired} released=${metrics.released} active=${metrics.acquired - metrics.released} total=${shared._allConnections?.length || 0} free=${shared._freeConnections?.length || 0} waiting=${shared._connectionQueue?.length || 0}`);
    });

    shared.on('enqueue', () => {
        metrics.enqueued++;
        // console.warn(`[DB ENQUEUE] enqueued=${metrics.enqueued} acquired=${metrics.acquired} released=${metrics.released} active=${metrics.acquired - metrics.released} total=${shared._allConnections?.length || 0} free=${shared._freeConnections?.length || 0} waiting=${shared._connectionQueue?.length || 0}`);
    });
    shared.on('error', error => console.error('[DB pool]', error.message));
  }
  return shared;
}
class Database {
  // All repository instances use this one pool, per Node process.
  get pool() { return pool(); }
  async connect() { await this.query('SELECT 1'); }
  async query(sql, values = [], options = {}) {
    const context = transactions.getStore();
    if (context?.closed) throw new Error('Transaction already finished');
    // A deadlock inside an explicit transaction requires replaying the entire
    // transaction, never just its last statement. This opt-in is autocommit only.
    const retries = options.retryDeadlocks && !context && /^\s*UPDATE\b/i.test(sql)
      ? integer('DB_DEADLOCK_RETRIES', 3, 0, 5) : 0;
    for (let attempt = 0; ; attempt++) {
      metrics.queries++;
      try {
        return await callbackCall(context?.connection || this.pool, 'query', sql, values);
      } catch (error) {
        metrics.errors++;
        if (error.code !== 'ER_LOCK_DEADLOCK' || attempt >= retries) throw error;
        // MySQL has rolled back the failed autocommit statement. Connection errors
        // and lock timeouts are deliberately excluded: their outcome may differ.
        await new Promise(resolve => setTimeout(resolve, 25 * 2 ** attempt + Math.floor(Math.random() * 25)));
      }
    }
  }
  joinSelect(sql, values = []) { return this.query(sql, values); }
  async withTransaction(work) {
    const existing = transactions.getStore();
    if (existing) {
      if (existing.closed) throw new Error('Transaction already finished');
      return work(this);
    }
    let connection;
    try { connection = await callbackCall(this.pool, 'getConnection'); }
    catch (error) { error.transactionRolledBack = true; throw error; }
    const context = { connection, closed: false };
    let reusable = true;
    let committing = false;
    try {
      await callbackCall(connection, 'beginTransaction');
      const result = await transactions.run(context, () => work(this));
      committing = true;
      await callbackCall(connection, 'commit');
      return result;
    } catch (error) {
      // Once COMMIT was sent, its outcome may be unknown even if ROLLBACK succeeds.
      error.transactionRolledBack = false;
      try { await callbackCall(connection, 'rollback'); error.transactionRolledBack = !committing; }
      catch (rollbackError) { reusable = false; error.rollbackError = rollbackError; }
      throw error;
    } finally {
      context.closed = true;
      if (reusable) connection.release(); else connection.destroy();
    }
  }
  // No supplied caller used the old, unsafe shared transactionConn API.
  beginTransaction() { throw new Error('Use withTransaction(async db => { ... })'); }
  commit() { throw new Error('Commit is managed by withTransaction'); }
  rollback() { throw new Error('Rollback is managed by withTransaction'); }
  transactionQuery(sql, values = []) {
    if (!transactions.getStore()) throw new Error('No transaction started');
    return this.query(sql, values);
  }
  select(table, columns = ['*'], where = null, excludeId = null, limit = null, orderBy = null) {
    const conditions = [], values = [];
    for (const [key, value] of Object.entries(where || {})) {
      conditions.push(`${identifier(key)} = ?`); values.push(value);
    }
    if (excludeId != null) { conditions.push('id != ?'); values.push(excludeId); }
    let sql = `SELECT ${columns.join(',')} FROM ${identifier(table)}`;
    if (conditions.length) sql += ` WHERE ${conditions.join(' AND ')}`;
    if (orderBy) sql += ` ORDER BY ${orderBy}`;
    if (limit != null) {
      if (!Number.isInteger(Number(limit)) || Number(limit) < 0) throw new Error('Invalid limit');
      sql += ' LIMIT ?'; values.push(Number(limit));
    }
    return this.query(sql, values);
  }
  insert(table, data) { return this.query(`INSERT IGNORE INTO ${identifier(table)} SET ?`, data); }
  update(table, data, where) {
    if (!Object.keys(data || {}).length || !Object.keys(where || {}).length) throw new Error('Update data and WHERE are required');
    return this.query(`UPDATE ${identifier(table)} SET ${Object.keys(data).map(k => `${identifier(k)} = ?`).join(',')} WHERE ${Object.keys(where).map(k => `${identifier(k)} = ?`).join(' AND ')}`, [...Object.values(data), ...Object.values(where)]);
  }
  delete(table, where) {
    if (!Object.keys(where || {}).length) throw new Error('Delete WHERE is required');
    return this.query(`DELETE FROM ${identifier(table)} WHERE ${Object.keys(where).map(k => `${identifier(k)} = ?`).join(' AND ')}`, Object.values(where));
  }
  async insertMany(table, rows) {
    if (!Array.isArray(rows) || !rows.length) return { affectedRows: 0 };
    const size = integer('DB_BATCH_SIZE', 200, 1, 1000);
    return this.withTransaction(async () => {
      let affectedRows = 0;
      for (let i = 0; i < rows.length; i += size) {
        const result = await this.query(`INSERT INTO ${identifier(table)} VALUES ?`, [rows.slice(i, i + size)]);
        affectedRows += result.affectedRows;
      }
      return { affectedRows };
    });
  }
  stats() { return { ...metrics, connectionLimit: integer('DB_POOL_LIMIT', 10, 1, 1000) }; }
  close() {
    if (!closing) closing = shared ? callbackCall(shared, 'end') : Promise.resolve();
    return closing;
  }
}
// module.exports = Database;

module.exports = new Database();
module.exports.Database = Database;
