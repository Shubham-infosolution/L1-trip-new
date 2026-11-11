'use strict';
const db = require('../model/dbquery');
const { integer } = require('./settings');

// Keep a claimed Redis batch atomic: a failed later SQL chunk rolls back earlier chunks.
module.exports = function chunkRepository(repository, methods) {
  for (const name of methods) {
    const original = repository[name];
    if (typeof original !== 'function') throw new Error(`Missing bulk method ${name}`);
    repository[name] = async function(rows = [], ...args) {
      if (!Array.isArray(rows)) throw new TypeError(`${name} expects an array`);
      if (!rows.length) return { affectedRows: 0 };
      const size = integer('DB_BATCH_SIZE', 200, 1, 1000);
      return db.withTransaction(async () => {
        let affectedRows = 0;
        for (let i = 0; i < rows.length; i += size) {
          const result = await original.call(this, rows.slice(i, i + size), ...args);
          if (!result || !Number.isFinite(result.affectedRows)) throw new Error(`${name} did not return a DB result`);
          affectedRows += result.affectedRows;
        }
        return { affectedRows };
      });
    };
  }
};
