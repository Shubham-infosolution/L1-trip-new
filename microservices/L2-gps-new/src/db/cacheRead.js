'use strict';
// Small process-local TTL cache with single-flight loads, for configuration only.
module.exports = function cacheRead(repository, method, ttl = 30000, limit = 1000) {
  const original = repository[method], entries = new Map();
  repository[method] = async function(...args) {
    const key = JSON.stringify(args), now = Date.now();
    let entry = entries.get(key);
    if (!entry || entry.expires <= now) {
      if (entries.size >= limit) entries.delete(entries.keys().next().value);
      entry = { expires: now + ttl };
      entry.promise = Promise.resolve().then(() => original.apply(this, args));
      entries.set(key, entry);
      entry.promise.catch(() => { if (entries.get(key) === entry) entries.delete(key); });
    }
    return structuredClone(await entry.promise);
  };
};
