'use strict';
const cron = require('node-cron');
const tasks = new Set();
const active = new Set();
let stopping = false;
module.exports = {
  schedule(expression, callback, options) {
    let running = false;
    const task = cron.schedule(expression, async (...args) => {
      if (stopping || running) return;
      running = true;
      const work = Promise.resolve().then(() => callback(...args));
      active.add(work);
      try { await work; }
      catch (error) { console.error('[Scheduled job]', error); }
      finally { running = false; active.delete(work); }
    }, options);
    tasks.add(task);
    return task;
  },
  async stopAll() {
    stopping = true;
    await Promise.all([...tasks].map(task => task.stop()));
    await Promise.allSettled([...active]);
    await Promise.all([...tasks].map(task => task.destroy()));
    tasks.clear();
  }
};
