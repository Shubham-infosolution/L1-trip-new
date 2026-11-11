'use strict';
const { monitorEventLoopDelay } = require('node:perf_hooks');
module.exports = function startEventLoopMonitor(label) {
    const lag = monitorEventLoopDelay({ resolution: 20 });
    lag.enable();
    const timer = setInterval(() => {
        const maxMs = Math.round(lag.max / 1e6);
        if (maxMs >= 1000) console.warn(`[EventLoop] ${label} | maxLagMs=${maxMs} | rssMB=${Math.round(process.memoryUsage().rss / 1048576)}`);
        lag.reset();
    }, 30000);
    timer.unref();
    return () => { clearInterval(timer); lag.disable(); };
};
