'use strict';
const { fork } = require('node:child_process');
const path = require('node:path');

module.exports = function startBatchWorker() {
    let child, restart, stopping = false, failures = 0;
    function launch() {
        if (stopping) return;
        child = fork(path.join(__dirname, '../workers/alerts.js'), [], {
            stdio: ['ignore', 'inherit', 'inherit', 'ipc'], windowsHide: true,
            execArgv: process.execArgv.filter(arg => !arg.startsWith('--inspect'))
        });
        const current = child;
        current.on('message', message => {
            if (message?.type === 'ready') {
                failures = 0;
                console.log('[BatchWorker] Ready');
            }
        });
        current.on('error', error => console.error('[BatchWorker]', error));
        current.once('exit', (code, signal) => {
            if (child === current) child = null;
            if (!stopping) {
                const delay = Math.min(30000, 1000 * 2 ** Math.min(failures++, 5));
                console.error(`[BatchWorker] Exited (${code ?? signal}); restarting in ${delay}ms`);
                restart = setTimeout(launch, delay);
            }
        });
    }
    launch();
    return { async stop() {
        stopping = true;
        clearTimeout(restart);
        if (!child || child.exitCode !== null || child.signalCode) return;
        const current = child;
        await new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                current.kill();
                reject(new Error('Batch worker shutdown timed out'));
            }, 20000);
            current.once('exit', () => { clearTimeout(timer); resolve(); });
            if (current.connected) current.send({ type: 'shutdown' }, error => { if (error) current.kill(); });
            else current.kill();
        });
    } };
};
