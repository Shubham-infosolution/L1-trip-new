'use strict';
require('dotenv').config({ quiet: true });
const cron = require('../utils/managedCron');
const Database = require('../model/dbquery');
const writeBatch = require('../db/writeBatch');
const RedisRepository = require('../repositories/RedisRepository');
const stopMonitor = require('../utils/eventLoopMonitor')('alerts');
require('../services/AlertBatchService');
require('../services/notification.service');
require('../jobs/alert.scheduler').start();

let stopping = false;
async function shutdown() {
    if (stopping) return;
    stopping = true;
    const timeout = setTimeout(() => process.exit(1), 18000);
    try {
        await cron.stopAll();
        await writeBatch.close();
        await Database.close();
        await RedisRepository.quit();
        stopMonitor();
        clearTimeout(timeout);
        if (process.connected) process.disconnect();
        process.exitCode = 0;
    } catch (error) {
        console.error('[BatchWorker] Shutdown failed:', error);
        process.exit(1);
    }
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
process.once('disconnect', shutdown);
process.on('message', message => { if (message?.type === 'shutdown') shutdown(); });
if (process.send) process.send({ type: 'ready' });
