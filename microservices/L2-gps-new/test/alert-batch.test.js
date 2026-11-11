'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function setup({ skippedCount = 0, failure, acknowledge = true, affectedRows = 2, alerts = [{ trip_id: 1, last_location: "23,85" }, { trip_id: 2, last_location: "24,86" }] } = {}) {
    const calls = [];
    const notifications = [];
    const key = 'vehicle_alert:processing:test';
    const mocks = {
        './alertNotifications': async rows => { notifications.push({rows, afterAck: calls.at(-1)?.[0] === 'ack'}); },
        '../utils/managedCron': { schedule() {} },
        '../repositories/RedisRepository': {
            async getVehicleAlertQueueLength() { return 2; },
            async moveVehicleAlertsToProcessing() { return { key, alerts }; },
            async deleteVehicleAlertProcessingQueue(k) { calls.push(['ack', k]); return acknowledge; },
            async retryVehicleAlertBatch(k) { calls.push(['retry', k]); }
        },
        '../repositories/TripRepository': { async bulkInsertAlertDetails(rows) {
            calls.push(['insert', rows]);
            if (failure) throw failure;
            return { affectedRows, insertedAlerts: rows.slice(0, rows.length-skippedCount), skippedCount };
        } }
    };
    const context = { module: {exports:{}}, process:{env:{}},
        console:{log(){},warn(){},error(){}}, require(name){
            if (name === '../utils/vehicleAlertIdentity') return require('../src/utils/vehicleAlertIdentity');
            if (name === '../utils/filterAlertLocations') return require('../src/utils/filterAlertLocations');
            return mocks[name];
        } };
    vm.runInNewContext(fs.readFileSync('src/services/AlertBatchService.js','utf8'),context);
    return { service:context.module.exports, calls, key, notifications };
}

test('alert batches acknowledge Redis only after the complete database insert', async () => {
    const s=setup(); await s.service.processAlertBatch();
    assert.deepEqual(s.calls.map(c=>c[0]),['insert','ack']);
});

test('duplicate events in an existing batch are inserted once and acknowledged correctly', async () => {
    const row={last_location:'23,85',vehicle_no:'JH13D9661',trip_id:40371,alert_id:13,last_date_time:'2026-10-23 12:00:00'};
    const s=setup({alerts:[row,{...row}],affectedRows:1});
    await s.service.processAlertBatch();
    assert.equal(s.calls[0][1].length,1);
    assert.deepEqual(s.calls.map(c=>c[0]),['insert','ack']);
});

test('transient errors retry only when the transaction is confirmed rolled back', async () => {
    for(const rolledBack of [true,false]) {
        const failure=Object.assign(new Error('deadlock'),{code:'ER_LOCK_DEADLOCK',transactionRolledBack:rolledBack});
        const s=setup({failure}); await s.service.processAlertBatch();
        assert.deepEqual(s.calls.map(c=>c[0]),rolledBack?['insert','retry']:['insert']);
    }
});

test('unknown commit, permanent SQL errors, and acknowledgement failure never replay inserts', async () => {
    for(const options of [
        {failure:Object.assign(new Error('connection lost during commit'),{code:'ECONNRESET',transactionRolledBack:false})},
        {failure:Object.assign(new Error('bad SQL'),{code:'ER_BAD_FIELD_ERROR',transactionRolledBack:true})},
        {acknowledge:false}, {affectedRows:1}
    ]) {
        const s=setup(options); await s.service.processAlertBatch();
        assert.equal(s.calls.some(c=>c[0]==='retry'),false);
    }
});

 test('mixed location batch inserts only valid rows and acknowledges the whole claim', async () => {
    const s = setup({alerts:[{last_location:null},{last_location:'23,85'},{last_location:'23,'}],affectedRows:1});
    await s.service.processAlertBatch();
    assert.equal(s.calls[0][1].length,1);
    assert.deepEqual(s.calls.map(c=>c[0]),['insert','ack']);
 });
 test('all invalid locations acknowledge the claim without a database insert', async () => {
    const s = setup({alerts:[{last_location:null},{}]});
    await s.service.processAlertBatch();
    assert.deepEqual(s.calls.map(c=>c[0]),['ack']);
 });

test('GPS alert notifications run after successful acknowledgement only', async () => {
 const s=setup();await s.service.processAlertBatch();
 assert.equal(s.notifications.length,1);assert.equal(s.notifications[0].afterAck,true);
 assert.equal(s.notifications[0].rows.length,2);
 const failed=setup({acknowledge:false});await failed.service.processAlertBatch();assert.equal(failed.notifications.length,0);
});

test('manually completed queued alerts are acknowledged without notifications', async () => {
 const s=setup({affectedRows:0,skippedCount:2});await s.service.processAlertBatch();
 assert.deepEqual(s.calls.map(c=>c[0]),['insert','ack']);assert.equal(s.notifications.length,0);
});
test('mixed active and completed trips notify only inserted active alerts', async () => {
 const s=setup({affectedRows:1,skippedCount:1});await s.service.processAlertBatch();
 assert.deepEqual(s.calls.map(c=>c[0]),['insert','ack']);
 assert.equal(s.notifications.length,1);assert.equal(s.notifications[0].rows.length,1);
 assert.equal(s.notifications[0].rows[0].trip_id,1);
});
