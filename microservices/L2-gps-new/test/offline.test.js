'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const moment = require('moment');
const { createRequire } = require('node:module');
const path = require('node:path');

function load(file, mocks) {
    const context = { module: { exports: {} }, console, process: { env: {} },
        require(name) { return Object.hasOwn(mocks, name) ? mocks[name] : createRequire(path.resolve(file))(name); } };
    vm.runInNewContext(fs.readFileSync(file, 'utf8'), context, { filename: file });
    return context.module.exports;
}

function setup() {
    const values = new Map();
    let fail = false;
    const Redis = load('src/repositories/RedisRepository.js', {
        ioredis: class {
            on() {}
            async get(key) { if (fail) throw new Error('Redis unavailable'); return values.get(key); }
            async set(key, value) { if (fail) throw new Error('Redis unavailable'); values.set(key, value); }
        }
    });
    const trip = { trip_id: 40371, trip_status: 1 };
    const Trips = {
        async getActiveTrip() { return [{ trip_id: 35467, trip_status: 2 }, trip]; },
        async getAlertConfig() { return { alert_time: 5 }; }
    };
    const Service = load('src/services/offline.service.js', {
        '../repositories/RedisRepository': Redis,
        '../repositories/TripRepository': Trips,
        '../helpers/geoHelper': { async formatServerTime() {
            return moment().subtract(20, 'minutes').format('YYYY-MM-DD HH:mm:ss');
        } }
    });
    Redis.getLatestPacket = async () => ({ gps_timestamp: 123 });
    return { Redis, Service, values, Trips, failRedis() { fail = true; } };
}

test('offline state persists independently for each vehicle and trip', async () => {
    const { Redis, values, failRedis } = setup();
    assert.equal(await Redis.getOfflineState('JH13D9661:40371'), null);
    await Redis.saveOfflineState('JH13D9661:40371', { alertSent: true, offline_start_timestamp: 123 });
    assert.equal((await Redis.getOfflineState('JH13D9661:40371')).offline_start_timestamp, 123);
    assert.equal(await Redis.getOfflineState('JH13D9661:35467'), null);
    assert.equal(values.has('offline:JH13D9661:40371'), true);
    failRedis();
    await assert.rejects(Redis.getOfflineState('JH13D9661:40371'), /Redis unavailable/);
    await assert.rejects(Redis.saveOfflineState('JH13D9661:40371', {}), /Redis unavailable/);
});

test('offline service accepts active-trip arrays and suppresses duplicate alerts', async () => {
    const { Service, Redis } = setup();
    const ids = [];
    Service.saveOfflineAlert = async trip => { ids.push(trip.trip_id); return true; };
    await Service.process({ vehicle_no: 'JH13D9661' }, {});
    await Service.process({ vehicle_no: 'JH13D9661' }, {});
    assert.deepEqual(ids, [40371]);
    assert.equal((await Redis.getOfflineState('JH13D9661:40371')).alertSent, true);
});

test('failed enqueue leaves offline state eligible for retry', async () => {
    const { Service, Redis } = setup();
    let attempts = 0;
    Service.saveOfflineAlert = async () => ++attempts > 1;
    await Service.process({ vehicle_no: 'JH13D9661' }, {});
    assert.equal((await Redis.getOfflineState('JH13D9661:40371')).alertSent, false);
    await Service.process({ vehicle_no: 'JH13D9661' }, {});
    assert.equal(attempts, 2);
    assert.equal((await Redis.getOfflineState('JH13D9661:40371')).alertSent, true);
});

test('completed trips do not generate offline alerts', async () => {
    const { Service, Trips } = setup();
    Trips.getActiveTrip = async () => [{ trip_id: 35467, trip_status: 2 }];
    let calls = 0;
    Service.saveOfflineAlert = async () => { calls++; return true; };
    await Service.process({ vehicle_no: 'JH13D9661' }, {});
    assert.equal(calls, 0);
});
