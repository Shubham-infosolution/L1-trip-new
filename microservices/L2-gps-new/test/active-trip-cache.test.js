'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');

let data = null, ttl = -2;
const writes = [];
const originalLoad = Module._load;
Module._load = function (name, ...args) {
    if (name === 'ioredis') return class {
        on() {}
        async get() { return data; }
        async ttl() { return ttl; }
        async set(...args) { writes.push(args); return 'OK'; }
    };
    return originalLoad.call(this, name, ...args);
};
const RedisRepository = require('../src/repositories/RedisRepository');
Module._load = originalLoad;

test('permanent and expired active trip snapshots trigger a database fallback', async () => {
    data = JSON.stringify([{ trip_id: 35467, trip_status: 1 }]);
    for (ttl of [-1, -2, 0]) {
        assert.deepEqual(await RedisRepository.getActiveTrip('JH13D9661'), []);
    }
    ttl = 60;
    assert.deepEqual(await RedisRepository.getActiveTrip('JH13D9661'),
        [{ trip_id: 35467, trip_status: 1 }]);
});

test('active trip snapshots retain all trips and expire after 60 seconds', async () => {
    const trips = [{ trip_id: 40371 }, { trip_id: 40372 }];
    assert.equal(await RedisRepository.saveActiveTrip('JH13D9661', 2, trips), true);
    assert.deepEqual(writes.at(-1), ['active_trip:v2:2:JH13D9661', JSON.stringify(trips), 'EX', 60]);
});

test('permanent and expired vehicle cache entries trigger database revalidation', async () => {
    const vehicle = { vehicle_no: 'JH13D9661', level: 2 };
    data = JSON.stringify(vehicle);
    for (ttl of [-1, -2, 0]) {
        assert.equal(await RedisRepository.getVehicleCache(vehicle.vehicle_no), null);
    }
    ttl = 59;
    assert.deepEqual(await RedisRepository.getVehicleCache(vehicle.vehicle_no), vehicle);
    data = null;
    assert.equal(await RedisRepository.getVehicleCache(vehicle.vehicle_no), null);
});

test('vehicle cache writes expire after 60 seconds', async () => {
    const vehicle = { vehicle_no: 'JH13D9661', level: 2 };
    assert.equal(await RedisRepository.setVehicleCache(vehicle.vehicle_no, vehicle), true);
    assert.deepEqual(writes.at(-1), ['vehicle:JH13D9661', JSON.stringify(vehicle), 'EX', 60]);
});
