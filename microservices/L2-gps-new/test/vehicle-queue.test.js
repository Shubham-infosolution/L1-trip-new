'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { setImmediate } = require('node:timers/promises');
process.env.PACKET_CONCURRENCY = '2';
const { getVehicleQueue } = require('../src/utils/vehicleQueue');

test('global packet concurrency stays bounded while each vehicle retains order', async () => {
    let active = 0, max = 0;
    const running = new Set(), order = [];
    const work = (vehicle, index) => getVehicleQueue(vehicle).add(async () => {
        assert.equal(running.has(vehicle), false);
        running.add(vehicle);
        active++; max = Math.max(max, active);
        await setImmediate();
        order.push(`${vehicle}:${index}`);
        running.delete(vehicle); active--;
    });
    await Promise.all(['A','B','C','D'].flatMap(v => [work(v, 1), work(v, 2)]));
    assert.equal(max, 2);
    for (const v of ['A','B','C','D']) assert.ok(order.indexOf(`${v}:1`) < order.indexOf(`${v}:2`));
});

test('failed packet does not block the next packet or leak concurrency slots', async () => {
    const queue = getVehicleQueue('E');
    const bad = queue.add(async () => { throw new Error('packet failed'); });
    const good = queue.add(async () => 42);
    await assert.rejects(bad, /packet failed/);
    assert.equal(await good, 42);
    await setImmediate();
    assert.notEqual(getVehicleQueue('E'), queue);
});
