'use strict';
const PQueue = require('p-queue').default;
const { setImmediate } = require('node:timers/promises');
const { integer } = require('../db/settings');
const shared = new PQueue({ concurrency: integer('PACKET_CONCURRENCY', 8, 1, 100) });
const queues = new Map();
function getVehicleQueue(vehicleNo) {
    if (!queues.has(vehicleNo)) {
        const serial = new PQueue({ concurrency: 1 });
        const queue = { add(work) {
            return serial.add(() => shared.add(async () => {
                await setImmediate();
                return work();
            }));
        } };
        serial.on('idle', () => {
            if (!serial.size && !serial.pending && queues.get(vehicleNo) === queue) queues.delete(vehicleNo);
        });
        queues.set(vehicleNo, queue);
    }
    return queues.get(vehicleNo);
}
module.exports = { getVehicleQueue };
