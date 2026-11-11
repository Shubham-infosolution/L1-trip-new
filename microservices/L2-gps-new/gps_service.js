'use strict';

const dotenv = require("dotenv");

dotenv.config({
    quiet: true
});

console.log(`Starting ${process.env.SERVICE_NAME}...`);

/*
|--------------------------------------------------------------------------
| MQTT Subscriber
|--------------------------------------------------------------------------
*/
const startSubscriber =
    require("./src/mqtt/subscriber");

/*
|--------------------------------------------------------------------------
| Alert Scheduler
|--------------------------------------------------------------------------
*/
const batchWorker = require("./src/utils/batchWorker")();
const stopMonitor = require("./src/utils/eventLoopMonitor")("mqtt");

/*
|--------------------------------------------------------------------------
| Common Alert Batch Service
|--------------------------------------------------------------------------
| All alerts except packet service are pushed into:
| vehicle_alert_queue
|
| AlertBatchService processes the common queue and performs
| bulk insert into tbl_vehicle_alert_details.
|--------------------------------------------------------------------------
*/
// Alert/notification writers and disconnected checks run in the child worker.

/*
|--------------------------------------------------------------------------
| Database / Batch / Cron / Redis
|--------------------------------------------------------------------------
*/
const Database =
    require("./src/model/dbquery");

const writeBatch =
    require("./src/db/writeBatch");

const managedCron =
    require("./src/utils/managedCron");

const RedisRepository =
    require("./src/repositories/RedisRepository");


/*
|--------------------------------------------------------------------------
| Start MQTT Subscriber
|--------------------------------------------------------------------------
*/
startSubscriber();


/*
|--------------------------------------------------------------------------
| Start Alert Scheduler
|--------------------------------------------------------------------------
*/
// The batch worker starts its own schedules.


/*
|--------------------------------------------------------------------------
| Graceful Shutdown
|--------------------------------------------------------------------------
*/

let shuttingDown = false;

async function shutdown(signal) {

    if (shuttingDown) {
        return;
    }

    shuttingDown = true;

    console.log(
        `[Shutdown] ${signal}: draining active work`
    );

    const timeout = setTimeout(() => {

        console.error(
            "[Shutdown] ❌ DRAIN TIMEOUT"
        );

        process.exit(1);

    }, Number(
        process.env.SHUTDOWN_TIMEOUT_MS || 30000
    ));

    try {

        console.log("[Shutdown] STEP 1: stopping MQTT...");

        await startSubscriber.stop();

        console.log("[Shutdown] STEP 1 DONE: MQTT stopped");

        await batchWorker.stop();
        stopMonitor();


        console.log("[Shutdown] STEP 2: stopping managed cron...");

        await managedCron.stopAll();

        console.log("[Shutdown] STEP 2 DONE: cron stopped");


        console.log("[Shutdown] STEP 3: closing writeBatch...");

        await writeBatch.close();

        console.log("[Shutdown] STEP 3 DONE: writeBatch closed");


        console.log("[Shutdown] STEP 4: closing database...");

        await Database.close();

        console.log("[Shutdown] STEP 4 DONE: database closed");


        console.log("[Shutdown] STEP 5: closing Redis...");

        await RedisRepository.quit();

        console.log("[Shutdown] STEP 5 DONE: Redis closed");


        clearTimeout(timeout);

        console.log(
            "[Shutdown] ✅ Graceful shutdown completed"
        );

        process.exitCode = 0;

    } catch (error) {

        clearTimeout(timeout);

        console.error(
            "[Shutdown] ❌ ERROR:",
            error
        );

        process.exit(1);
    }
}


/*
|--------------------------------------------------------------------------
| Process Signals
|--------------------------------------------------------------------------
*/

process.once(
    "SIGTERM",
    () => shutdown("SIGTERM")
);

process.once(
    "SIGINT",
    () => shutdown("SIGINT")
);
