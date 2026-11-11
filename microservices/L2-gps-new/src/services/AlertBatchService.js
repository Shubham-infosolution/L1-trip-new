const cron = require("../utils/managedCron");
const vehicleAlertIdentity = require('../utils/vehicleAlertIdentity');
const filterAlertLocations = require('../utils/filterAlertLocations');

const RedisRepository =
    require("../repositories/RedisRepository");

const TripRepository =
    require("../repositories/TripRepository");


class AlertBatchService {

    // =========================================================
    // CONFIG
    // =========================================================

    static ALERT_BATCH_CRON_TIME =
        process.env.ALERT_BATCH_CRON_TIME ||
        "*/30 * * * * *";

    static ALERT_BATCH_SIZE =
        Number(
            process.env.ALERT_BATCH_SIZE ||
            1000
        );


    // =========================================================
    // PROCESS ALL ALERTS
    // =========================================================

    static async processAlertBatch() {

        let processingKey = null;

        try {

            // console.log(
            //     "[AlertBatch] PROCESS START"
            // );

            // =====================================================
            // CHECK QUEUE
            // =====================================================

            const queueLength =
                await RedisRepository
                    .getVehicleAlertQueueLength();

            // console.log(
            //     "[AlertBatch] QUEUE LENGTH:",
            //     queueLength
            // );

            if (!queueLength) {

                // console.log(
                //     "[AlertBatch] NO ALERTS"
                // );

                return;
            }


            // =====================================================
            // MOVE COMMON ALERT QUEUE TO PROCESSING
            // =====================================================

            const batch =
                await RedisRepository
                    .moveVehicleAlertsToProcessing(
                        this.ALERT_BATCH_SIZE
                    );

            // console.log(
            //     "[AlertBatch] MOVED TO PROCESSING | COUNT:",
            //     batch?.alerts?.length || 0,
            //     "| KEY:",
            //     batch?.key
            // );


            if (
                !batch ||
                !Array.isArray(batch.alerts) ||
                batch.alerts.length === 0
            ) {

                // console.log(
                //     "[AlertBatch] NO ALERTS IN PROCESSING QUEUE"
                // );

                return;
            }


            processingKey =
                batch.key;

            // Existing queues may predate enqueue deduplication.
            const seen = new Set();
            const validAlerts = filterAlertLocations(batch.alerts);
            const uniqueAlerts = validAlerts.filter(alert => {
                const identity = vehicleAlertIdentity(alert);
                if (!identity) return true;
                if (seen.has(identity)) return false;
                seen.add(identity);
                return true;
            });

            const result = uniqueAlerts.length === 0
                ? { affectedRows: 0, insertedAlerts: [], skippedCount: 0 }
                : await TripRepository
                    .bulkInsertAlertDetails(
                        uniqueAlerts
                    );


            // =====================================================
            // INSERT CONFIRMATION
            // =====================================================

            const insertedAlerts = result?.insertedAlerts;
            if (!Array.isArray(insertedAlerts) ||
                insertedAlerts.length + result.skippedCount !== uniqueAlerts.length) {
                throw new Error('Alert insert/skip result mismatch');
            }
            const expectedCount = insertedAlerts.length;

            const insertedCount =
                Number(
                    result?.affectedRows || 0
                );

            if (
                insertedCount !==
                expectedCount
            ) {

                throw new Error(
                    `Alert insert mismatch | ` +
                    `Expected: ${expectedCount} | ` +
                    `Inserted: ${insertedCount}`
                );
            }


            // =====================================================
            // DELETE PROCESSING QUEUE
            // =====================================================

            const acknowledged = await RedisRepository
                .deleteVehicleAlertProcessingQueue(
                    processingKey
                );

            if (!acknowledged) throw new Error('Alert batch committed but Redis acknowledgement failed');

            processingKey = null;

            // Alert INSERTs are committed and acknowledged; notification errors must not replay them.
            try {
                if (insertedAlerts.length) await require('./alertNotifications')(insertedAlerts);
            } catch (error) {
                console.error('[AlertBatch] Alerts saved but notification delivery failed', error);
            }



            // =====================================================
            // REMAINING QUEUE
            // =====================================================

            const remainingQueue =
                await RedisRepository
                    .getVehicleAlertQueueLength();


            // console.log(
            //     "[AlertBatch] REMAINING QUEUE:",
            //     remainingQueue
            // );


            // console.log(
            //     "[AlertBatch] PROCESS COMPLETE"
            // );

        } catch (error) {

            const transientCodes = new Set([
                'ER_LOCK_DEADLOCK', 'ER_LOCK_WAIT_TIMEOUT', 'ECONNRESET',
                'ETIMEDOUT', 'ECONNREFUSED', 'ER_CON_COUNT_ERROR', 'PROTOCOL_CONNECTION_LOST'
            ]);
            if (processingKey && error.transactionRolledBack === true && transientCodes.has(error.code)) {
                try {
                    await RedisRepository.retryVehicleAlertBatch(processingKey);
                    console.warn('[AlertBatch] Rolled-back batch returned to queue for retry:', processingKey);
                    return;
                } catch (retryError) {
                    console.error('[AlertBatch] Retry queue failed; processing batch retained:', processingKey, retryError);
                }
            }

            // =====================================================
            // IMPORTANT
            // =====================================================

            console.error(
                "[AlertBatch] ERROR:",
                error
            );


            if (processingKey) {

                console.error(
                    "[AlertBatch] PROCESSING QUEUE RETAINED | KEY:",
                    processingKey
                );
            }
        }
    }


    // =========================================================
    // START CRON
    // =========================================================

    static startAlertBatchCron() {

        cron.schedule(
            this.ALERT_BATCH_CRON_TIME,
            async () => {

                // console.log(
                //     "[AlertBatch] CRON TICK"
                // );

                try {

                    await this.processAlertBatch();

                } catch (error) {

                    console.error(
                        "[AlertBatch] CRON ERROR:",
                        error
                    );

                }

            }
        );


        // console.log(
        //     "[AlertBatch] Common alert writer started"
        // );
    }
}


// =============================================================
// START ALERT BATCH CRON
// =============================================================

AlertBatchService.startAlertBatchCron();


module.exports =
    AlertBatchService;
