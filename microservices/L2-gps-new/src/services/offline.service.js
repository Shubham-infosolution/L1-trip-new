const alertLocation = require("../utils/alertLocation");
const moment = require("moment");

const TripRepository = require("../repositories/TripRepository");
const RedisRepository = require("../repositories/RedisRepository");
const geoHelper = require("../helpers/geoHelper");


class OfflineService {

    // =========================================================
    // ALERT IDS
    // =========================================================

    static OFFLINE_ALERT_ID = 3;
    static ONLINE_ALERT_ID = 15;


    // =========================================================
    // PROCESS OFFLINE / ONLINE
    //
    // GPS Packet
    //      ↓
    // OfflineService
    //      ↓
    // vehicle_alert_queue
    //      ↓
    // AlertBatchService
    //      ↓
    // tbl_vehicle_alert_details
    //
    // No separate Offline queue
    // No separate Offline batch
    // No direct DB insert
    // No tbl_vehicle_alert_event
    // No alert_path
    // last_location = latitude,longitude
    // =========================================================

    static async process(vehicle, payload) {

        try {

            // =====================================================
            // VEHICLE NUMBER
            // =====================================================

            const vehicleNo =
                String(
                    vehicle?.vehicle_no || ""
                ).trim();

            if (!vehicleNo) {
                return;
            }


            // =====================================================
            // ACTIVE TRIP
            // =====================================================

            const activeTrips =
                await TripRepository.getActiveTrip(
                    vehicleNo
                );

            const activeTrip = Array.isArray(activeTrips)
                ? activeTrips.find(trip => Number(trip?.trip_status) === 1)
                : activeTrips;

            if (
                !activeTrip ||
                Number(activeTrip.trip_status) !== 1
            ) {
                return;
            }


            // =====================================================
            // OFFLINE ALERT CONFIG
            // =====================================================

            const offlineAlert =
                await TripRepository.getAlertConfig(
                    this.OFFLINE_ALERT_ID
                );

            if (!offlineAlert) {

                return;
            }


            // =====================================================
            // ALERT TIME
            // =====================================================

            const alertTime =
                Number(
                    offlineAlert.alert_time || 0
                );

            if (alertTime <= 0) {

                return;
            }


            // =====================================================
            // GET LATEST PACKET FROM REDIS
            // =====================================================

            const latestPacket =
                await RedisRepository.getLatestPacket(
                    vehicleNo
                );

            if (!latestPacket) {
                return;
            }


            // =====================================================
            // GPS PACKET TIME
            // =====================================================

            const formattedPacketTime =
                await geoHelper.formatServerTime(
                    latestPacket.gps_timestamp
                );

            if (!formattedPacketTime) {

                console.error(
                    `[Offline] INVALID GPS TIME | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `GPS_TIMESTAMP: ${latestPacket?.gps_timestamp}`
                );

                return;
            }


            // =====================================================
            // PARSE PACKET TIME
            // =====================================================

            const packetReceiveTime =
                moment(
                    formattedPacketTime,
                    "YYYY-MM-DD HH:mm:ss",
                    true
                );

            if (!packetReceiveTime.isValid()) {

                console.error(
                    `[Offline] INVALID PACKET TIME | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TIME: ${formattedPacketTime}`
                );

                return;
            }


            // =====================================================
            // CURRENT TIME
            // =====================================================

            const currentTime = moment();


            // =====================================================
            // OFFLINE DURATION
            // =====================================================

            const diffMinutes =
                Math.max(
                    0,
                    currentTime.diff(
                        packetReceiveTime,
                        "minutes"
                    )
                );


            // =====================================================
            // REDIS STATE KEY
            //
            // Vehicle + Trip
            // =====================================================

            const redisKey =
                `${vehicleNo}:${activeTrip.trip_id}`;


            // =====================================================
            // GET OFFLINE STATE
            // =====================================================

            let state =
                await RedisRepository.getOfflineState(
                    redisKey
                );


            // =====================================================
            // INITIAL STATE
            // =====================================================

            if (!state) {

                state = {
                    alertSent: false,
                    offline_start_timestamp:
                        latestPacket.gps_timestamp
                };

                await RedisRepository.saveOfflineState(
                    redisKey,
                    state
                );
            }


            // =====================================================
            // OFFLINE CONDITION
            // =====================================================

            if (
                diffMinutes >= alertTime
            ) {

                // =================================================
                // ALREADY OFFLINE
                //
                // Don't generate duplicate alert
                // =================================================

                if (state.alertSent) {

                    return;
                }


                // =================================================
                // GENERATE OFFLINE ALERT
                // =================================================

                const queued =
                    await this.saveOfflineAlert(
                        activeTrip,
                        vehicle,
                        latestPacket,
                        offlineAlert,
                        diffMinutes,
                        formattedPacketTime
                    );


                // =================================================
                // QUEUE FAILED
                //
                // Don't change state.
                // Next execution will retry.
                // =================================================

                if (!queued) {

                    console.error(
                        `[Offline] OFFLINE ALERT QUEUE FAILED | ` +
                        `VEHICLE: ${vehicleNo} | ` +
                        `TRIP: ${activeTrip.trip_id}`
                    );

                    return;
                }


                // =================================================
                // SAVE OFFLINE STATE
                // =================================================

                state.alertSent = true;

                state.offline_start_timestamp =
                    latestPacket.gps_timestamp;


                await RedisRepository.saveOfflineState(
                    redisKey,
                    state
                );

                return;
            }


            // =====================================================
            // ONLINE CONDITION
            //
            // Previously offline
            // Now packet is received again
            // =====================================================

            if (state.alertSent) {

                // =================================================
                // GENERATE ONLINE ALERT
                // =================================================

                const queued =
                    await this.saveOnlineAlert(
                        activeTrip,
                        vehicle,
                        latestPacket
                    );


                // =================================================
                // QUEUE FAILED
                //
                // Don't reset state.
                // Retry on next packet.
                // =================================================

                if (!queued) {

                    console.error(
                        `[Offline] ONLINE ALERT QUEUE FAILED | ` +
                        `VEHICLE: ${vehicleNo} | ` +
                        `TRIP: ${activeTrip.trip_id}`
                    );

                    return;
                }


                // =================================================
                // RESET OFFLINE STATE
                // =================================================

                state = {
                    alertSent: false,
                    offline_start_timestamp:
                        latestPacket.gps_timestamp
                };


                await RedisRepository.saveOfflineState(
                    redisKey,
                    state
                );

            }

        } catch (error) {

            console.error(
                `${vehicle?.vehicle_no || "UNKNOWN"} | ` +
                `OFFLINE SERVICE ERROR:`,
                error
            );
        }
    }


    // =========================================================
    // SAVE OFFLINE ALERT
    //
    // COMMON REDIS QUEUE
    //
    // vehicle_alert_queue
    //
    // alert_id = 3
    // last_location = latitude,longitude
    // =========================================================

    static async saveOfflineAlert(
        activeTrip,
        vehicle,
        payload,
        offlineAlert,
        offlineMinutes,
        packetReceiveTime
    ) {

        try {

            const vehicleNo =
                String(
                    vehicle?.vehicle_no || ""
                ).trim();

            if (!vehicleNo) {
                return false;
            }


            // =====================================================
            // MESSAGE
            // =====================================================

            const message =
                `${offlineAlert?.alert_name || "Offline Alert"}\n` +
                `Vehicle : ${vehicleNo}\n` +
                `Trip ID : ${activeTrip?.trip_unique_id || activeTrip?.trip_id}\n` +
                `Last Packet Time : ${packetReceiveTime}\n` +
                `Offline Duration : ${offlineMinutes} Minutes\n` +
                `Vehicle is currently offline.`;


            // =====================================================
            // COMMON VEHICLE ALERT QUEUE
            // =====================================================

            const queued =
                await RedisRepository.addVehicleAlert({

                    trip_id:
                        activeTrip?.trip_id ?? null,

                    subtrip_id:
                        activeTrip?.sub_trip_id ??
                        activeTrip?.subtrip_id ??
                        null,

                    vehicle_no:
                        vehicleNo,

                    alert_id:
                        this.OFFLINE_ALERT_ID,

                    message:
                        message,

                    last_location: alertLocation(payload),

                    last_date_time:
                        packetReceiveTime,

                    long_halt_minutes:
                        offlineMinutes,

                    speed:
                        null,

                    district_id:
                        activeTrip?.district_id ??
                        vehicle?.district_id ??
                        null,

                    status:
                        1
                });


            // =====================================================
            // QUEUE RESULT
            // =====================================================

            if (!queued) {

                console.error(
                    `[Offline] REDIS QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `ALERT_ID: ${this.OFFLINE_ALERT_ID}`
                );

                return false;
            }

            return true;

        } catch (error) {

            console.error(
                `[Offline] SAVE OFFLINE ALERT ERROR | ` +
                `VEHICLE: ${vehicle?.vehicle_no || "UNKNOWN"}`,
                error
            );

            return false;
        }
    }


    // =========================================================
    // SAVE ONLINE ALERT
    //
    // SAME COMMON REDIS QUEUE
    //
    // vehicle_alert_queue
    //
    // alert_id = 15
    // last_location = latitude,longitude
    // =========================================================

    static async saveOnlineAlert(
        activeTrip,
        vehicle,
        latestPacket
    ) {

        try {

            const vehicleNo =
                String(
                    vehicle?.vehicle_no || ""
                ).trim();

            if (!vehicleNo) {
                return false;
            }


            // =====================================================
            // PACKET TIME
            // =====================================================

            const packetTime =
                await geoHelper.formatServerTime(
                    latestPacket?.gps_timestamp
                );

            if (!packetTime) {

                console.error(
                    `[Offline] ONLINE TIME INVALID | ` +
                    `VEHICLE: ${vehicleNo}`
                );

                return false;
            }


            // =====================================================
            // MESSAGE
            // =====================================================

            const message =
                `Vehicle ${vehicleNo} is back online.\n` +
                `Trip ID : ${activeTrip?.trip_unique_id || activeTrip?.trip_id}\n` +
                `Online Time : ${packetTime}`;


            // =====================================================
            // COMMON VEHICLE ALERT QUEUE
            // =====================================================

            const queued =
                await RedisRepository.addVehicleAlert({

                    trip_id:
                        activeTrip?.trip_id ?? null,

                    subtrip_id:
                        activeTrip?.sub_trip_id ??
                        activeTrip?.subtrip_id ??
                        null,

                    vehicle_no:
                        vehicleNo,

                    alert_id:
                        this.ONLINE_ALERT_ID,

                    message:
                        message,

                    last_location: alertLocation(latestPacket),

                    last_date_time:
                        packetTime,

                    long_halt_minutes:
                        null,

                    speed:
                        Number(
                            latestPacket?.speed || 0
                        ),

                    district_id:
                        activeTrip?.district_id ??
                        vehicle?.district_id ??
                        null,

                    status:
                        1
                });


            // =====================================================
            // QUEUE RESULT
            // =====================================================

            if (!queued) {

                console.error(
                    `[Offline] ONLINE REDIS QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `ALERT_ID: ${this.ONLINE_ALERT_ID}`
                );

                return false;
            }

            return true;

        } catch (error) {

            console.error(
                `[Offline] SAVE ONLINE ALERT ERROR | ` +
                `VEHICLE: ${vehicle?.vehicle_no || "UNKNOWN"}`,
                error
            );

            return false;
        }
    }
}


module.exports = OfflineService;
