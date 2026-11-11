const alertLocation = require("../utils/alertLocation");
"use strict";

const TripRepository =
    require("../repositories/TripRepository");

const RedisRepository =
    require("../repositories/RedisRepository");

const geoHelper =
    require("../helpers/geoHelper");


class OverSpeedService {

    static OVER_SPEED_ALERT_ID = 7;


    // =========================================================
    // PROCESS OVER SPEED
    //
    // GPS Packet
    //      ↓
    // OverSpeedService
    //      ↓
    // RedisRepository.addVehicleAlert()
    //      ↓
    // vehicle_alert_queue
    //      ↓
    // AlertBatchService
    //      ↓
    // tbl_vehicle_alert_details
    //
    // NO direct DB insert
    // NO separate OverSpeed queue
    // NO separate OverSpeed batch
    // NO separate OverSpeed cron
    // =========================================================

    static async process(
        vehicle,
        payload
    ) {

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
            // GPS TIMESTAMP
            // =====================================================

            if (
                payload?.gps_timestamp === undefined ||
                payload?.gps_timestamp === null
            ) {
                return;
            }


            const gpsTimestamp =
                Number(
                    payload.gps_timestamp
                );

            if (
                !Number.isFinite(gpsTimestamp) ||
                gpsTimestamp <= 0
            ) {
                return;
            }


            // =====================================================
            // ACTIVE TRIPS
            // =====================================================
            //
            // getActiveTrip() can return multiple active trips.
            // Process every active trip.
            // =====================================================

            let activeTrips =
                await TripRepository.getActiveTrip(
                    vehicleNo
                );


            // Backward compatibility
            if (
                activeTrips &&
                !Array.isArray(activeTrips)
            ) {
                activeTrips = [activeTrips];
            }


            if (
                !Array.isArray(activeTrips) ||
                activeTrips.length === 0
            ) {
                return;
            }


            // =====================================================
            // PROCESS EACH ACTIVE TRIP
            // =====================================================

            for (
                const activeTrip of activeTrips
            ) {

                if (!activeTrip) {
                    continue;
                }

                try {

                    await this.processTrip(
                        vehicle,
                        payload,
                        activeTrip
                    );

                } catch (tripError) {

                    console.error(
                        `[OverSpeed] TRIP PROCESS ERROR | ` +
                        `VEHICLE: ${vehicleNo} | ` +
                        `TRIP: ${
                            activeTrip?.trip_id ||
                            activeTrip?.id ||
                            "N/A"
                        }`,
                        tripError
                    );
                }
            }


        } catch (error) {

            console.error(
                "OVER SPEED ERROR:",
                error
            );
        }
    }


    // =========================================================
    // PROCESS SINGLE ACTIVE TRIP
    // =========================================================

    static async processTrip(
        vehicle,
        payload,
        activeTrip
    ) {

        // =====================================================
        // TRIP STATUS
        // =====================================================

        if (
            Number(
                activeTrip?.trip_status
            ) !== 1
        ) {
            return;
        }


        // =====================================================
        // ALERT CONFIG
        // =====================================================

        const overSpeedAlert =
            await TripRepository.getAlertConfig(
                this.OVER_SPEED_ALERT_ID
            );

        if (!overSpeedAlert) {
            return;
        }


        // =====================================================
        // CURRENT SPEED
        // =====================================================

        const speed =
            Number(
                payload?.speed || 0
            );

        if (
            !Number.isFinite(speed)
        ) {
            return;
        }


        // =====================================================
        // SPEED LIMIT
        // =====================================================

        const speedLimit =
            Number(
                overSpeedAlert.alert_time
            );


        // =====================================================
        // INVALID SPEED LIMIT
        // =====================================================

        if (
            !Number.isFinite(speedLimit) ||
            speedLimit <= 0
        ) {
            return;
        }


        // =====================================================
        // SERVER TIME
        // =====================================================

        const packetReceiveTime =
            await geoHelper.formatServerTime(
                payload?.gps_timestamp
            );

        if (!packetReceiveTime) {
            return;
        }


        // =====================================================
        // REDIS STATE KEY
        //
        // Trip-wise state is important when multiple
        // active trips exist for same vehicle.
        // =====================================================

        const tripId =
            activeTrip?.id ??
            activeTrip?.trip_id;

        if (!tripId) {
            return;
        }


        const redisKey =
            `${vehicle.vehicle_no}:${tripId}`;


        // =====================================================
        // PREVIOUS STATE
        // =====================================================

        let state =
            await RedisRepository.getOverSpeedState(
                redisKey
            );


        // =====================================================
        // INITIAL STATE
        // =====================================================

        if (!state) {

            state = {

                alertSent:
                    false,

                alertCount:
                    0,

                speed:
                    0,

                gps_timestamp:
                    null
            };
        }


        // =====================================================
        // VEHICLE NORMAL
        // =====================================================

        if (
            speed <= speedLimit
        ) {

            if (
                state.alertSent
            ) {

                state.alertSent =
                    false;

                state.speed =
                    speed;

                state.gps_timestamp =
                    payload.gps_timestamp;


                await RedisRepository.saveOverSpeedState(
                    redisKey,
                    state
                );
            }

            return;
        }


        // =====================================================
        // MAXIMUM 3 ALERTS
        // =====================================================

        if (
            Number(
                state.alertCount || 0
            ) >= 3
        ) {
            return;
        }


        // =====================================================
        // ALREADY ALERTED
        //
        // Vehicle must first come back below speed limit
        // before another OverSpeed alert can be generated.
        // =====================================================

        if (
            state.alertSent
        ) {
            return;
        }


        // =====================================================
        // SAVE / QUEUE ALERT
        // =====================================================

        const saved =
            await this.saveOverSpeedAlert(
                activeTrip,
                vehicle,
                payload,
                overSpeedAlert,
                speed,
                packetReceiveTime
            );


        // =====================================================
        // QUEUE FAILED
        //
        // DO NOT UPDATE STATE
        // Next packet will retry.
        // =====================================================

        if (!saved) {
            return;
        }


        // =====================================================
        // UPDATE REDIS STATE
        // =====================================================

        state.alertSent =
            true;

        state.alertCount =
            Number(
                state.alertCount || 0
            ) + 1;

        state.speed =
            speed;

        state.gps_timestamp =
            payload.gps_timestamp;


        await RedisRepository.saveOverSpeedState(
            redisKey,
            state
        );
    }


    // =========================================================
    // SAVE OVER SPEED ALERT
    //
    // COMMON REDIS QUEUE
    //
    // alert_id = 7
    // last_location = latitude,longitude
    // no alert_path
    // =========================================================

    static async saveOverSpeedAlert(
        activeTrip,
        vehicle,
        payload,
        overSpeedAlert,
        speed,
        packetReceiveTime
    ) {

        try {

            if (!activeTrip) {
                return false;
            }


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
                `${overSpeedAlert.alert_name}\n` +
                `Vehicle : ${vehicleNo}\n` +
                `Trip ID : ${
                    activeTrip?.trip_id ||
                    activeTrip?.id ||
                    "N/A"
                }\n` +
                `Speed : ${speed} Km/h\n` +
                `Time : ${packetReceiveTime}\n` +
                `Vehicle is moving above the permitted speed limit.`;


            // =====================================================
            // COMMON VEHICLE ALERT QUEUE
            // =====================================================

            const queued =
                await RedisRepository.addVehicleAlert({

                    district_id:
                        activeTrip?.district_id ??
                        vehicle?.district_id ??
                        null,

                    vehicle_no:
                        vehicleNo,

                    // Keep existing trip ID mapping.
                    trip_id:
                        activeTrip?.id ??
                        activeTrip?.trip_id ??
                        null,

                    subtrip_id:
                        activeTrip?.sub_trip_id ??
                        activeTrip?.subtrip_id ??
                        null,

                    alert_id:
                        Number(
                            overSpeedAlert?.alert_id ||
                            this.OVER_SPEED_ALERT_ID
                        ),

                    message:
                        message,

                    last_location: alertLocation(payload),

                    last_date_time:
                        packetReceiveTime,

                    long_halt_minutes:
                        null,

                    speed:
                        speed,

                    status:
                        1
                });


            // =====================================================
            // QUEUE FAILED
            // =====================================================

            if (!queued) {

                console.error(
                    `[OverSpeed] COMMON QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TRIP: ${
                        activeTrip?.trip_id ||
                        activeTrip?.id ||
                        "N/A"
                    }`
                );

                return false;
            }

            return true;


        } catch (error) {

            console.error(
                "[OverSpeed] SAVE ALERT ERROR:",
                error
            );

            return false;
        }
    }
}


module.exports =
    OverSpeedService;