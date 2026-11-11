const alertLocation = require("../utils/alertLocation");
"use strict";

const TripRepository =
    require("../repositories/TripRepository");

const RouteRepository =
    require("../repositories/routeRepository");

const RedisRepository =
    require("../repositories/RedisRepository");

const geoHelper =
    require("../helpers/geoHelper");


class OffRouteService {

    // =========================================================
    // ALERT IDS
    // =========================================================

    static OFF_ROUTE_ALERT_ID = 5;
    static ON_ROUTE_ALERT_ID = 6;


    // =========================================================
    // MAIN PROCESS
    //
    // GPS Packet
    //      ↓
    // OffRouteService
    //      ↓
    // each active trip
    //      ↓
    // vehicle_alert_queue
    //      ↓
    // AlertBatchService
    //      ↓
    // tbl_vehicle_alert_details
    //
    // NO direct DB insert
    // NO separate OffRoute queue
    // NO separate OnRoute queue
    // NO alert_path
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
            // GPS TIMESTAMP
            // =====================================================

            if (
                payload?.gps_timestamp === undefined ||
                payload?.gps_timestamp === null
            ) {
                return;
            }

            const gpsTimestamp =
                Number(payload.gps_timestamp);

            if (
                !Number.isFinite(gpsTimestamp) ||
                gpsTimestamp <= 0
            ) {
                return;
            }


            // =====================================================
            // GPS LATITUDE / LONGITUDE
            // =====================================================

            const currentLat =
                Number(payload?.latitude);

            const currentLng =
                Number(payload?.longitude);

            if (
                !Number.isFinite(currentLat) ||
                !Number.isFinite(currentLng)
            ) {
                return;
            }


            // =====================================================
            // ACTIVE TRIPS
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


            // =====================================================
            // NO ACTIVE TRIP
            // =====================================================

            if (
                !Array.isArray(activeTrips) ||
                activeTrips.length === 0
            ) {
                return;
            }


            // =====================================================
            // PROCESS EACH ACTIVE TRIP
            // =====================================================

            for (const activeTrip of activeTrips) {

                if (!activeTrip) {
                    continue;
                }

                try {

                    await this.processTrip(
                        vehicle,
                        payload,
                        activeTrip,
                        vehicleNo,
                        currentLat,
                        currentLng,
                        gpsTimestamp
                    );

                } catch (tripError) {

                    console.error(
                        `[OffRoute] TRIP PROCESS ERROR | ` +
                        `VEHICLE: ${vehicleNo} | ` +
                        `TRIP: ${activeTrip?.trip_id || "N/A"}`,
                        tripError
                    );
                }
            }

        } catch (error) {

            console.error(
                `${vehicle?.vehicle_no || "UNKNOWN"} | ` +
                `OFF ROUTE SERVICE ERROR:`,
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
        activeTrip,
        vehicleNo,
        currentLat,
        currentLng,
        gpsTimestamp
    ) {

        // =====================================================
        // TRIP ID
        // =====================================================

        const tripId =
            Number(activeTrip?.trip_id || 0);

        if (!tripId) {
            return;
        }


        // =====================================================
        // TRIP STATUS
        // =====================================================

        if (
            Number(activeTrip?.trip_status) !== 1 ||
            Number(activeTrip?.status ?? 1) !== 1
        ) {

            await RedisRepository.deleteOffRouteState(
                vehicleNo,
                tripId
            );

            return;
        }


        // =====================================================
        // CURRENT ROUTE
        // =====================================================

        const route =
            await RouteRepository.getCurrentRoute(
                activeTrip,
                vehicle?.level
            );

        if (
            !route ||
            !route.polyline
        ) {
            return;
        }


        // =====================================================
        // DECODE ROUTE
        // =====================================================

        const routePoints =
            geoHelper.decodePolyline(
                route.polyline
            );

        if (
            !Array.isArray(routePoints) ||
            routePoints.length === 0
        ) {
            return;
        }


        // =====================================================
        // CALCULATE ROUTE DEVIATION
        // =====================================================

        const deviation =
            geoHelper.getNearestRouteDeviation(
                currentLat,
                currentLng,
                routePoints
            );

        if (
            !Number.isFinite(deviation)
        ) {
            return;
        }


        // =====================================================
        // KM -> METER
        // =====================================================

        const deviationMeter =
            Math.round(
                deviation * 1000
            );


        // =====================================================
        // ALERT CONFIG
        // =====================================================

        const alertConfig =
            await RouteRepository.getOffRouteAlertConfig();

        if (!alertConfig) {
            return;
        }


        // =====================================================
        // OFF ROUTE THRESHOLD
        //
        // alert_time = initial distance threshold
        // =====================================================

        const offRouteThreshold =
            Number(
                alertConfig.alert_time || 0
            );

        if (
            !Number.isFinite(offRouteThreshold) ||
            offRouteThreshold <= 0
        ) {
            return;
        }


        // =====================================================
        // GET TRIP-WISE REDIS STATE
        // =====================================================

        let offRouteState =
            await RedisRepository.getOffRouteState(
                vehicleNo,
                tripId
            );


        // =====================================================
        // VEHICLE BACK ON ROUTE
        // =====================================================

        if (
            deviationMeter <
            offRouteThreshold
        ) {

            // -------------------------------------------------
            // Generate ON ROUTE only if previous state
            // was OFF_ROUTE
            // -------------------------------------------------

            if (
                offRouteState &&
                offRouteState.status === "OFF_ROUTE"
            ) {

                const queued =
                    await this.saveOnRouteAlert(
                        activeTrip,
                        vehicle,
                        payload,
                        deviationMeter
                    );

                // -------------------------------------------------
                // Queue failed
                // Keep state so next packet can retry
                // -------------------------------------------------

                if (!queued) {

                    console.error(
                        `[OffRoute] ON ROUTE ALERT QUEUE FAILED | ` +
                        `VEHICLE: ${vehicleNo} | ` +
                        `TRIP: ${tripId}`
                    );

                    return;
                }

            }


            // -------------------------------------------------
            // Remove trip-wise OFF ROUTE state
            // -------------------------------------------------

            if (offRouteState) {

                await RedisRepository.deleteOffRouteState(
                    vehicleNo,
                    tripId
                );
            }

            return;
        }


        // =====================================================
        // FIRST OFF ROUTE STATE
        // =====================================================

        if (!offRouteState) {

            offRouteState = {

                status:
                    "OFF_ROUTE",

                alert_count:
                    0,

                deviation:
                    deviationMeter,

                gps_timestamp:
                    gpsTimestamp,

                start_timestamp:
                    gpsTimestamp,

                last_alert_distance:
                    0,

                last_alert_timestamp:
                    0
            };


            await RedisRepository.saveOffRouteState(
                vehicleNo,
                tripId,
                offRouteState
            );
        }


        // =====================================================
        // MAXIMUM 3 ALERTS
        // =====================================================

        const currentAlertCount =
            Number(
                offRouteState.alert_count || 0
            );

        if (
            currentAlertCount >= 3
        ) {

            offRouteState.deviation =
                deviationMeter;

            offRouteState.gps_timestamp =
                gpsTimestamp;


            await RedisRepository.saveOffRouteState(
                vehicleNo,
                tripId,
                offRouteState
            );

            return;
        }


        // =====================================================
        // NEXT ALERT THRESHOLD
        // =====================================================

        let nextThreshold = 0;

        switch (currentAlertCount) {

            // =================================================
            // FIRST ALERT
            // =================================================

            case 0:

                nextThreshold =
                    Number(
                        alertConfig.alert_time || 0
                    );

                break;


            // =================================================
            // SECOND ALERT
            // =================================================

            case 1:

                nextThreshold =
                    Number(
                        alertConfig.alert_reminder_time || 0
                    );

                break;


            // =================================================
            // THIRD ALERT
            // =================================================

            case 2:

                nextThreshold =
                    Number(
                        alertConfig.alert_interval || 0
                    );

                break;


            default:

                await RedisRepository.saveOffRouteState(
                    vehicleNo,
                    tripId,
                    offRouteState
                );

                return;
        }


        // =====================================================
        // THRESHOLD CHECK
        // =====================================================

        if (
            nextThreshold <= 0 ||
            deviationMeter < nextThreshold
        ) {

            offRouteState.deviation =
                deviationMeter;

            offRouteState.gps_timestamp =
                gpsTimestamp;


            await RedisRepository.saveOffRouteState(
                vehicleNo,
                tripId,
                offRouteState
            );

            return;
        }


        // =====================================================
        // PREVIOUS ALERT DISTANCE
        // =====================================================

        const previousDistance =
            Number(
                offRouteState.last_alert_distance || 0
            );


        // =====================================================
        // PREVIOUS ALERT TIMESTAMP
        // =====================================================

        const previousTimestamp =
            Number(
                offRouteState.last_alert_timestamp || 0
            );


        // =====================================================
        // VEHICLE MUST MOVE FURTHER
        // =====================================================

        if (
            previousDistance > 0 &&
            deviationMeter <= previousDistance
        ) {

            offRouteState.deviation =
                deviationMeter;

            offRouteState.gps_timestamp =
                gpsTimestamp;


            await RedisRepository.saveOffRouteState(
                vehicleNo,
                tripId,
                offRouteState
            );

            return;
        }


        // =====================================================
        // MOVED DISTANCE
        // =====================================================

        const movedDistance =
            previousDistance > 0
                ? Math.max(
                    0,
                    deviationMeter -
                    previousDistance
                )
                : 0;


        // =====================================================
        // TIME FROM LAST ALERT
        // =====================================================

        const timeFromLastAlert =
            previousTimestamp > 0
                ? Math.max(
                    0,
                    gpsTimestamp -
                    previousTimestamp
                )
                : 0;


        // =====================================================
        // TIME IN MINUTES
        // =====================================================

        const timeFromLastAlertMinutes =
            Math.floor(
                timeFromLastAlert /
                60000
            );


        // =====================================================
        // CURRENT ALERT NUMBER
        // =====================================================

        const currentAlertNo =
            currentAlertCount + 1;


        // =====================================================
        // SAVE OFF ROUTE ALERT
        // =====================================================

        const queued =
            await this.saveOffRouteAlert(
                activeTrip,
                vehicle,
                payload,
                deviationMeter,
                currentAlertNo,
                previousDistance,
                previousTimestamp,
                movedDistance,
                timeFromLastAlert,
                timeFromLastAlertMinutes
            );


        // =====================================================
        // QUEUE FAILED
        //
        // DO NOT UPDATE STATE
        // Next packet will retry
        // =====================================================

        if (!queued) {

            console.error(
                `[OffRoute] OFF ROUTE ALERT QUEUE FAILED | ` +
                `VEHICLE: ${vehicleNo} | ` +
                `TRIP: ${tripId} | ` +
                `ALERT: ${currentAlertNo}`
            );

            return;
        }


        // =====================================================
        // UPDATE REDIS STATE
        // =====================================================

        offRouteState.alert_count =
            currentAlertNo;

        offRouteState.deviation =
            deviationMeter;

        offRouteState.gps_timestamp =
            gpsTimestamp;

        offRouteState.last_alert_distance =
            deviationMeter;

        offRouteState.last_alert_timestamp =
            gpsTimestamp;


        await RedisRepository.saveOffRouteState(
            vehicleNo,
            tripId,
            offRouteState
        );

    }


    // =========================================================
    // SAVE OFF ROUTE ALERT
    //
    // COMMON REDIS QUEUE
    //
    // alert_id = 5
    // last_location = latitude,longitude
    // no alert_path
    // off_route_in_km = deviation in KM
    // =========================================================

    static async saveOffRouteAlert(
        activeTrip,
        vehicle,
        payload,
        deviationMeter,
        currentAlertNo,
        previousDistance,
        previousTimestamp,
        movedDistance,
        timeFromLastAlert,
        timeFromLastAlertMinutes
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
            // DEVIATION KM
            // =====================================================

            const deviationKm =
                Number(
                    (
                        deviationMeter /
                        1000
                    ).toFixed(2)
                );


            // =====================================================
            // SERVER TIME
            // =====================================================

            const detectedAt =
                await geoHelper.formatServerTime(
                    payload?.gps_timestamp
                );

            if (!detectedAt) {

                console.error(
                    `[OffRoute] INVALID SERVER TIME | ` +
                    `VEHICLE: ${vehicleNo}`
                );

                return false;
            }


            // =====================================================
            // MOVED DISTANCE
            // =====================================================

            let movedKmFromLastAlert =
                "N/A";

            if (
                previousDistance > 0
            ) {

                movedKmFromLastAlert =
                    (
                        Math.max(
                            0,
                            deviationMeter -
                            previousDistance
                        ) /
                        1000
                    ).toFixed(2);
            }


            // =====================================================
            // TIME GAP
            // =====================================================

            let timeGap =
                "First Alert";

            if (
                previousTimestamp > 0
            ) {

                const totalMinutes =
                    Math.max(
                        0,
                        Math.floor(
                            (
                                Number(
                                    payload?.gps_timestamp
                                ) -
                                previousTimestamp
                            ) /
                            60000
                        )
                    );


                const hours =
                    Math.floor(
                        totalMinutes / 60
                    );


                const minutes =
                    totalMinutes % 60;


                timeGap =
                    `${hours} Hr ${minutes} Min`;
            }


            // =====================================================
            // MESSAGE
            // =====================================================

            const message =
                `Off Route Alert\n\n` +
                `Vehicle Number : ${vehicleNo}\n` +
                `Trip No : ${
                    activeTrip?.trip_unique_id ||
                    activeTrip?.trip_id ||
                    "N/A"
                }\n` +
                `Expected Route : ${
                    activeTrip?.destination_store_name ||
                    "N/A"
                }\n` +
                `Alert Number : ${currentAlertNo}\n` +
                `Deviation Distance : ${deviationKm} KM\n` +
                `Moved From Last Alert : ${
                    movedKmFromLastAlert === "N/A"
                        ? "N/A"
                        : movedKmFromLastAlert + " KM"
                }\n` +
                `Time From Last Alert : ${timeGap}\n` +
                `Detected At : ${detectedAt}`;


            // =====================================================
            // COMMON VEHICLE ALERT QUEUE
            // =====================================================

            const queueResult =
                await RedisRepository.addVehicleAlert({

                    district_id:
                        activeTrip?.district_id ??
                        vehicle?.district_id ??
                        null,

                    vehicle_no:
                        vehicleNo,

                    trip_id:
                        activeTrip?.trip_id ??
                        null,

                    subtrip_id:
                        activeTrip?.sub_trip_id ??
                        activeTrip?.subtrip_id ??
                        null,

                    alert_id:
                        this.OFF_ROUTE_ALERT_ID,

                    message:
                        message,

                    last_location: alertLocation(payload),

                    last_date_time:
                        detectedAt,

                    long_halt_minutes:
                        null,

                    speed:
                        Number(
                            payload?.speed || 0
                        ),

                    // =================================================
                    // IMPORTANT
                    // OFF ROUTE DISTANCE IN KM
                    // =================================================
                    off_route_in_km:
                        deviationKm,

                    status:
                        1
                });


            if (!queueResult) {

                console.error(
                    `[OffRoute] COMMON QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TRIP: ${activeTrip?.trip_id}`
                );

                return false;
            }

            return true;

        } catch (error) {

            console.error(
                `[OffRoute] SAVE OFF ROUTE ALERT ERROR | ` +
                `VEHICLE: ${
                    vehicle?.vehicle_no ||
                    "UNKNOWN"
                }`,
                error
            );

            return false;
        }
    }


    // =========================================================
    // SAVE ON ROUTE ALERT
    //
    // COMMON REDIS QUEUE
    //
    // alert_id = 6
    // last_location = latitude,longitude
    // no alert_path
    // off_route_in_km = NULL
    // =========================================================

    static async saveOnRouteAlert(
        activeTrip,
        vehicle,
        payload,
        deviationMeter
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
            // SERVER TIME
            // =====================================================

            const detectedAt =
                await geoHelper.formatServerTime(
                    payload?.gps_timestamp
                );

            if (!detectedAt) {

                console.error(
                    `[OffRoute] ON ROUTE TIME INVALID | ` +
                    `VEHICLE: ${vehicleNo}`
                );

                return false;
            }


            // =====================================================
            // DEVIATION KM
            // =====================================================

            const deviationKm =
                (
                    Number(
                        deviationMeter || 0
                    ) /
                    1000
                ).toFixed(2);


            // =====================================================
            // MESSAGE
            // =====================================================

            const message =
                `On Route Alert\n\n` +
                `Vehicle No : ${vehicleNo}\n` +
                `Trip No : ${
                    activeTrip?.trip_unique_id ||
                    activeTrip?.trip_id ||
                    "N/A"
                }\n` +
                `Deviation Distance : ${deviationKm} KM\n` +
                `Detected At : ${detectedAt}\n\n` +
                `The vehicle is now back on the designated route.`;


            // =====================================================
            // COMMON VEHICLE ALERT QUEUE
            // =====================================================

            const queueResult =
                await RedisRepository.addVehicleAlert({

                    district_id:
                        activeTrip?.district_id ??
                        vehicle?.district_id ??
                        null,

                    vehicle_no:
                        vehicleNo,

                    trip_id:
                        activeTrip?.trip_id ??
                        null,

                    subtrip_id:
                        activeTrip?.sub_trip_id ??
                        activeTrip?.subtrip_id ??
                        null,

                    alert_id:
                        this.ON_ROUTE_ALERT_ID,

                    message:
                        message,

                    last_location: alertLocation(payload),

                    last_date_time:
                        detectedAt,

                    long_halt_minutes:
                        null,

                    speed:
                        Number(
                            payload?.speed || 0
                        ),

                    // ON ROUTE ke liye NULL
                    off_route_in_km:
                        null,

                    status:
                        1
                });


            if (!queueResult) {

                console.error(
                    `[OffRoute] ON ROUTE COMMON QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TRIP: ${activeTrip?.trip_id}`
                );

                return false;
            }

            return true;

        } catch (error) {

            console.error(
                `[OffRoute] SAVE ON ROUTE ALERT ERROR | ` +
                `VEHICLE: ${
                    vehicle?.vehicle_no ||
                    "UNKNOWN"
                }`,
                error
            );

            return false;
        }
    }
}


module.exports =
    OffRouteService;