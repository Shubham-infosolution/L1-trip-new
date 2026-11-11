const alertLocation = require("../utils/alertLocation");
"use strict";

const TripRepository =
    require("../repositories/TripRepository");

const RedisRepository =
    require("../repositories/RedisRepository");

const AlertEventRepository =
    require("../repositories/AlertEventRepository");

const geoHelper =
    require("../helpers/geoHelper");


class IgnitionOffService {

    /*
    |--------------------------------------------------------------------------
    | ALERT IDS
    |--------------------------------------------------------------------------
    */

    static OFF_ALERT_ID = 13;

    static ON_ALERT_ID = 14;


    /*
    |--------------------------------------------------------------------------
    | MAIN PROCESS
    |--------------------------------------------------------------------------
    */

    static async process(
        vehicle,
        payload
    ) {

        try {

            const vehicleNo =
                String(
                    vehicle?.vehicle_no || ""
                ).trim();

            if (!vehicleNo) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ACTIVE TRIP
            |--------------------------------------------------------------------------
            */

            const activeTrips =
                await TripRepository.getActiveTrip(
                    vehicleNo
                );

            const activeTrip =
                Array.isArray(activeTrips)
                    ? activeTrips.find(
                        trip =>
                            Number(
                                trip?.trip_status
                            ) === 1
                    )
                    : activeTrips;

            if (
                !activeTrip ||
                Number(activeTrip.trip_status) !== 1
            ) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | IGNITION STATUS
            |--------------------------------------------------------------------------
            */

            const ignition =
                Number(
                    payload?.ignition_status
                );


            /*
            |--------------------------------------------------------------------------
            | GPS TIME
            |--------------------------------------------------------------------------
            | packetTime is the source of truth.
            | Do NOT convert it to ISO for currentDate.
            |--------------------------------------------------------------------------
            */

            const packetTime =
                await geoHelper.formatServerTime(
                    payload?.gps_timestamp
                );

            if (!packetTime) {
                return;
            }

            /*
             * Current time remains exactly:
             *
             * 2026-09-21 19:23:29
             */
            const currentDate = packetTime;

            if (!currentDate) {
                return;
            }
            /*
            |--------------------------------------------------------------------------
            | LOCATION
            |--------------------------------------------------------------------------
            */

            const lat =
                Number(
                    payload?.latitude ??
                    payload?.lat
                );

            const lng =
                Number(
                    payload?.longitude ??
                    payload?.lng
                );

            const validLocation =
                Number.isFinite(lat) &&
                Number.isFinite(lng);


            /*
            |--------------------------------------------------------------------------
            | IGNITION ON
            |--------------------------------------------------------------------------
            */

            if (ignition === 1) {

                await this.handleIgnitionOn(
                    vehicle,
                    payload,
                    activeTrip,
                    vehicleNo,
                    currentDate,
                    validLocation,
                    lat,
                    lng
                );

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | IGNITION OFF
            |--------------------------------------------------------------------------
            */

            await this.handleIgnitionOff(
                vehicle,
                payload,
                activeTrip,
                vehicleNo,
                currentDate,
                validLocation,
                lat,
                lng
            );

        }
        catch (error) {

            console.error(
                `${vehicle?.vehicle_no || "UNKNOWN"} | ` +
                `IGNITION OFF SERVICE ERROR:`,
                error
            );

        }

    }


    /*
    |--------------------------------------------------------------------------
    | IGNITION OFF
    |--------------------------------------------------------------------------
    */

    static async handleIgnitionOff(
        vehicle,
        payload,
        activeTrip,
        vehicleNo,
        currentDate,
        validLocation,
        lat,
        lng
    ) {

        /*
        |--------------------------------------------------------------------------
        | GET EXISTING OFF START
        |--------------------------------------------------------------------------
        */

        let offStart =
            await RedisRepository
                .getIgnitionOffStart(
                    vehicleNo
                );


        /*
        |--------------------------------------------------------------------------
        | FIRST OFF PACKET
        |--------------------------------------------------------------------------
        */

        if (!offStart) {

            /*
             * Store packetTime directly.
             *
             * Example:
             * 2026-09-21 19:23:29
             */
            offStart =
                currentDate;

            await RedisRepository
                .saveIgnitionOffStart(
                    vehicleNo,
                    offStart
                );

            return;
        }


        /*
        |--------------------------------------------------------------------------
        | OFF START TIME
        |--------------------------------------------------------------------------
        */

        const offStartDate =
            this.parseDateTime(
                offStart
            );

        if (!offStartDate) {

            await RedisRepository
                .saveIgnitionOffStart(
                    vehicleNo,
                    currentDate
                );

            return;
        }


        /*
        |--------------------------------------------------------------------------
        | OFF DURATION
        |--------------------------------------------------------------------------
        */

        /*
         * currentDate is a string.
         *
         * Convert ONLY for duration calculation.
         * Do not replace currentDate itself.
         */
        const currentTimestamp =
            this.parseDateTime(
                currentDate
            )?.getTime();

        if (!Number.isFinite(currentTimestamp)) {
            return;
        }

        const offMinutes =
            Math.max(
                0,
                Math.floor(
                    (
                        currentTimestamp -
                        offStartDate.getTime()
                    ) / 60000
                )
            );


        /*
        |--------------------------------------------------------------------------
        | GET ALERT CONFIG
        |--------------------------------------------------------------------------
        */

        const alertConfig =
            await TripRepository.getAlertConfig(
                this.OFF_ALERT_ID
            );

        if (!alertConfig) {
            return;
        }

        const alertTime =
            Number(
                alertConfig.alert_time || 0
            );


        /*
        |--------------------------------------------------------------------------
        | CHECK EXISTING EVENT
        |--------------------------------------------------------------------------
        */

        const activeEvent =
            await AlertEventRepository
                .getActiveAlertEvent(
                    vehicleNo,
                    activeTrip.trip_id,
                    this.OFF_ALERT_ID
                );


        /*
        |--------------------------------------------------------------------------
        | EVENT ALREADY EXISTS
        |--------------------------------------------------------------------------
        */

        if (activeEvent) {

            await this.updateOffEvent(
                activeEvent.id,
                offMinutes,
                validLocation,
                lat,
                lng
            );

            return;
        }


        /*
        |--------------------------------------------------------------------------
        | ALERT TIME NOT REACHED
        |--------------------------------------------------------------------------
        */

        if (
            offMinutes <
            alertTime
        ) {
            return;
        }


        /*
        |--------------------------------------------------------------------------
        | MESSAGE
        |--------------------------------------------------------------------------
        */

        let durationText = "";

        if (offMinutes > 0) {

            durationText =
                `Ignition OFF Duration : ` +
                `${this.formatDuration(
                    offMinutes
                )}\n`;
        }

        const message =
            `Ignition OFF\n` +
            `Vehicle : ${vehicleNo}\n` +
            `Trip ID : ${activeTrip.trip_unique_id}\n` +
            durationText +
            `Vehicle ignition is currently OFF.`;


        /*
        |--------------------------------------------------------------------------
        | SAVE ALERT EVENT
        |--------------------------------------------------------------------------
        */

        await AlertEventRepository
            .savealertsEventLog({

                trip_id:
                    activeTrip.trip_id,

                vehicle_no:
                    vehicleNo,

                alert_id:
                    this.OFF_ALERT_ID,

                start_lat:
                    validLocation
                        ? lat
                        : null,

                start_lng:
                    validLocation
                        ? lng
                        : null,

                off_route_in_km:
                    0,

                duration_minutes:
                    offMinutes
            });


        /*
        |--------------------------------------------------------------------------
        | COMMON VEHICLE ALERT QUEUE
        |--------------------------------------------------------------------------
        */

        if (
            typeof RedisRepository.addVehicleAlert !==
            "function"
        ) {

            throw new Error(
                "RedisRepository.addVehicleAlert is not available"
            );
        }


        const queued =
            await RedisRepository
                .addVehicleAlert({

                    trip_id:
                        activeTrip.trip_id,

                    subtrip_id:
                        activeTrip.sub_trip_id,

                    vehicle_no:
                        vehicleNo,

                    alert_id:
                        this.OFF_ALERT_ID,

                    district_id:
                        vehicle.district_id,

                    message,

                    
                    last_location: alertLocation(payload),

                    /*
                     * Use packetTime directly.
                     */
                    last_date_time:
                        currentDate,

                    long_halt_minutes:
                        Number(
                            offMinutes
                        ),

                    off_route_in_km:
                        null,

                    speed:
                        Number(
                            payload?.speed || 0
                        ),

                    status:
                        1,

                    in_out_status:
                        null,

                    geofence:
                        null
                });


        if (!queued) {

            console.error(
                `[Ignition] COMMON ALERT QUEUE FAILED | ` +
                `Vehicle: ${vehicleNo} | ` +
                `Trip: ${activeTrip.trip_id}`
            );

            return false;
        }

        return true;
    }


    /*
    |--------------------------------------------------------------------------
    | IGNITION ON
    |--------------------------------------------------------------------------
    */

    static async handleIgnitionOn(
        vehicle,
        payload,
        activeTrip,
        vehicleNo,
        currentDate,
        validLocation,
        lat,
        lng
    ) {

        /*
        |--------------------------------------------------------------------------
        | GET OFF START
        |--------------------------------------------------------------------------
        */

        const offStart =
            await RedisRepository
                .getIgnitionOffStart(
                    vehicleNo
                );


        /*
        |--------------------------------------------------------------------------
        | VEHICLE WAS NOT PREVIOUSLY OFF
        |--------------------------------------------------------------------------
        */

        if (!offStart) {
            return;
        }


        /*
        |--------------------------------------------------------------------------
        | OFF START TIME
        |--------------------------------------------------------------------------
        */

        const offStartDate =
            this.parseDateTime(
                offStart
            );

        if (!offStartDate) {

            await RedisRepository
                .deleteIgnitionOffStart(
                    vehicleNo
                );

            return;
        }


        /*
        |--------------------------------------------------------------------------
        | FINAL OFF DURATION
        |--------------------------------------------------------------------------
        */

        const currentTimestamp =
            this.parseDateTime(
                currentDate
            )?.getTime();

        if (!Number.isFinite(currentTimestamp)) {
            return;
        }

        const offMinutes =
            Math.max(
                0,
                Math.floor(
                    (
                        currentTimestamp -
                        offStartDate.getTime()
                    ) / 60000
                )
            );


        /*
        |--------------------------------------------------------------------------
        | FIND ACTIVE OFF EVENT
        |--------------------------------------------------------------------------
        */

        const offEvent =
            await AlertEventRepository
                .getActiveAlertEvent(
                    vehicleNo,
                    activeTrip.trip_id,
                    this.OFF_ALERT_ID
                );


        /*
        |--------------------------------------------------------------------------
        | CLOSE OFF EVENT
        |--------------------------------------------------------------------------
        */

        if (offEvent) {

            await this.closeOffEvent(
                offEvent.id,
                offMinutes,
                validLocation,
                lat,
                lng
            );

        }


        /*
        |--------------------------------------------------------------------------
        | DELETE REDIS OFF START
        |--------------------------------------------------------------------------
        */

        await RedisRepository
            .deleteIgnitionOffStart(
                vehicleNo
            );


        /*
        |--------------------------------------------------------------------------
        | SAVE IGNITION ON ALERT
        |--------------------------------------------------------------------------
        */

        await this.saveIgnitionOnAlert(
            vehicle,
            payload,
            activeTrip,
            vehicleNo,
            currentDate,
            validLocation,
            lat,
            lng,
            offMinutes
        );

    }


    /*
    |--------------------------------------------------------------------------
    | UPDATE OFF EVENT WHILE STILL OFF
    |--------------------------------------------------------------------------
    */

    static async updateOffEvent(eventId, offMinutes, validLocation, lat, lng) {
        // Progress must leave end_date_time NULL until ignition turns ON.
        await AlertEventRepository.updateIdleEvent(eventId, {
            duration_minutes: offMinutes,
            lat: validLocation ? lat : null,
            lng: validLocation ? lng : null
        });
    }

    static async closeOffEvent(
        eventId,
        offMinutes,
        validLocation,
        lat,
        lng
    ) {

        if (
            typeof AlertEventRepository
                .updateAlertEventDuration ===
            "function"
        ) {

            await AlertEventRepository
                .updateAlertEventDuration(
                    eventId,
                    {
                        duration_minutes:
                            offMinutes,

                        end_lat:
                            validLocation
                                ? lat
                                : null,

                        end_lng:
                            validLocation
                                ? lng
                                : null
                    }
                );

            return;
        }


        if (
            typeof AlertEventRepository
                .updateAlertEvent ===
            "function"
        ) {

            await AlertEventRepository
                .updateAlertEvent(
                    eventId,
                    {
                        duration_minutes:
                            offMinutes,

                        end_lat:
                            validLocation
                                ? lat
                                : null,

                        end_lng:
                            validLocation
                                ? lng
                                : null
                    }
                );

        }

    }


    /*
    |--------------------------------------------------------------------------
    | IGNITION ON ALERT
    |--------------------------------------------------------------------------
    */

    static async saveIgnitionOnAlert(
        vehicle,
        payload,
        activeTrip,
        vehicleNo,
        currentDate,
        validLocation,
        lat,
        lng,
        offMinutes
    ) {

        const message =
            `Ignition ON\n` +
            `Vehicle : ${vehicleNo}\n` +
            `Trip ID : ${activeTrip.trip_unique_id}\n` +
            `Ignition OFF Duration : ` +
            `${this.formatDuration(
                offMinutes
            )}\n` +
            `Vehicle ignition is now ON.`;


        /*
        |--------------------------------------------------------------------------
        | COMMON VEHICLE ALERT QUEUE
        |--------------------------------------------------------------------------
        */

        if (
            typeof RedisRepository.addVehicleAlert !==
            "function"
        ) {

            throw new Error(
                "RedisRepository.addVehicleAlert is not available"
            );
        }


        const queued =
            await RedisRepository
                .addVehicleAlert({

                    trip_id:
                        activeTrip.trip_id,

                    subtrip_id:
                        activeTrip.sub_trip_id,

                    vehicle_no:
                        vehicleNo,

                    alert_id:
                        this.ON_ALERT_ID,

                    district_id:
                        vehicle.district_id,

                    message,

                    
                    last_location: alertLocation(payload),

                    /*
                     * Exact GPS packet time.
                     */
                    last_date_time:
                        currentDate,

                    long_halt_minutes:
                        Number(
                            offMinutes
                        ),

                    off_route_in_km:
                        null,

                    speed:
                        Number(
                            payload?.speed || 0
                        ),

                    status:
                        1,

                    in_out_status:
                        null,

                    geofence:
                        null
                });


        if (!queued) {

            console.error(
                `[Ignition] ON ALERT QUEUE FAILED | ` +
                `Vehicle: ${vehicleNo} | ` +
                `Trip: ${activeTrip.trip_id}`
            );

            return false;
        }

        return true;
    }


    /*
    |--------------------------------------------------------------------------
    | DATE PARSER
    |--------------------------------------------------------------------------
    */

    static parseDateTime(
        value
    ) {

        if (!value) {
            return null;
        }


        if (
            value instanceof Date
        ) {

            return Number.isNaN(
                value.getTime()
            )
                ? null
                : value;
        }


        const str =
            String(
                value
            ).trim();


        /*
        |--------------------------------------------------------------------------
        | ISO DATE
        |--------------------------------------------------------------------------
        */

        if (
            str.includes("T")
        ) {

            const date =
                new Date(str);

            return Number.isNaN(
                date.getTime()
            )
                ? null
                : date;
        }


        /*
        |--------------------------------------------------------------------------
        | MYSQL DATE
        |--------------------------------------------------------------------------
        */

        const date =
            new Date(
                str.replace(
                    " ",
                    "T"
                ) +
                "+05:30"
            );

        return Number.isNaN(
            date.getTime()
        )
            ? null
            : date;

    }


    /*
    |--------------------------------------------------------------------------
    | FORMAT DURATION
    |--------------------------------------------------------------------------
    */

    static formatDuration(
        minutes
    ) {

        const totalMinutes =
            Math.max(
                0,
                Math.round(
                    Number(
                        minutes || 0
                    )
                )
            );


        const hours =
            Math.floor(
                totalMinutes / 60
            );


        const mins =
            totalMinutes % 60;


        if (
            hours > 0 &&
            mins > 0
        ) {

            return (
                `${hours} Hr ${mins} Min`
            );

        }


        if (
            hours > 0
        ) {

            return (
                `${hours} Hr`
            );

        }


        return (
            `${mins} Min`
        );

    }

}


module.exports =
    IgnitionOffService;