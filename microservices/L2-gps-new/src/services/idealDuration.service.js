const alertLocation = require("../utils/alertLocation");
'use strict';

const TripRepository =
    require("../repositories/TripRepository");

const AlertEventRepository =
    require("../repositories/AlertEventRepository");

const IdealDurationRepository =
    require("../repositories/idealDuration.repository");

const RedisRepository =
    require("../repositories/RedisRepository");


class IdealDurationService {

    /*
    |--------------------------------------------------------------------------
    | CONFIG
    |--------------------------------------------------------------------------
    */

    static ALERT_ID = 12;

    /*
    | Speed >= 1 means vehicle is moving.
    | Speed < 1 means vehicle is idle.
    */
    static SPEED_LIMIT = 1;


    /*
    |--------------------------------------------------------------------------
    | PROCESS
    |--------------------------------------------------------------------------
    */

    static async process(
        vehicle,
        payload
    ) {

        try {

            /*
            |--------------------------------------------------------------------------
            | VEHICLE VALIDATION
            |--------------------------------------------------------------------------
            */

            const vehicleNo =
                String(
                    vehicle?.vehicle_no || ""
                ).trim();


            if (!vehicleNo) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ACTIVE TRIPS
            |--------------------------------------------------------------------------
            |
            | getActiveTrip() returns ARRAY.
            |
            */

            const activeTrips =
                await TripRepository.getActiveTrip(
                    vehicleNo
                );


            if (
                !Array.isArray(activeTrips) ||
                activeTrips.length === 0
            ) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | FIND RUNNING TRIP
            |--------------------------------------------------------------------------
            */

            const activeTrip =
                activeTrips.find(
                    trip =>
                        Number(trip?.trip_status) === 1
                );


            /*
            |--------------------------------------------------------------------------
            | NO RUNNING TRIP
            |--------------------------------------------------------------------------
            */

            if (!activeTrip) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ALERT CONFIG
            |--------------------------------------------------------------------------
            */

            const idealAlert =
                await TripRepository.getAlertConfig(
                    this.ALERT_ID
                );


            if (!idealAlert) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ALERT TIME
            |--------------------------------------------------------------------------
            */

            const alertTime =
                Number(
                    idealAlert.alert_time || 0
                );


            if (
                !Number.isFinite(alertTime) ||
                alertTime <= 0
            ) {

                console.warn(
                    `[IdealDuration] INVALID ALERT TIME | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `VALUE: ${idealAlert.alert_time}`
                );

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | SPEED
            |--------------------------------------------------------------------------
            */

            const speed =
                Number(
                    payload?.speed || 0
                );


            /*
            |--------------------------------------------------------------------------
            | CURRENT IST TIME
            |--------------------------------------------------------------------------
            */

            const currentIST =
                await IdealDurationRepository
                    .getCurrentISTDateTime();


            if (!currentIST) {
                return;
            }


            const currentDate =
                this.parseDateTime(
                    currentIST
                );


            if (!currentDate) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | VEHICLE MOVING
            |--------------------------------------------------------------------------
            |
            | Speed >= 1
            |
            | Close existing Idle event and reset Idle state.
            |
            */

            if (
                speed >= this.SPEED_LIMIT
            ) {

                await this.closeActiveEvent(
                    activeTrip,
                    vehicleNo,
                    payload,
                    currentDate
                );


                /*
                |--------------------------------------------------------------------------
                | RESET IDLE START
                |--------------------------------------------------------------------------
                */

                await RedisRepository
                    .deleteIdealDurationIgnition(
                        vehicleNo
                    );


                return;
            }


            /*
            |--------------------------------------------------------------------------
            | VEHICLE IDLE
            |--------------------------------------------------------------------------
            */

            let stoppedStart =
                await RedisRepository
                    .getIdealDurationIgnition(
                        vehicleNo
                    );


            /*
            |--------------------------------------------------------------------------
            | FIRST IDLE PACKET
            |--------------------------------------------------------------------------
            */

            if (!stoppedStart) {

                stoppedStart =
                    currentDate.toISOString();


                await RedisRepository
                    .saveIdealDurationIgnition(
                        vehicleNo,
                        stoppedStart
                    );


                return;
            }


            /*
            |--------------------------------------------------------------------------
            | IDLE START DATE
            |--------------------------------------------------------------------------
            */

            const stoppedStartDate =
                this.parseDateTime(
                    stoppedStart
                );


            if (!stoppedStartDate) {

                await RedisRepository
                    .saveIdealDurationIgnition(
                        vehicleNo,
                        currentDate.toISOString()
                    );

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | IDLE DURATION
            |--------------------------------------------------------------------------
            */

            const stoppedMinutes =
                Math.max(
                    0,
                    Math.floor(
                        (
                            currentDate.getTime() -
                            stoppedStartDate.getTime()
                        ) / 60000
                    )
                );


            

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
            | CHECK ACTIVE IDLE EVENT
            |--------------------------------------------------------------------------
            */

            const activeEvent =
                await AlertEventRepository
                    .getActiveAlertEvent(
                        vehicleNo,
                        activeTrip.trip_id,
                        this.ALERT_ID
                    );


            /*
            |--------------------------------------------------------------------------
            | EXISTING IDLE EVENT
            |--------------------------------------------------------------------------
            |
            | Same idle session:
            | only update duration/location.
            |
            */

            if (activeEvent) {

                await this.updateIdleEvent(
                    activeEvent.id,
                    stoppedMinutes,
                    validLocation,
                    lat,
                    lng
                );


                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ALERT THRESHOLD
            |--------------------------------------------------------------------------
            */

            if (
                stoppedMinutes <
                alertTime
            ) {

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | TRIP DELAY
            |--------------------------------------------------------------------------
            */

            const tripDate =
                this.parseDateTime(
                    activeTrip.trip_date
                );


            const expectedArrival =
                this.parseDateTime(
                    activeTrip.expected_arrival_time
                );


            let lateMinutes = 0;


            if (
                tripDate &&
                expectedArrival
            ) {

                const idealDurationMinutes =
                    Math.max(
                        0,
                        Math.round(
                            (
                                expectedArrival.getTime() -
                                tripDate.getTime()
                            ) / 60000
                        )
                    );


                const actualDurationMinutes =
                    Math.max(
                        0,
                        Math.round(
                            (
                                currentDate.getTime() -
                                tripDate.getTime()
                            ) / 60000
                        )
                    );


                lateMinutes =
                    Math.max(
                        0,
                        actualDurationMinutes -
                        idealDurationMinutes
                    );
            }


            /*
            |--------------------------------------------------------------------------
            | DELAY TEXT
            |--------------------------------------------------------------------------
            */

            let delayText =
                "Trip is on schedule.";


            if (
                lateMinutes > 0
            ) {

                delayText =
                    `Trip is delayed by ` +
                    `${this.formatDuration(
                        lateMinutes
                    )}.`;
            }


            /*
            |--------------------------------------------------------------------------
            | IDLE DURATION TEXT
            |--------------------------------------------------------------------------
            */

            const idleDuration =
                this.formatDuration(
                    stoppedMinutes
                );


            /*
            |--------------------------------------------------------------------------
            | MESSAGE
            |--------------------------------------------------------------------------
            */

            const message =
                `Vehicle ${vehicleNo} is IDLE for ` +
                `${idleDuration} during Trip ` +
                `${activeTrip.trip_unique_id || activeTrip.trip_id || activeTrip.id}. ` +
                `${delayText}`;


            /*
            |--------------------------------------------------------------------------
            | CREATE ALERT EVENT
            |--------------------------------------------------------------------------
            |
            | IMPORTANT:
            |
            | alert_path intentionally NOT sent.
            |
            | Requirement:
            | tbl_vehicle_alert_event.alert_path
            | should not be inserted/updated.
            |
            */

            const eventResult =
                await AlertEventRepository
                    .savealertsEventLog({

                        trip_id:
                            activeTrip.trip_id,

                        vehicle_no:
                            vehicleNo,

                        alert_id:
                            this.ALERT_ID,

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
                            stoppedMinutes

                    });


            const eventId =
                eventResult?.insertId;


            /*
            |--------------------------------------------------------------------------
            | COMMON VEHICLE ALERT QUEUE
            |--------------------------------------------------------------------------
            |
            | No separate Idle queue.
            |
            | vehicle_alert_queue
            |        ↓
            | AlertBatchService
            |        ↓
            | tbl_vehicle_alert_details
            |
            */

            const lastDateTime =
                await this.getPacketDateTime(
                    payload
                );


            const queued =
                await RedisRepository
                    .addVehicleAlert({

                        trip_id:
                            activeTrip.trip_id,

                        subtrip_id:
                            activeTrip.sub_trip_id ??
                            activeTrip.subtrip_id ??
                            activeTrip.subtripId ??
                            null,

                        vehicle_no:
                            vehicleNo,

                        alert_id:
                            this.ALERT_ID,

                        message:
                            message,

                        

                        last_location: alertLocation(payload),

                        last_date_time:
                            lastDateTime,

                        long_halt_minutes:
                            stoppedMinutes,

                        speed:
                            Math.round(speed),

                        off_route_in_km:
                            null,

                        district_id:
                            vehicle?.district_id ??
                            activeTrip?.district_id ??
                            activeTrip?.district ??
                            null,

                        status:
                            1,

                        in_out_status:
                            null,

                        geofence:
                            null

                    });


            /*
            |--------------------------------------------------------------------------
            | QUEUE FAILED
            |--------------------------------------------------------------------------
            */

            if (!queued) {

                console.error(
                    `[IdealDuration] COMMON ALERT QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TRIP: ${activeTrip.trip_id}`
                );

                /*
                |--------------------------------------------------------------------------
                | IMPORTANT
                |--------------------------------------------------------------------------
                | Event already created.
                |
                | If queue fails, do not create another event
                | in the next packet.
                |
                | Existing event will be picked by
                | getActiveAlertEvent().
                |--------------------------------------------------------------------------
                */

                return;
            }


        } catch (error) {

            console.error(
                `${vehicle?.vehicle_no || "UNKNOWN"} | ` +
                `IDEAL DURATION SERVICE ERROR:`,
                error
            );
        }

    }


    /*
    |--------------------------------------------------------------------------
    | GET PACKET DATE TIME
    |--------------------------------------------------------------------------
    */

    static async getPacketDateTime(
        payload
    ) {

        try {

            if (
                payload?.gps_timestamp !== undefined &&
                payload?.gps_timestamp !== null
            ) {

                const formatted =
                    await require("../helpers/geoHelper")
                        .formatServerTime(
                            payload.gps_timestamp
                        );


                if (formatted) {
                    return formatted;
                }
            }


            return null;


        } catch (error) {

            console.error(
                "[IdealDuration] PACKET TIME ERROR:",
                error
            );

            return null;
        }

    }


    /*
    |--------------------------------------------------------------------------
    | UPDATE ACTIVE IDLE EVENT
    |--------------------------------------------------------------------------
    */

    static async updateIdleEvent(
        eventId,
        durationMinutes,
        validLocation,
        lat,
        lng
    ) {

        try {

            await AlertEventRepository
                .updateIdleEvent(
                    eventId,
                    {
                        duration_minutes:
                            durationMinutes,

                        lat:
                            validLocation
                                ? lat
                                : null,

                        lng:
                            validLocation
                                ? lng
                                : null
                    }
                );


        } catch (error) {

            console.error(
                "[IdealDuration] UPDATE IDLE EVENT ERROR:",
                error
            );
        }

    }


    /*
    |--------------------------------------------------------------------------
    | CLOSE EVENT WHEN VEHICLE STARTS MOVING
    |--------------------------------------------------------------------------
    */

    static async closeActiveEvent(
        activeTrip,
        vehicleNo,
        payload,
        currentDate
    ) {

        try {

            const activeEvent =
                await AlertEventRepository
                    .getActiveAlertEvent(
                        vehicleNo,
                        activeTrip.trip_id,
                        this.ALERT_ID
                    );


            if (!activeEvent) {
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
            | CLOSE EVENT
            |--------------------------------------------------------------------------
            */

            if (
                typeof AlertEventRepository
                    .updateAlertEventDuration ===
                "function"
            ) {

                await AlertEventRepository
                    .updateAlertEventDuration(
                        activeEvent.id,
                        {
                            duration_minutes:
                                null,

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
            else if (
                typeof AlertEventRepository
                    .updateAlertEvent ===
                "function"
            ) {

                await AlertEventRepository
                    .updateAlertEvent(
                        activeEvent.id,
                        {
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


        } catch (error) {

            console.error(
                `${vehicleNo} | ` +
                `CLOSE IDLE EVENT ERROR:`,
                error
            );
        }

    }


    /*
    |--------------------------------------------------------------------------
    | PARSE DATE
    |--------------------------------------------------------------------------
    */

    static parseDateTime(
        value
    ) {

        if (!value) {
            return null;
        }


        /*
        |--------------------------------------------------------------------------
        | DATE OBJECT
        |--------------------------------------------------------------------------
        */

        if (
            value instanceof Date
        ) {

            return Number.isNaN(
                value.getTime()
            )
                ? null
                : value;
        }


        /*
        |--------------------------------------------------------------------------
        | STRING
        |--------------------------------------------------------------------------
        */

        const valueString =
            String(
                value
            ).trim();


        if (!valueString) {
            return null;
        }


        /*
        |--------------------------------------------------------------------------
        | ISO DATE
        |--------------------------------------------------------------------------
        */

        if (
            valueString.includes("T")
        ) {

            const date =
                new Date(
                    valueString
                );


            return Number.isNaN(
                date.getTime()
            )
                ? null
                : date;
        }


        /*
        |--------------------------------------------------------------------------
        | MYSQL DATETIME
        |--------------------------------------------------------------------------
        |
        | Server/application timezone = IST
        |
        |--------------------------------------------------------------------------
        */

        const date =
            new Date(
                valueString.replace(
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
    IdealDurationService;