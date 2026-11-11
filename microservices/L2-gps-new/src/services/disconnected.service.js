const alertLocation = require("../utils/alertLocation");
"use strict";

const moment = require("moment");

const TripRepository =
    require("../repositories/TripRepository");

const RedisRepository =
    require("../repositories/RedisRepository");

const geoHelper =
    require("../helpers/geoHelper");


class DisconnectedService {

    static ALERT_ID = 9;


    /*
    |--------------------------------------------------------------------------
    | Process Disconnected Alert
    |--------------------------------------------------------------------------
    */

    static async process(vehicle) {

        try {

            if (!vehicle || !vehicle.vehicle_no) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Active Trips
            |--------------------------------------------------------------------------
            */

            const activeTrips =
                await TripRepository.getActiveTrip(
                    vehicle.vehicle_no
                );

            if (
                !Array.isArray(activeTrips) ||
                activeTrips.length === 0
            ) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Running Trip
            |--------------------------------------------------------------------------
            */

            const activeTrip =
                activeTrips.find(
                    trip =>
                        Number(trip.trip_status) === 1
                );

            if (!activeTrip) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Alert Configuration
            |--------------------------------------------------------------------------
            */

            const disconnectedAlert =
                await TripRepository.getAlertConfig(
                    this.ALERT_ID
                );

            if (!disconnectedAlert) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Latest GPS Packet
            |--------------------------------------------------------------------------
            */

            const latestPacket =
                await RedisRepository.getLatestPacket(
                    vehicle.vehicle_no
                );

            if (!latestPacket) {
                return null;
            }


            if (!latestPacket.gps_timestamp) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Last Packet Receive Time
            |--------------------------------------------------------------------------
            */

            const formattedPacketTime =
                await geoHelper.formatServerTime(
                    latestPacket.gps_timestamp
                );

            const packetReceiveTime =
                moment(
                    formattedPacketTime,
                    "YYYY-MM-DD HH:mm:ss"
                );


            if (!packetReceiveTime.isValid()) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Current Server Time
            |--------------------------------------------------------------------------
            */

            const currentTime =
                moment();


            /*
            |--------------------------------------------------------------------------
            | Disconnected Duration
            |--------------------------------------------------------------------------
            */

            const diffMinutes =
                currentTime.diff(
                    packetReceiveTime,
                    "minutes"
                );


            const alertTime =
                Number(
                    disconnectedAlert.alert_time
                );


            if (
                !Number.isFinite(alertTime) ||
                alertTime <= 0
            ) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Redis State
            |--------------------------------------------------------------------------
            */

            const redisKey =
                `${vehicle.vehicle_no}:${activeTrip.id}`;


            let state =
                await RedisRepository.getDisconnectedState(
                    redisKey
                );


            if (!state) {

                state = {
                    alertSent: false
                };

            }


            /*
            |--------------------------------------------------------------------------
            | Vehicle Connected Again
            |--------------------------------------------------------------------------
            */

            if (diffMinutes < alertTime) {

                if (state.alertSent) {

                    state.alertSent = false;

                    await RedisRepository
                        .saveDisconnectedState(
                            redisKey,
                            state
                        );
                }

                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Duplicate Alert
            |--------------------------------------------------------------------------
            */

            if (state.alertSent) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Queue Alert
            |--------------------------------------------------------------------------
            */

            const queued =
                await this.saveDisconnectedAlert(
                    activeTrip,
                    vehicle,
                    latestPacket,
                    disconnectedAlert,
                    diffMinutes,
                    packetReceiveTime.format(
                        "YYYY-MM-DD HH:mm:ss"
                    )
                );


            /*
            |--------------------------------------------------------------------------
            | Update Redis Only After Queue Success
            |--------------------------------------------------------------------------
            */

            if (!queued) {
                return null;
            }


            state.alertSent = true;

            await RedisRepository
                .saveDisconnectedState(
                    redisKey,
                    state
                );


            return true;

        } catch (error) {

            console.error(
                "[Disconnected] ERROR:",
                error
            );

            return null;
        }
    }


    /*
    |--------------------------------------------------------------------------
    | Queue Disconnected Alert
    |--------------------------------------------------------------------------
    */

    static async saveDisconnectedAlert(
        activeTrip,
        vehicle,
        payload,
        disconnectedAlert,
        disconnectedMinutes,
        packetReceiveTime
    ) {

        try {

            if (!activeTrip) {
                return false;
            }


            /*
            |--------------------------------------------------------------------------
            | Alert Message
            |--------------------------------------------------------------------------
            */

            const message =
                `${disconnectedAlert.alert_name}\n` +
                `Vehicle : ${vehicle.vehicle_no}\n` +
                `Trip ID : ${activeTrip.trip_id}\n` +
                `Last Packet Time : ${packetReceiveTime}\n` +
                `Disconnected Duration : ${disconnectedMinutes} Minutes\n` +
                `Vehicle is disconnected from the tracking server.`;


            

            const queued =
                await RedisRepository.addVehicleAlert({

                    trip_id:
                        activeTrip.id ??
                        activeTrip.trip_id ??
                        null,

                    subtrip_id:
                        activeTrip.sub_trip_id ??
                        activeTrip.subtrip_id ??
                        activeTrip.subtripId ??
                        null,

                    vehicle_no:
                        vehicle.vehicle_no,

                    alert_id:
                        this.ALERT_ID,

                    message,

                    
                    last_location: alertLocation(payload),

                    last_date_time:
                        packetReceiveTime,

                    /*
                    | Existing DB structure is using this
                    | field for disconnected duration.
                    */
                    long_halt_minutes:
                        disconnectedMinutes,

                    speed:
                        null,

                    off_route_in_km:
                        null,

                    district_id:
                        vehicle.district_id ??
                        null,

                    status:
                        1,

                    in_out_status:
                        null,

                    geofence:
                        null
                });


            if (queued) {

                // console.log(
                //     `[Disconnected] ALERT QUEUED | ` +
                //     `Vehicle: ${vehicle.vehicle_no} | ` +
                //     `Trip: ${activeTrip.id} | ` +
                //     `Duration: ${disconnectedMinutes} min`
                // );
            }


            return queued;

        } catch (error) {

            console.error(
                "[Disconnected] QUEUE ERROR:",
                error
            );

            return false;
        }
    }
}


module.exports = DisconnectedService;