const alertLocation = require("../utils/alertLocation");
'use strict';

const moment = require("moment");

const TripRepository =
    require("../repositories/TripRepository");

const RedisRepository =
    require("../repositories/RedisRepository");

const geoHelper =
    require("../helpers/geoHelper");


class SearchingNetworkService {

    static ALERT_ID = 8;


    static async process(vehicle, payload) {

        try {

            if (
                !vehicle ||
                !vehicle.vehicle_no
            ) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | GET ACTIVE TRIPS
            |--------------------------------------------------------------------------
            */

            const activeTrips =
                await TripRepository.getActiveTrip(
                    vehicle.vehicle_no
                );

            /*
            |--------------------------------------------------------------------------
            | NO ACTIVE TRIP
            |--------------------------------------------------------------------------
            */

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

            const searchingNetworkAlert =
                await TripRepository.getAlertConfig(
                    this.ALERT_ID
                );


            if (!searchingNetworkAlert) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ALERT TIME
            |--------------------------------------------------------------------------
            */

            const alertTime =
                Number(
                    searchingNetworkAlert.alert_time
                );


            if (
                !Number.isFinite(alertTime) ||
                alertTime <= 0
            ) {

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | GET LATEST PACKET
            |--------------------------------------------------------------------------
            */

            const latestPacket =
                await RedisRepository.getLatestPacket(
                    vehicle.vehicle_no
                );


            if (!latestPacket) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | GPS TIMESTAMP VALIDATION
            |--------------------------------------------------------------------------
            */

            if (
                latestPacket.gps_timestamp === undefined ||
                latestPacket.gps_timestamp === null ||
                latestPacket.gps_timestamp === ""
            ) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | FORMAT GPS TIME
            |--------------------------------------------------------------------------
            */

            const packetTimeString =
                await geoHelper.formatServerTime(
                    latestPacket.gps_timestamp
                );


            if (!packetTimeString) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | PARSE GPS TIME AS IST
            |--------------------------------------------------------------------------
            */

            const packetReceiveTime =
                moment(
                    packetTimeString,
                    "YYYY-MM-DD HH:mm:ss",
                    true
                ).utcOffset(
                    "+05:30",
                    true
                );


            if (!packetReceiveTime.isValid()) {

                console.warn(
                    `[SearchingNetwork] INVALID PACKET TIME | ` +
                    `VEHICLE: ${vehicle.vehicle_no} | ` +
                    `TIME: ${packetTimeString}`
                );

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | CURRENT IST TIME
            |--------------------------------------------------------------------------
            */

            const currentTime =
                moment().utcOffset("+05:30");


            /*
            |--------------------------------------------------------------------------
            | NETWORK GAP
            |--------------------------------------------------------------------------
            */

            const diffMinutes =
                currentTime.diff(
                    packetReceiveTime,
                    "minutes"
                );

            if (diffMinutes < 0) {
                return;
            }

            let state =
                await RedisRepository.getSearchingNetworkState(
                    vehicle.vehicle_no,
                    activeTrip.trip_id
                );


            /*
            |--------------------------------------------------------------------------
            | DEFAULT STATE
            |--------------------------------------------------------------------------
            */

            if (
                !state ||
                typeof state !== "object"
            ) {

                state = {
                    alertSent: false
                };

            }


            /*
            |--------------------------------------------------------------------------
            | NETWORK NORMAL
            |--------------------------------------------------------------------------
            */

            if (
                diffMinutes < alertTime
            ) {

                /*
                |--------------------------------------------------------------------------
                | NETWORK RESTORED
                |--------------------------------------------------------------------------
                */

                if (
                    state.alertSent === true
                ) {

                    // console.log(
                    //     `[SearchingNetwork] NETWORK RESTORED | ` +
                    //     `VEHICLE: ${vehicle.vehicle_no} | ` +
                    //     `TRIP: ${activeTrip.trip_id} | ` +
                    //     `GAP: ${diffMinutes} MIN`
                    // );

                }


                state.alertSent = false;


                await RedisRepository.saveSearchingNetworkState(
                    vehicle.vehicle_no,
                    activeTrip.trip_id,
                    state
                );


                return;
            }


            /*
            |--------------------------------------------------------------------------
            | ALERT ALREADY SENT
            |--------------------------------------------------------------------------
            */

            if (
                state.alertSent === true
            ) {
                return;
            }


            /*
            |--------------------------------------------------------------------------
            | QUEUE SEARCHING NETWORK ALERT
            |--------------------------------------------------------------------------
            */

            const queued =
                await this.saveSearchingNetworkAlert(
                    activeTrip,
                    vehicle,
                    latestPacket,
                    searchingNetworkAlert,
                    diffMinutes,
                    packetReceiveTime.format(
                        "YYYY-MM-DD HH:mm:ss"
                    )
                );


            /*
            |--------------------------------------------------------------------------
            | QUEUE FAILED
            |--------------------------------------------------------------------------
            */

            if (!queued) {

                console.error(
                    `[SearchingNetwork] QUEUE FAILED | ` +
                    `VEHICLE: ${vehicle.vehicle_no} | ` +
                    `TRIP: ${activeTrip.trip_id}`
                );

                /*
                |--------------------------------------------------------------------------
                | Retry next cycle
                |--------------------------------------------------------------------------
                */

                return;
            }


            /*
            |--------------------------------------------------------------------------
            | MARK ALERT SENT
            |--------------------------------------------------------------------------
            */

            state.alertSent = true;


            await RedisRepository.saveSearchingNetworkState(
                vehicle.vehicle_no,
                activeTrip.trip_id,
                state
            );

        } catch (error) {

            console.error(
                "[SearchingNetwork] PROCESS ERROR:",
                error
            );

        }

    }

    static async saveSearchingNetworkAlert(
        activeTrip,
        vehicle,
        payload,
        searchingNetworkAlert,
        diffMinutes,
        packetReceiveTime
    ) {

        try {

            /*
            |--------------------------------------------------------------------------
            | VALIDATION
            |--------------------------------------------------------------------------
            */

            if (!activeTrip) {
                return false;
            }


            if (!vehicle?.vehicle_no) {
                return false;
            }


            if (!searchingNetworkAlert) {
                return false;
            }


            /*
            |--------------------------------------------------------------------------
            | TRIP ID
            |--------------------------------------------------------------------------
            */

            const tripId =
                Number(activeTrip.trip_id);


            if (
                !Number.isFinite(tripId) ||
                tripId <= 0
            ) {

                console.error(
                    `[SearchingNetwork] INVALID TRIP ID | ` +
                    `${activeTrip.id}`
                );

                return false;
            }


            /*
            |--------------------------------------------------------------------------
            | SUBTRIP ID
            |--------------------------------------------------------------------------
            */

            const subtripId =
                activeTrip.sub_trip_id ??
                activeTrip.subtrip_id ??
                activeTrip.subtripId ??
                null;

            /*
            |--------------------------------------------------------------------------
            | VEHICLE NUMBER
            |--------------------------------------------------------------------------
            */

            const vehicleNo =
                vehicle.vehicle_no;


            /*
            |--------------------------------------------------------------------------
            | ALERT NAME
            |--------------------------------------------------------------------------
            */

            const alertName =
                searchingNetworkAlert.alert_name ||
                "Searching Network";


            /*
            |--------------------------------------------------------------------------
            | DISPLAY TRIP ID
            |--------------------------------------------------------------------------
            */

            const displayTripId =
                activeTrip.trip_id ||
                "-";


            /*
            |--------------------------------------------------------------------------
            | MESSAGE
            |--------------------------------------------------------------------------
            |
            | Reverse geocode intentionally nahi kar rahe.
            | Isse unnecessary external/API load aur event-loop blocking
            | avoid hoga.
            |
            |--------------------------------------------------------------------------
            */

            const message =
                `${alertName}\n` +
                `Vehicle : ${vehicleNo}\n` +
                `Trip ID : ${displayTripId}\n` +
                `Last Packet Time : ${packetReceiveTime}\n` +
                `Network Gap : ${diffMinutes} Minutes\n` +
                `Vehicle is currently searching for network.`;


            /*
            |--------------------------------------------------------------------------
            | COMMON ALERT OBJECT
            |--------------------------------------------------------------------------
            */

            const alertData = {

                trip_id:
                    tripId,

                subtrip_id:
                    subtripId,

                vehicle_no:
                    vehicleNo,

                alert_id:
                    this.ALERT_ID,

                message:
                    message,

                

                last_location: alertLocation(payload),

                last_date_time:
                    packetReceiveTime,

                /*
                |--------------------------------------------------------------------------
                | NETWORK GAP
                |--------------------------------------------------------------------------
                |
                | Existing schema mein dedicated searching_network_minutes
                | field nahi hai, isliye current common structure mein
                | diffMinutes long_halt_minutes mein ja raha hai.
                |
                |--------------------------------------------------------------------------
                */

                long_halt_minutes:
                    diffMinutes,

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

            };


            /*
            |--------------------------------------------------------------------------
            | COMMON VEHICLE ALERT QUEUE
            |--------------------------------------------------------------------------
            */

            const queued =
                await RedisRepository.addVehicleAlert(
                    alertData
                );


            /*
            |--------------------------------------------------------------------------
            | QUEUE FAILED
            |--------------------------------------------------------------------------
            */

            if (!queued) {

                console.error(
                    `[SearchingNetwork] COMMON QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TRIP: ${tripId}`
                );

                return false;
            }


            /*
            |--------------------------------------------------------------------------
            | SUCCESS
            |--------------------------------------------------------------------------
            */

            return true;


        } catch (error) {

            console.error(
                "[SearchingNetwork] SAVE ALERT ERROR:",
                error
            );

            return false;
        }

    }

}


module.exports =
    SearchingNetworkService;