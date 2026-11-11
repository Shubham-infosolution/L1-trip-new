const alertLocation = require("../utils/alertLocation");
const moment = require("moment");

const TripRepository = require("../repositories/TripRepository");
const RedisRepository = require("../repositories/RedisRepository");
const geoHelper = require("../helpers/geoHelper");

class LongHaltService {
    static ALERT_ID = 2;

    static async process(vehicle, payload) {
        try {
            const vehicleNo = String(vehicle?.vehicle_no || "").trim();

            if (!vehicleNo) {
                return;
            }

            // Validate GPS timestamp
            const gpsTimestamp = Number(payload?.gps_timestamp);

            if (!Number.isFinite(gpsTimestamp) || gpsTimestamp <= 0) {
               
                return;
            }

            const packetReceiveTime =
                geoHelper.formatServerTime(gpsTimestamp);
         
            if (!packetReceiveTime) {
                return;
            }

            const activeTrips =
                await TripRepository.getActiveTrip(vehicleNo);


            if (!Array.isArray(activeTrips) || activeTrips.length === 0) {
                await RedisRepository.deleteLongHaltState(vehicleNo);
                return;
            }

            // Select latest active trip using database trip ID
            const activeTrip = activeTrips.reduce((latest, current) =>
                Number(current?.trip_id || 0) >
                Number(latest?.trip_id || 0)
                    ? current
                    : latest
            );

            if (!activeTrip || Number(activeTrip.trip_status) !== 1) {
                await RedisRepository.deleteLongHaltState(vehicleNo);
                return;
            }

            // Get long-halt alert configuration
            const longHaltAlert =
                await TripRepository.getAlertConfig(this.ALERT_ID);

            if (!longHaltAlert) {
                return;
            }

            const alertTime = Number(longHaltAlert.alert_time || 0);

            if (!Number.isFinite(alertTime) || alertTime <= 0) {
                console.log(
                    `[LongHalt] INVALID ALERT TIME | VEHICLE: ${vehicleNo}`
                );
                return;
            }

            const speed = Number(payload?.speed ?? 0);

            if (!Number.isFinite(speed)) {
                return;
            }

            let state =
                await RedisRepository.getLongHaltState(vehicleNo);

            // Remove outdated Redis state format
            if (
                state &&
                (
                    state.lastAlertNumber !== undefined ||
                    state.alert1Sent === undefined ||
                    state.alert2Sent === undefined
                )
            ) {
                await RedisRepository.deleteLongHaltState(vehicleNo);
                state = null;
            }

            // Vehicle is moving: clear halt state
            if (speed > 1) {
                await RedisRepository.deleteLongHaltState(vehicleNo);
                return;
            }

            // First stationary packet: initialize halt timer
            if (!state) {
                state = {
                    haltStartTime: packetReceiveTime,
                    alert1Sent: false,
                    alert2Sent: false
                };

                await RedisRepository.saveLongHaltState(
                    vehicleNo,
                    state
                );

                return;
            }

            const haltStartTime = state.haltStartTime;

            if (!haltStartTime) {
                state = {
                    haltStartTime: packetReceiveTime,
                    alert1Sent: false,
                    alert2Sent: false
                };

                await RedisRepository.saveLongHaltState(
                    vehicleNo,
                    state
                );

                return;
            }

            // Parse GPS-based halt times
            const haltStart = moment(
                haltStartTime,
                "YYYY-MM-DD HH:mm:ss",
                true
            );

            const currentTime = moment(
                packetReceiveTime,
                "YYYY-MM-DD HH:mm:ss",
                true
            );

            if (!haltStart.isValid() || !currentTime.isValid()) {
                console.error(
                    `[LongHalt] INVALID TIME | VEHICLE: ${vehicleNo} | ` +
                    `START: ${haltStartTime} | CURRENT: ${packetReceiveTime}`
                );
                return;
            }

            // Reset timer if an older GPS packet arrives
            if (currentTime.isBefore(haltStart)) {
                state = {
                    haltStartTime: packetReceiveTime,
                    alert1Sent: false,
                    alert2Sent: false
                };

                await RedisRepository.saveLongHaltState(
                    vehicleNo,
                    state
                );

                return;
            }

            // Calculate halt duration in minutes
            const haltMinutes = Math.max(
                0,
                currentTime.diff(haltStart, "minutes")
            );

            const alertId = Number(
                longHaltAlert.alert_id || this.ALERT_ID
            );

            // ALERT 1: after alert_time minutes
            if (haltMinutes >= alertTime && !state.alert1Sent) {
                const alertQueued =
                    await this.generateLongHaltAlert(
                        activeTrip,
                        vehicle,
                        payload,
                        alertId,
                        haltMinutes,
                        packetReceiveTime,
                        speed,
                        `halt:${haltStartTime}:1`
                    );

                if (alertQueued) {
                    state.alert1Sent = true;
                }
            }

            // ALERT 2: after 2 * alert_time minutes
            // Separate condition so Alert 2 can be checked independently.
            if (
                haltMinutes >= alertTime * 2 &&
                !state.alert2Sent
            ) {
                const alertQueued =
                    await this.generateLongHaltAlert(
                        activeTrip,
                        vehicle,
                        payload,
                        alertId,
                        haltMinutes,
                        packetReceiveTime,
                        speed,
                        `halt:${haltStartTime}:2`
                    );

                if (alertQueued) {
                    state.alert2Sent = true;
                }
            }

            // Preserve original GPS-based halt start time
            state.haltStartTime = haltStartTime;

            await RedisRepository.saveLongHaltState(
                vehicleNo,
                state
            );

        } catch (error) {
            console.error(
                `${vehicle?.vehicle_no || "UNKNOWN"} | LONG HALT ERROR:`,
                error
            );
        }
    }

    static async generateLongHaltAlert(
        activeTrip,
        vehicle,
        payload,
        alertId,
        haltMinutes,
        packetReceiveTime,
        speed,
        eventKey
    ) {
        try {
            const vehicleNo =
                String(vehicle?.vehicle_no || "").trim();

            if (!vehicleNo || !activeTrip) {
                return false;
            }

            // Format halt duration
            const hours = Math.floor(haltMinutes / 60);
            const minutes = haltMinutes % 60;

            let haltDuration = "";

            if (hours > 0) {
                haltDuration += `${hours} Hr `;
            }

            if (minutes > 0 || hours === 0) {
                haltDuration += `${minutes} Min`;
            }

            // Alert message uses GPS time
            const message =
                `Long Halt\n` +
                `Vehicle : ${vehicleNo}\n` +
                `Trip ID : ${activeTrip.trip_unique_id}\n` +
                `Last Activity Time : ${packetReceiveTime}\n` +
                `Halt Duration : ${haltDuration}\n` +
                `Vehicle has remained stationary for an extended period.`;

            // Queue alert with GPS time as last_date_time
            const queueResult =
                await RedisRepository.addVehicleAlert({
                    event_key: eventKey,
                    trip_id: activeTrip.trip_id,

                    subtrip_id:
                        activeTrip.sub_trip_id ||
                        activeTrip.subtrip_id ||
                        null,

                    vehicle_no: vehicleNo,
                    alert_id: Number(alertId),

                    district_id:
                        activeTrip.district_id ??
                        vehicle?.district_id ??
                        null,

                    message,
                    last_location: alertLocation(payload),
                    last_date_time: packetReceiveTime,
                    long_halt_minutes: haltMinutes,
                    speed,
                    status: 1
                });

            if (!queueResult) {
                console.error(
                    `[LongHalt] ALERT QUEUE FAILED | ` +
                    `VEHICLE: ${vehicleNo} | ` +
                    `TRIP: ${activeTrip.trip_id} | ` +
                    `ALERT_ID: ${alertId}`
                );

                return false;
            }

            return true;

        } catch (error) {
            console.error(
                `${vehicle?.vehicle_no || "UNKNOWN"} | ` +
                `LONG HALT ALERT QUEUE ERROR:`,
                error
            );

            return false;
        }
    }
}

module.exports = LongHaltService;
