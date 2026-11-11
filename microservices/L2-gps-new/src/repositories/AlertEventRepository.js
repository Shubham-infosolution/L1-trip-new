const writeBatch = require("../db/writeBatch");
const db = require("../model/dbquery");
const IST_NOW = "CONVERT_TZ(NOW(), @@session.time_zone, '+05:30')";

class AlertEventRepository {

    static async savealertsEventLog(data) {
        return db.query(`INSERT INTO tbl_vehicle_alert_event
            (trip_id, vehicle_no, alert_id, start_date_time, duration_minutes,
             start_lat, start_lng, off_route_in_km, status, c_date, d_date)
            VALUES (?, ?, ?, ${IST_NOW}, ?, ?, ?, ?, 1, ${IST_NOW}, ${IST_NOW})`,
            [data.trip_id, data.vehicle_no, data.alert_id, Number(data.duration_minutes || 0),
             data.start_lat, data.start_lng, data.off_route_in_km || 0]);
    }

    static async updateAlertEvent(eventId, data) {
        return writeBatch.update(`UPDATE tbl_vehicle_alert_event SET
            end_date_time = ${IST_NOW},
            duration_minutes = ROUND(TIMESTAMPDIFF(SECOND, start_date_time, ${IST_NOW}) / 60, 2),
            end_lat = ?, end_lng = ?, off_route_in_km = ?, d_date = ${IST_NOW}
            WHERE id = ? AND status = 1`,
            [data.end_lat, data.end_lng, data.off_route_in_km || 0, eventId]);
    }

    static async updateIdleEvent(eventId, data) {
        return writeBatch.update(`UPDATE tbl_vehicle_alert_event SET
            duration_minutes = ?, end_lat = ?, end_lng = ?, d_date = ${IST_NOW}
            WHERE id = ? AND status = 1 AND end_date_time IS NULL`,
            [Number(data.duration_minutes || 0), data.lat ?? null, data.lng ?? null, eventId]);
    }

    static async getActiveAlertEvent(
        vehicleNo,
        tripId,
        alertId
    ) {

        const result =
            await db.query(`

                SELECT *

                FROM tbl_vehicle_alert_event

                WHERE

                    vehicle_no = ?

                    AND trip_id = ?

                    AND alert_id = ?

                    AND end_date_time IS NULL

                    AND status = 1

                ORDER BY
                    id DESC

                LIMIT 1

            `, [

                vehicleNo,
                tripId,
                alertId

            ]);

return result?.[0] || null;

    }

}
module.exports = AlertEventRepository;
