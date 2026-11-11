

const db = require("../model/dbquery");
const filterAlertLocations = require("../utils/filterAlertLocations");
const RedisRepository = require("../repositories/RedisRepository");

class TripRepository {

    static async getActiveTripFromDB(vehicle_no) {

        const vehicleNumber =
            String(vehicle_no ?? "").trim();

        if (!vehicleNumber) {
            return [];
        }

        const sql = `
            SELECT
                tp.id AS trip_id,
                tp.district_id,

                sp.id AS sub_trip_id,

                tp.trip_id AS trip_unique_id,
                tp.vehicle_no,
                tp.route_id,
                tp.trip_status,

                tp.route_total_distance,
                tp.travelled_distance,
                tp.actual_travelled_distance,
                tp.remaining_distance,
                tp.route_percentage,

                tp.trip_date,
                tp.expected_arrival_time,
                tp.completion_date,

                tp.average_speed,
                tp.estimated_remaining_minutes,
                tp.calculated_expected_delivery_time,

                tp.driver_id,

                rt.source_store_id,
                rt.destination_store_id,

                src.store_name AS source_store_name,
                src.lat AS source_lat,
                src.lng AS source_lng,

                dest.store_name AS destination_store_name,
                dest.lat AS destination_lat,
                dest.lng AS destination_lng,

                rt.time_duration

            FROM tbl_trip tp

            LEFT JOIN tbl_subtrip sp
                ON sp.trip_id = tp.id
                AND sp.subtrip_status = 1

            LEFT JOIN tbl_route_master rt
                ON rt.id = tp.route_id
                AND rt.status = 1

            LEFT JOIN tbl_store_location_master src
                ON src.id = rt.source_store_id
                AND src.status = 1

            LEFT JOIN tbl_store_location_master dest
                ON dest.id = rt.destination_store_id
                AND dest.status = 1

            WHERE
                tp.level = 2
                AND tp.vehicle_no = ?
                AND tp.trip_status = 1
                AND tp.status = 1

            ORDER BY tp.id DESC
        `;

        const result =
            await db.joinSelect(
                sql,
                [vehicleNumber]
            );

        return Array.isArray(result)
            ? result
            : [];
    }

    static async getActiveTrip(vehicle_no) {

        const vehicleNumber =
            String(vehicle_no ?? "").trim();

        if (!vehicleNumber) {
            return [];
        }

const cachedTrips =
            await RedisRepository.getActiveTrip(
                vehicleNumber,
                2
            );

        if (
            Array.isArray(cachedTrips) &&
            cachedTrips.length > 0
        ) {
            return cachedTrips;
        }

const trips =
            await this.getActiveTripFromDB(
                vehicleNumber
            );

        if (
            !Array.isArray(trips) ||
            trips.length === 0
        ) {
            return [];
        }

await RedisRepository.saveActiveTrip(
            vehicleNumber,
            2,
            trips
        );

        return trips;
    }

    static async getAlertConfig(alertId) {

        const rows =
            await db.query(`
                SELECT
                    id AS alert_id,
                    alert_name,
                    alert_short_name,
                    alert_time

                FROM tbl_alert_master

                WHERE
                    id = ?
                    AND status = 1

                LIMIT 1
            `, [
                alertId
            ]);

        return rows.length > 0
            ? rows[0]
            : null;
    }

    static async getActiveTripVehicles() {

        return await db.query(`
            SELECT
                tp.id,
                tp.trip_id,
                stp.id AS sub_trip_id,
                tp.vehicle_no,
                tp.trip_status,
                vm.district_id

            FROM tbl_trip tp

            LEFT JOIN tbl_subtrip stp
                ON tp.id = stp.trip_id
                AND stp.status = 1

            INNER JOIN tbl_vehicle_master vm
                ON vm.vehicle_no = tp.vehicle_no

            WHERE
                tp.level = 2
                AND tp.trip_status = 1
                AND tp.status = 1
        `);
    }

    static async bulkInsertAlertDetails(alerts = []) {
        if (!Array.isArray(alerts)) throw new TypeError('Expected alerts array');
        const valid = filterAlertLocations(alerts);
        if (!valid.length) return {affectedRows: 0, insertedAlerts: [], skippedCount: alerts.length};
        const ids = [...new Set(valid.map(row => Number(row.trip_id))
            .filter(id => Number.isSafeInteger(id) && id > 0))].sort((a,b) => a-b);
        if (!ids.length) return {affectedRows: 0, insertedAlerts: [], skippedCount: alerts.length};
        return db.withTransaction(async tx => {
            // Lock current trip rows until INSERT commit. Manual completion either
            // commits first and is skipped, or waits for this transaction to finish.
            // Never use the cached active-trip snapshot for this final decision.
            const trips = await tx.joinSelect(`
                SELECT id, vehicle_no, trip_status, status, level FROM tbl_trip
                WHERE id IN (${ids.map(() => '?').join(',')})
                ORDER BY id FOR UPDATE
            `, ids);
            const active = new Map(trips.filter(trip => Number(trip.trip_status) === 1
                && Number(trip.status) === 1 && Number(trip.level) === 2)
                .map(trip => [Number(trip.id), String(trip.vehicle_no).trim().toUpperCase()]));
            const eligible = valid.filter(row => active.has(Number(row.trip_id))
                && active.get(Number(row.trip_id)) === String(row.vehicle_no || '').trim().toUpperCase());
            const size = require('../db/settings').integer('DB_BATCH_SIZE', 200, 1, 1000);
            let affectedRows = 0;
            for (let i = 0; i < eligible.length; i += size) {
                const chunk = eligible.slice(i, i + size);
                const result = await this.insertActiveAlertDetails(chunk);
                if (Number(result?.affectedRows) !== chunk.length) throw new Error('Active alert INSERT count mismatch');
                affectedRows += result.affectedRows;
            }
            return {affectedRows, insertedAlerts: eligible, skippedCount: alerts.length - eligible.length};
        });
    }

    static async insertActiveAlertDetails(alerts) {

        try {

            if (
                !Array.isArray(alerts) ||
                alerts.length === 0
            ) {
                return {
                    affectedRows: 0
                };
            }

            // Skip alerts unless both coordinates are present and valid.
            alerts = filterAlertLocations(alerts);

            if (alerts.length === 0) return { affectedRows: 0 };

            const values = [];

            const placeholders =
                alerts.map(alert => {            

values.push(

                        alert.trip_id ?? null,

                        alert.subtrip_id ?? null,

                        alert.vehicle_no ?? null,

                        alert.alert_id ?? null,

                        alert.message ?? null,

                        alert.last_location ?? null,

                        alert.last_date_time ?? null,

                        alert.long_halt_minutes ?? null,

                        alert.speed ?? null,

                        alert.off_route_in_km ?? null,

                        alert.district_id ?? null,

                        alert.status ?? 1,

                        alert.in_out_status ?? null,

                        alert.geofence ?? null
                    );

return `(
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        CONVERT_TZ(
                            NOW(),
                            @@session.time_zone,
                            '+05:30'
                        ),
                        ?, ?, ?, ?
                    )`;

                }).join(",");

const query = `
                INSERT INTO tbl_vehicle_alert_details
                (
                    trip_id,
                    subtrip_id,
                    vehicle_no,
                    alert_id,
                    message,
                    last_location,
                    last_date_time,
                    long_halt_minutes,
                    speed,
                    off_route_in_km,
                    c_date,
                    district_id,
                    status,
                    in_out_status,
                    geofence
                )
                VALUES
                    ${placeholders}
            `;

            const result =
                await db.query(
                    query,
                    values
                );

            return result;

} catch (error) {

            console.error(
                "[TripRepository] BULK ALERT INSERT ERROR:",
                error
            );

            throw error;
        }
    }
}

module.exports = TripRepository;



require("../db/cacheRead")(
    TripRepository,
    "getAlertConfig"
);

