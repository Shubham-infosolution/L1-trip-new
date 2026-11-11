const db = require("../model/dbquery");
class RouteRepository {

    static async getCurrentRoute(
        trip,
        vehicleLevel
    ) {

        try {

            if (!trip) {
                return null;
            }

if (!trip.route_id) {

                return null;
            }

const result =
                await db.query(
                    `
                    SELECT
                        rm.id,
                        rm.route_name,
                        rm.polystring AS polyline,
                        rm.source_store_id,
                        rm.destination_store_id

                    FROM tbl_route_master rm

                    WHERE rm.id = ?

                    LIMIT 1
                    `,
                    [
                        trip.route_id
                    ]
                );

return result?.[0] || null;

}
        catch (error) {

            console.log(
                "GET CURRENT ROUTE ERROR:",
                error
            );

            return null;
        }
    }

    static async getOffRouteAlertConfig() {

        const result = await db.joinSelect(`
                SELECT
                    id,alert_name,
                    alert_time,alert_reminder_time,
                    alert_interval
                FROM tbl_alert_master
                WHERE
                    id = 5
                AND
                    status = 1
                LIMIT 1
            `);

        return result?.[0] || null;

    }

}

module.exports = RouteRepository;

require('../db/cacheRead')(RouteRepository, 'getOffRouteAlertConfig');
