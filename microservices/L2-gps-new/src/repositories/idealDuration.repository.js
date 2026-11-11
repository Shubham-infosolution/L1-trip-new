const db = require("../model/dbquery");

class IdealDurationRepository {

    static async getCurrentISTDateTime() {

        try {

            const result =
                await db.query(`

                    SELECT
                        CONVERT_TZ(
                            NOW(),
                            @@session.time_zone,
                            '+05:30'
                        ) AS current_ist

                `);

return (
                result?.[0]?.current_ist ||
                null
            );

        }
        catch (error) {

            console.error(
                "GET CURRENT IST ERROR:",
                error
            );

            return null;

        }

    }

}

module.exports =
    IdealDurationRepository;
