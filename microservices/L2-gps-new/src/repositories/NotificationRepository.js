const writeBatch = require("../db/writeBatch");
const db = require("../model/dbquery");

class NotificationRepository {
    static async getAlertNames(alertIds) {
        const ids = [...new Set(alertIds.map(Number))];
        if (!ids.length) return [];
        if (ids.some(id => !Number.isSafeInteger(id) || id <= 0)) throw new Error('Positive alert IDs required');
        return db.joinSelect(`SELECT id, alert_name FROM tbl_alert_master WHERE id IN (${ids.map(() => '?').join(',')})`, ids);
    }


    // =====================================================
    // DISTRICT USERS
    // =====================================================

    static async getDistrictUsers(
        districtId,
        userTypes
    ) {

        try {

            const result = await db.joinSelect(`
                SELECT DISTINCT
                    uda.user_id
                FROM tbl_user_districtwise_store_assignment uda
                WHERE uda.district_id = ${districtId}
                AND uda.status = 1
                AND uda.user_type IN (${userTypes})
            `);

            return result || [];

        } catch (error) {

            console.error(
                `[NotificationRepository] GET DISTRICT USERS ERROR:`,
                error
            );

            throw error;
        }
    }


    // =====================================================
    // STORE + DISTRICT + STATE + DRIVER + TRANSPORTER USERS
    // =====================================================

    static async getStoreUsers(district_id, store_id, userTypes = "", driver_id = null, transporter_id = null) {
        const types = String(userTypes).split(',').map(value => value.trim()).filter(Boolean).map(Number);
        if (types.some(value => !Number.isInteger(value) || value <= 0)) {
            throw new Error('Invalid notification userTypes');
        }
        const conditions = ['um.user_type = 2'];
        const values = [];
        conditions.push(`EXISTS (
            SELECT 1 FROM tbl_user_districtwise_store_assignment uda
            WHERE uda.user_id = um.id AND uda.district_id = ?
              AND uda.store_id IS NULL AND uda.user_type = 3 AND uda.status = 1
        )`);
        values.push(district_id ?? null);
        if (types.length) {
            conditions.push(`EXISTS (
                SELECT 1 FROM tbl_user_districtwise_store_assignment uda
                WHERE uda.user_id = um.id AND uda.district_id = ? AND uda.store_id = ?
                  AND uda.user_type IN (${types.map(() => '?').join(',')}) AND uda.status = 1
            )`);
            values.push(district_id ?? null, store_id ?? null, ...types);
        }
        if (driver_id) { conditions.push('(um.id = ? AND um.user_type = 6)'); values.push(driver_id); }
        if (transporter_id) { conditions.push('(um.id = ? AND um.user_type = 5)'); values.push(transporter_id); }
        return await db.joinSelect(`
            SELECT um.id AS user_id FROM tbl_user_master um
            WHERE um.status = 1 AND (${conditions.join(' OR ')})
            GROUP BY um.id
        `, values) || [];
    }

    static async bulkInsertNotifications(notifications) {
        return require("../services/notificationDelivery")(notifications);
    }

    // =====================================================
    // GEO FENCE HISTORY NOTIFICATION
    // =====================================================

    static async savegeo_fence_historyNotification(
        data
    ) {

        try {
            const result = await writeBatch.insert(`
                INSERT INTO tbl_geo_fence_history_notifications
                (
                    district_id,
                    vehicle_no,
                    level,
                    title,
                    message,
                    notification_type,
                    reference_id
                )
                VALUES
                (?, ?, ?, ?, ?, ?, ?)
            `, [

                data.district_id ?? null,

                data.vehicle_no ?? null,

                data.level ?? null,

                data.title ?? null,

                data.message ?? null,

                data.notification_type ?? null,

                data.reference_id ?? null

            ]);

            return result;

        } catch (error) {

            console.error(
                `[NotificationRepository] ` +
                `GEO FENCE HISTORY INSERT ERROR:`,
                error
            );

            throw error;
        }
    }
}


module.exports = NotificationRepository;

