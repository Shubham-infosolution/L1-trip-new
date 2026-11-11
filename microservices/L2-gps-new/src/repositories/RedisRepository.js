const { randomUUID } = require("node:crypto");
const Redis = require("ioredis");
const vehicleAlertIdentity = require('../utils/vehicleAlertIdentity');

const redisClient = process.env.REDIS_URL
    ? new Redis(process.env.REDIS_URL)
    : new Redis();

class RedisRepository {

    static getClient() { return redisClient; }

    // Offline state belongs to a vehicle + trip, independently of trip cache TTL.
    static getOfflineStateKey(vehicleTripKey) {
        return `offline:${String(vehicleTripKey).trim()}`;
    }

    static async getOfflineState(vehicleTripKey) {
        if (!vehicleTripKey) return null;

        const data = await redisClient.get(this.getOfflineStateKey(vehicleTripKey));
        return data ? JSON.parse(data) : null;
    }

    static async saveOfflineState(vehicleTripKey, state) {
        if (!vehicleTripKey || !state) return false;

        // Do not expire deduplication state while a trip may still be offline.
        // Redis failures propagate so the service cannot assume persistence.
        await redisClient.set(
            this.getOfflineStateKey(vehicleTripKey),
            JSON.stringify(state)
        );
        return true;
    }

    // =========================================================
    // STORE CACHE
    // =========================================================

    static async isLatestPacket(
        vehicleNo,
        gpsTimestamp
    ) {
        try {
            const key = `packet:${vehicleNo}`;

            const lastPacket =
                await redisClient.get(key);

            if (!lastPacket) {
                return true;
            }

            const packet =
                JSON.parse(lastPacket);

            return (
                Number(gpsTimestamp) >
                Number(packet.gps_timestamp)
            );

        } catch (error) {
            console.error(
                `${vehicleNo} | CHECK LATEST PACKET ERROR:`,
                error
            );

            return true;
        }
    }

    static async saveLatestPacket(
        vehicleNo,
        payload
    ) {
        try {
            await redisClient.set(
                `packet:${vehicleNo}`,
                JSON.stringify(payload)
            );

            return true;

        } catch (error) {
            console.error(
                `${vehicleNo} | SAVE LATEST PACKET ERROR:`,
                error
            );

            return false;
        }
    }

    static async getLatestPacket(
        vehicleNo
    ) {
        try {
            const data =
                await redisClient.get(
                    `packet:${vehicleNo}`
                );

            if (!data) {
                return null;
            }

            return JSON.parse(data);

        } catch (error) {
            console.error(
                `${vehicleNo} | GET LATEST PACKET ERROR:`,
                error
            );

            return null;
        }
    }

static getLongHaltKey(vehicleNo) {
    return `long_halt:${vehicleNo}`;
}

static async getLongHaltState(vehicleNo) {
    try {

        const key =
            this.getLongHaltKey(vehicleNo);

        const data =
            await redisClient.get(key);

        if (!data) {
            return null;
        }

        return JSON.parse(data);

    } catch (error) {

        console.error(
            `[RedisRepository] GET LONG HALT STATE ERROR | ${vehicleNo}:`,
            error
        );

        return null;
    }
}

static async saveLongHaltState(
    vehicleNo,
    state
) {
    try {

        const key =
            this.getLongHaltKey(vehicleNo);

        await redisClient.set(
            key,
            JSON.stringify(state)
        );

        return true;

    } catch (error) {

        console.error(
            `[RedisRepository] SAVE LONG HALT STATE ERROR | ${vehicleNo}:`,
            error
        );

        return false;
    }
}

static async deleteLongHaltState(vehicleNo) {
    try {

        const key =
            this.getLongHaltKey(vehicleNo);

        await redisClient.del(key);

        return true;

    } catch (error) {

        console.error(
            `[RedisRepository] DELETE LONG HALT STATE ERROR | ${vehicleNo}:`,
            error
        );

        return false;
    }
}

    // =========================================================
    // OFF ROUTE REDIS STATE
    // =========================================================

    static getOffRouteStateKey(vehicleNo, tripId) {
        return `offroute:${vehicleNo}:${tripId}`;
    }

static async getOffRouteState(vehicleNo, tripId) {
        try {

            const key =
                this.getOffRouteStateKey(
                    vehicleNo,
                    tripId
                );

            const data =
                await redisClient.get(key);

            if (!data) {
                return null;
            }

            const state =
                JSON.parse(data);

            return state;

        } catch (error) {

            console.error(
                `[RedisRepository] GET OFF ROUTE STATE ERROR | ` +
                `VEHICLE: ${vehicleNo} | ` +
                `TRIP: ${tripId}`,
                error
            );

            return null;
        }
    }

static async saveOffRouteState(
        vehicleNo,
        tripId,
        state
    ) {
        try {

            const key =
                this.getOffRouteStateKey(
                    vehicleNo,
                    tripId
                );

            await redisClient.set(
                key,
                JSON.stringify(state)
            );

            return true;

        } catch (error) {

            console.error(
                `[RedisRepository] SAVE OFF ROUTE STATE ERROR | ` +
                `VEHICLE: ${vehicleNo} | ` +
                `TRIP: ${tripId}`,
                error
            );

            return false;
        }
    }

static async deleteOffRouteState(
        vehicleNo,
        tripId
    ) {
        try {

            const key =
                this.getOffRouteStateKey(
                    vehicleNo,
                    tripId
                );

            await redisClient.del(key);

            return true;

        } catch (error) {

            console.error(
                `[RedisRepository] DELETE OFF ROUTE STATE ERROR | ` +
                `VEHICLE: ${vehicleNo} | ` +
                `TRIP: ${tripId}`,
                error
            );

            return false;
        }
    }

static getOverSpeedStateKey(vehicleNo, tripId) {
    return `overspeed:${vehicleNo}:${tripId}`;
}

static async getOverSpeedState(
    vehicleNo,
    tripId = null
) {
    try {

        // Backward compatibility:
        // agar caller already complete key bhej raha hai
        const key =
            tripId !== null
                ? this.getOverSpeedStateKey(
                    vehicleNo,
                    tripId
                )
                : `overspeed:${vehicleNo}`;

        const data =
            await redisClient.get(key);

        if (!data) {
            return null;
        }

        return JSON.parse(data);

    } catch (error) {

        console.error(
            "[Redis] GET OVER SPEED STATE ERROR:",
            error.message
        );

        return null;
    }
}

static async saveOverSpeedState(
    vehicleNo,
    tripId,
    state = null
) {
    try {

        let key;
        let stateData;

        // Support both:
        // saveOverSpeedState(key, state)
        // saveOverSpeedState(vehicleNo, tripId, state)

        if (
            state === null &&
            typeof tripId === "object"
        ) {

            key =
                `overspeed:${vehicleNo}`;

            stateData =
                tripId;

        } else {

            key =
                this.getOverSpeedStateKey(
                    vehicleNo,
                    tripId
                );

            stateData =
                state;
        }

        if (!stateData) {
            return false;
        }

        await redisClient.set(
            key,
            JSON.stringify(stateData)
        );

        return true;

    } catch (error) {

        console.error(
            "[Redis] SAVE OVER SPEED STATE ERROR:",
            error.message
        );

        return false;
    }
}

static getIgnitionOffKey(vehicleNo) {

    return `ignition_off:${String(vehicleNo).trim()}`;

}

/*
|--------------------------------------------------------------------------
| SAVE IGNITION OFF START
|--------------------------------------------------------------------------
*/

static async saveIgnitionOffStart(
    vehicleNo,
    startTime
) {

    try {

        const key =
            this.getIgnitionOffKey(
                vehicleNo
            );

        await redisClient.set(
            key,
            String(startTime)
        );

        return true;

    } catch (error) {

        console.error(
            `${vehicleNo} | ` +
            `REDIS IGNITION OFF SAVE ERROR:`,
            error
        );

        return false;
    }

}

/*
|--------------------------------------------------------------------------
| GET IGNITION OFF START
|--------------------------------------------------------------------------
*/

static async getIgnitionOffStart(
    vehicleNo
) {

    try {

        const key =
            this.getIgnitionOffKey(
                vehicleNo
            );

        return await redisClient.get(
            key
        );

    } catch (error) {

        console.error(
            `${vehicleNo} | ` +
            `REDIS IGNITION OFF GET ERROR:`,
            error
        );

        return null;
    }

}

/*
|--------------------------------------------------------------------------
| DELETE IGNITION OFF START
|--------------------------------------------------------------------------
*/

static async deleteIgnitionOffStart(
    vehicleNo
) {

    try {

        const key =
            this.getIgnitionOffKey(
                vehicleNo
            );

        await redisClient.del(
            key
        );

        return true;

    } catch (error) {

        console.error(
            `${vehicleNo} | ` +
            `REDIS IGNITION OFF DELETE ERROR:`,
            error
        );

        return false;
    }

}

/*
|--------------------------------------------------------------------------
| SEARCHING NETWORK STATE
|--------------------------------------------------------------------------
|
| State sirf ye track karega ki current trip ke liye
| Searching Network alert already sent hai ya nahi.
|
| Alert data alag common vehicle_alert_queue mein jayega.
|
|--------------------------------------------------------------------------
*/

static getSearchingNetworkStateKey(vehicleNo, tripId) {

    return `searching_network:${String(vehicleNo).trim()}:${tripId}`;

}

/*
|--------------------------------------------------------------------------
| GET SEARCHING NETWORK STATE
|--------------------------------------------------------------------------
*/

static async getSearchingNetworkState(vehicleNo, tripId) {

    try {

        const key =
            this.getSearchingNetworkStateKey(
                vehicleNo,
                tripId
            );

        const data =
            await redisClient.get(key);

if (!data) {

            return null;

        }

try {

            return JSON.parse(data);

        } catch (error) {

            console.error(
                "[Redis] SEARCHING NETWORK STATE JSON PARSE ERROR:",
                error
            );

            return null;

        }

    } catch (error) {

        console.error(
            "[Redis] GET SEARCHING NETWORK STATE ERROR:",
            error
        );

        return null;

    }

}

/*
|--------------------------------------------------------------------------
| SAVE SEARCHING NETWORK STATE
|--------------------------------------------------------------------------
*/

static async saveSearchingNetworkState(
    vehicleNo,
    tripId,
    state
) {

    try {

        const key =
            this.getSearchingNetworkStateKey(
                vehicleNo,
                tripId
            );

await redisClient.set(
            key,
            JSON.stringify(state)
        );

return true;

    } catch (error) {

        console.error(
            "[Redis] SAVE SEARCHING NETWORK STATE ERROR:",
            error
        );

        return false;

    }

}

/*
|--------------------------------------------------------------------------
| DELETE SEARCHING NETWORK STATE
|--------------------------------------------------------------------------
|
| Trip complete hone par state remove kar sakte hain.
|
|--------------------------------------------------------------------------
*/

static getIdealDurationIgnitionKey(vehicleNo) {

    return (
        `ideal_duration_ignition:` +
        `${String(vehicleNo).trim()}`
    );

}

/*
|--------------------------------------------------------------------------
| GET IDLE START TIME
|--------------------------------------------------------------------------
*/

static async getIdealDurationIgnition(vehicleNo) {

    try {

        if (!vehicleNo) {
            return null;
        }

        const key =
            this.getIdealDurationIgnitionKey(
                vehicleNo
            );

        const data =
            await redisClient.get(key);

        if (!data) {
            return null;
        }

        return data;

    } catch (error) {

        console.error(
            "[Redis] GET IDEAL DURATION IGNITION ERROR:",
            error
        );

        return null;
    }

}

/*
|--------------------------------------------------------------------------
| SAVE IDLE START TIME
|--------------------------------------------------------------------------
*/

static async saveIdealDurationIgnition(
    vehicleNo,
    startTime
) {

    try {

        if (
            !vehicleNo ||
            !startTime
        ) {
            return false;
        }

        const key =
            this.getIdealDurationIgnitionKey(
                vehicleNo
            );

        await redisClient.set(
            key,
            String(startTime)
        );

        return true;

    } catch (error) {

        console.error(
            "[Redis] SAVE IDEAL DURATION IGNITION ERROR:",
            error
        );

        return false;
    }

}

/*
|--------------------------------------------------------------------------
| DELETE IDLE START TIME
|--------------------------------------------------------------------------
*/

static async deleteIdealDurationIgnition(
    vehicleNo
) {

    try {

        if (!vehicleNo) {
            return false;
        }

        const key =
            this.getIdealDurationIgnitionKey(
                vehicleNo
            );

        await redisClient.del(key);

        return true;

    } catch (error) {

        console.error(
            "[Redis] DELETE IDEAL DURATION IGNITION ERROR:",
            error
        );

        return false;
    }

}

/*
|--------------------------------------------------------------------------
| District Deviation State
|--------------------------------------------------------------------------
*/

static getDistrictDeviationStateKey(vehicleNo) {

    return `district_deviation:${String(vehicleNo).trim()}`;
}

/*
|--------------------------------------------------------------------------
| Get District State
|--------------------------------------------------------------------------
*/

static async getDistrictDeviationState(vehicleNo) {

    try {

        if (!vehicleNo) {
            return null;
        }

        const key =
            this.getDistrictDeviationStateKey(
                vehicleNo
            );

        const data =
            await redisClient.get(key);

        if (!data) {
            return null;
        }

        try {

            return JSON.parse(data);

        } catch (parseError) {

            console.error(
                "[Redis] Invalid district deviation JSON:",
                parseError
            );

            await redisClient.del(key);

            return null;
        }

    } catch (error) {

        console.error(
            "[Redis] getDistrictDeviationState ERROR:",
            error
        );

        throw error;
    }
}

/*
|--------------------------------------------------------------------------
| Save District State
|--------------------------------------------------------------------------
*/

static async saveDistrictDeviationState(
    vehicleNo,
    state
) {

    try {

        if (!vehicleNo || !state) {
            return false;
        }

        const key =
            this.getDistrictDeviationStateKey(
                vehicleNo
            );

        const data = {

            status:
                state.status || "IN",

            district:
                state.district ?? null,

            latitude:
                state.latitude ?? null,

            longitude:
                state.longitude ?? null,

            gps_timestamp:
                state.gps_timestamp ?? null
        };

        await redisClient.set(
            key,
            JSON.stringify(data)
        );

        return true;

    } catch (error) {

        console.error(
            "[Redis] saveDistrictDeviationState ERROR:",
            error
        );

        throw error;
    }
}

/*
|--------------------------------------------------------------------------
| Delete District State
|--------------------------------------------------------------------------
*/

static getDisconnectedStateKey(redisKey) {
    return `disconnected:${redisKey}`;
}

/*
|--------------------------------------------------------------------------
| Get
|--------------------------------------------------------------------------
*/

static async getDisconnectedState(redisKey) {

    try {

        if (!redisKey) {
            return null;
        }

        const key =
            this.getDisconnectedStateKey(redisKey);

        const data =
            await redisClient.get(key);

        if (!data) {
            return null;
        }

        try {

            return JSON.parse(data);

        } catch (error) {

            console.error(
                "[Redis] Invalid disconnected state:",
                error
            );

            await redisClient.del(key);

            return null;
        }

    } catch (error) {

        console.error(
            "[Redis] getDisconnectedState ERROR:",
            error
        );

        throw error;
    }
}

/*
|--------------------------------------------------------------------------
| Save
|--------------------------------------------------------------------------
*/

static async saveDisconnectedState(
    redisKey,
    state
) {

    try {

        if (!redisKey || !state) {
            return false;
        }

        const key =
            this.getDisconnectedStateKey(
                redisKey
            );

        const data = {
            alertSent:
                Boolean(state.alertSent)
        };

        await redisClient.set(
            key,
            JSON.stringify(data)
        );

        return true;

    } catch (error) {

        console.error(
            "[Redis] saveDisconnectedState ERROR:",
            error
        );

        throw error;
    }
}

/*
|--------------------------------------------------------------------------
| Delete
|--------------------------------------------------------------------------
*/

    static getNotificationQueueKey(
        type
    ) {
        return `notification:${String(type).trim()}`;
    }

    static getNotificationProcessingKey(
        type
    ) {
        return (
            `notification:${String(type).trim()}` +
            `:processing:${Date.now()}:${randomUUID()}`
        );
    }

    static async pushNotifications(
        type,
        notifications
    ) {
        try {
            if (
                !type ||
                !Array.isArray(notifications) ||
                notifications.length === 0
            ) {
                return 0;
            }

            const items =
                notifications.map(
                    notification =>
                        JSON.stringify(notification)
                );

            await redisClient.rpush(
                this.getNotificationQueueKey(type),
                ...items
            );

            return items.length;

        } catch (error) {
            console.error(
                `[Redis] PUSH NOTIFICATIONS ERROR | ${type}:`,
                error
            );

            throw error;
        }
    }

    static async getNotificationQueueLength(
        type
    ) {
        try {
            if (!type) {
                return 0;
            }

            return await redisClient.llen(
                this.getNotificationQueueKey(type)
            );

        } catch (error) {
            console.error(
                `[Redis] NOTIFICATION QUEUE LENGTH ERROR | ${type}:`,
                error
            );

            return 0;
        }
    }

    static async moveNotificationBatch(
        type,
        limit = 1000
    ) {
        let processingKey;
        try {

            if (!type) {
                throw new Error(
                    "Notification type is required"
                );
            }

            const queueKey =
                this.getNotificationQueueKey(type);

            processingKey =
                this.getNotificationProcessingKey(type);

            const batchLimit =
                Math.max(
                    1,
                    Math.min(
                        5000,
                        Math.floor(
                            Number(limit) || 1000
                        )
                    )
                );

            const items =
                await redisClient.eval(
                    `
                    local items = redis.call(
                        'LRANGE',
                        KEYS[1],
                        0,
                        tonumber(ARGV[1]) - 1
                    )

                    if #items == 0 then
                        return {}
                    end

                    redis.call(
                        'RPUSH',
                        KEYS[2],
                        unpack(items)
                    )

                    redis.call(
                        'LTRIM',
                        KEYS[1],
                        #items,
                        -1
                    )

                    return items
                    `,
                    2,
                    queueKey,
                    processingKey,
                    batchLimit
                );

            const notifications =
                (items || []).map(item => {

                    const row =
                        JSON.parse(item);

                    if (
                        !row ||
                        typeof row !== "object" ||
                        Array.isArray(row)
                    ) {
                        throw new Error(
                            "Invalid notification record"
                        );
                    }

                    if (
                        !Number.isInteger(
                            Number(row.user_id)
                        ) ||
                        Number(row.user_id) <= 0
                    ) {
                        throw new Error(
                            "Invalid notification recipient ID"
                        );
                    }

                    return row;
                });

            return {
                key:
                    processingKey,

                notifications
            };

        } catch (error) {
            error.processingKey = processingKey;
            console.error(
                `[Redis] MOVE NOTIFICATION BATCH ERROR | ${type}:`,
                error
            );

            throw error;
        }
    }

    static async deleteNotificationProcessingQueue(
        processingKey
    ) {
        try {
            if (!processingKey) {
                return true;
            }

            await redisClient.del(
                processingKey
            );

            return true;

        } catch (error) {
            console.error(
                "[Redis] DELETE NOTIFICATION PROCESSING QUEUE ERROR:",
                error
            );

            return false;
        }
    }

// =========================================================
    // VEHICLE CACHE
    // =========================================================

    static getVehicleCacheKey(
        vehicleNo
    ) {
        return `vehicle:${String(vehicleNo).trim()}`;
    }

    static async getVehicleCache(
        vehicleNo
    ) {
        try {
            if (!vehicleNo) {
                return null;
            }

            const key = this.getVehicleCacheKey(vehicleNo);
            const data = await redisClient.get(key);

            if (!data) {
                return null;
            }

            // Reject permanent entries left by older versions. A cache miss
            // makes VehicleRepository recheck the current database mapping.
            if (await redisClient.ttl(key) <= 0) {
                return null;
            }

            return JSON.parse(data);

        } catch (error) {
            console.error(
                "[Redis] VEHICLE CACHE GET ERROR:",
                error
            );

            return null;
        }
    }

    static async setVehicleCache(
        vehicleNo,
        vehicle
    ) {
        try {
            if (!vehicleNo || !vehicle) {
                return false;
            }

            await redisClient.set(
                this.getVehicleCacheKey(vehicleNo),
                JSON.stringify(vehicle),
                "EX",
                60
            );

            return true;

        } catch (error) {
            console.error(
                "[Redis] VEHICLE CACHE SET ERROR:",
                error
            );

            return false;
        }
    }

// =========================================================
    // COMMON VEHICLE ALERT QUEUE
    //
    // ONLY ONE ALERT QUEUE
    //
    // GeoFence
    // Long Halt
    // Offline
    // Overspeed
    // Off Route
    // Ignition
    // Searching Network
    // Idle
    // Disconnected
    //
    // All alerts -> vehicle_alert_queue
    // =========================================================

    static getVehicleAlertQueueKey() {
        return "vehicle_alert_queue";
    }

// =========================================================
    // ADD VEHICLE ALERT
    // =========================================================

    static async addVehicleAlert(
        alert
    ) {
        try {

            if (
                !alert ||
                typeof alert !== "object" ||
                Array.isArray(alert)
            ) {
                console.error(
                    "[Redis] INVALID VEHICLE ALERT:",
                    alert
                );

                return false;
            }

            if (
                !alert.vehicle_no ||
                !alert.alert_id
            ) {
                console.error(
                    "[Redis] INVALID VEHICLE ALERT | " +
                    "vehicle_no OR alert_id missing:",
                    alert
                );

                return false;
            }

            const alertId =
                Number(alert.alert_id);

            if (
                !Number.isFinite(alertId) ||
                alertId <= 0
            ) {
                console.error(
                    "[Redis] INVALID ALERT ID:",
                    alert.alert_id
                );

                return false;
            }

            alert.alert_id = alertId;

            if (
                alert.status === undefined ||
                alert.status === null
            ) {
                alert.status = 1;
            }

            // Preserve the comma-separated GPS location supplied by the service.

            const alertData =
                JSON.stringify(alert);

            const identity = vehicleAlertIdentity(alert);
            if (identity) {
                // Claim and enqueue atomically, including across GPS processes.
                // Duplicates count as success so service state advances normally.
                await redisClient.eval(`
                    if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
                    redis.call('RPUSH', KEYS[1], ARGV[1])
                    redis.call('SET', KEYS[2], '1', 'EX', 86400)
                    return 1
                `, 2, this.getVehicleAlertQueueKey(), `vehicle_alert:dedup:${identity}`, alertData);
            } else {
                await redisClient.rpush(this.getVehicleAlertQueueKey(), alertData);
            }

            return true;

        } catch (error) {
            console.error(
                "[Redis] ADD VEHICLE ALERT ERROR:",
                error
            );

            return false;
        }
    }

// =========================================================
    // GET VEHICLE ALERT QUEUE LENGTH
    // =========================================================

    static async getVehicleAlertQueueLength() {
        try {

            return await redisClient.llen(
                this.getVehicleAlertQueueKey()
            );

        } catch (error) {
            console.error(
                "[Redis] GET VEHICLE ALERT QUEUE LENGTH ERROR:",
                error
            );

            return 0;
        }
    }

// =========================================================
    // MOVE VEHICLE ALERTS TO PROCESSING
    //
    // FIFO
    // ATOMIC
    // =========================================================

    static async moveVehicleAlertsToProcessing(
        limit = 1000
    ) {
        try {

            limit = Math.max(
                1,
                Math.min(
                    5000,
                    Math.floor(
                        Number(limit) || 1000
                    )
                )
            );

            const sourceKey =
                this.getVehicleAlertQueueKey();

            const processingKey =
                `vehicle_alert:processing:` +
                `${Date.now()}:${randomUUID()}`;

            const items =
                await redisClient.eval(
                    `
                    local source = KEYS[1]
                    local target = KEYS[2]
                    local limit = tonumber(ARGV[1])

                    local items = redis.call(
                        'LRANGE',
                        source,
                        0,
                        limit - 1
                    )

                    if #items == 0 then
                        return {}
                    end

                    redis.call(
                        'RPUSH',
                        target,
                        unpack(items)
                    )

                    redis.call(
                        'LTRIM',
                        source,
                        #items,
                        -1
                    )

                    return items
                    `,
                    2,
                    sourceKey,
                    processingKey,
                    limit
                );

            if (
                !Array.isArray(items) ||
                items.length === 0
            ) {
                return {
                    key: null,
                    alerts: []
                };
            }

            const alerts = [];

            for (const item of items) {

                try {

                    const alert =
                        typeof item === "string"
                            ? JSON.parse(item)
                            : item;

                    if (
                        alert &&
                        typeof alert === "object" &&
                        !Array.isArray(alert)
                    ) {
                        alerts.push(alert);
                    }

                } catch (error) {

                    console.error(
                        "[Redis] VEHICLE ALERT JSON PARSE ERROR:",
                        error
                    );
                }
            }

            return {
                key: processingKey,
                alerts
            };

        } catch (error) {

            console.error(
                "[Redis] MOVE VEHICLE ALERTS ERROR:",
                error
            );

            throw error;
        }
    }

// =========================================================
    // GET PROCESSING VEHICLE ALERTS
    // =========================================================

    static async deleteVehicleAlertProcessingQueue(
        processingKey
    ) {
        try {

            if (!processingKey) {
                return true;
            }

            await redisClient.del(
                processingKey
            );

            return true;

        } catch (error) {

            console.error(
                "[Redis] DELETE VEHICLE ALERT PROCESSING ERROR:",
                error
            );

            return false;
        }
    }

    static async retryVehicleAlertBatch(processingKey) {
        if (!String(processingKey).startsWith('vehicle_alert:processing:')) {
            throw new Error('Invalid vehicle alert processing key');
        }
        // Only called after a confirmed rollback, never after an uncertain commit.
        return redisClient.eval(`
            local items = redis.call('LRANGE', KEYS[1], 0, -1)
            for i = #items, 1, -1 do
                redis.call('LPUSH', KEYS[2], items[i])
            end
            redis.call('DEL', KEYS[1])
            return #items
        `, 2, processingKey, this.getVehicleAlertQueueKey());
    }

// =========================================================
    // GEOFENCE DISTRIBUTED LOCK
    // =========================================================

static getActiveTripKey(vehicleNo, level = 2) {
        return `active_trip:v2:${level}:${vehicleNo}`;
    }

// =========================================================
    // PENDING TRIP
    // =========================================================

static async getActiveTrip(vehicleNo, level = 2) {
        try {
            const key = this.getActiveTripKey(
                vehicleNo,
                level
            );

            const data =
                await redisClient.get(key);

            if (!data) {
                return [];
            }

            // Older writers could leave an active trip cached indefinitely.
            // Treat those entries as a miss so the database refreshes them.
            if (await redisClient.ttl(key) <= 0) {
                return [];
            }

            const trips =
                JSON.parse(data);

            return Array.isArray(trips)
                ? trips
                : [trips];

        } catch (error) {
            console.error(
                "[RedisRepository] GET ACTIVE TRIP ERROR:",
                error
            );

            return [];
        }
    }

    static async saveActiveTrip(
        vehicleNo,
        level = 2,
        trips
    ) {
        try {
            const key = this.getActiveTripKey(
                vehicleNo,
                level
            );

            const tripArray =
                Array.isArray(trips)
                    ? trips
                    : [trips];

            if (tripArray.length === 0) {
                return false;
            }

            await redisClient.set(
                key,
                JSON.stringify(tripArray),
                "EX",
                60
            );

            return true;

        } catch (error) {
            console.error(
                "[RedisRepository] SAVE ACTIVE TRIP ERROR:",
                error
            );

            return false;
        }
    }

// =========================================================
    // REDIS HEALTH
    // =========================================================

    static async quit() {
        try {
            await redisClient.quit();
            return true;

        } catch (error) {
            console.error(
                "[Redis] QUIT ERROR:",
                error
            );

            return false;
        }
    }
}

// =========================================================
// REDIS EVENTS
// =========================================================

redisClient.on(
    "connect",
    () => {
        console.log(
            "✅ Redis Connected"
        );
    }
);

redisClient.on(
    "ready",
    () => {
        console.log(
            "✅ Redis Ready"
        );
    }
);

redisClient.on(
    "error",
    error => {
        console.error(
            "❌ Redis Error:",
            error.message
        );
    }
);

redisClient.on(
    "close",
    () => {
        console.log(
            "⚠️ Redis Connection Closed"
        );
    }
);

// =========================================================
// EXPORT
// =========================================================

module.exports = RedisRepository;
