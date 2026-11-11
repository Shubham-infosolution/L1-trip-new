const cron =
    require("../utils/managedCron");

const RedisRepository =
    require("../repositories/RedisRepository");

const NotificationRepository =
    require("../repositories/NotificationRepository");

const geoHelper =
    require("../helpers/geoHelper");

const { integer } =
    require("../db/settings");


const NOTIFICATION_BATCH_SIZE =
    integer(
        "NOTIFICATION_BATCH_SIZE",
        1000,
        1,
        1000
    );


const NOTIFICATION_CRON =
    process.env.NOTIFICATION_CRON ||
    "*/30 * * * * *";


class NotificationService {

    static async process({
        vehicle,
        payload,
        tripResult,
        distance
    }) {

        try {

            if (
                !vehicle ||
                !payload
            ) {
                return true;
            }


            if (!tripResult?.event) {
                return true;
            }


            switch (
                tripResult.event
            ) {

                case "TRIP_STARTED":

                    await this.sendTripStartedNotification(
                        vehicle,
                        tripResult.trip,
                        payload
                    );

                    return true;

                case "SUBTRIP_COMPLETED":

                    await this.sendSubTripCompletedNotification(
                        vehicle,
                        tripResult.trip,
                        tripResult.subTrip,
                        payload,
                        distance
                    );

                    return true;


                case "TRIP_COMPLETED":

                    await this.sendTripCompletedNotification(
                        vehicle,
                        tripResult.trip,
                        payload,
                        distance
                    );

                    return true;


                case "TRIP_RUNNING":

                    return true;


                default:

                    return true;
            }


        } catch (error) {

            console.error(
                "NOTIFICATION PROCESS ERROR:",
                error
            );

            return false;
        }

    }

    static async sendTripStartedNotification(
        vehicle,
        trip,
        payload
    ) {

        try {

            if (!trip) {
                return true;
            }


            const packetReceiveTime = await geoHelper.formatServerTime(
                    payload.gps_timestamp
                );


            // -------------------------------------------------
            // Source store required
            // -------------------------------------------------

            if (
                trip.source_store_id === undefined ||
                trip.source_store_id === null
            ) {

                console.warn(
                    "[Notification] " +
                    "TRIP_STARTED_MISSING_SOURCE_STORE",
                    {
                        vehicle_no:
                            vehicle.vehicle_no,

                        trip_id:
                            trip.trip_id
                    }
                );

                return true;
            }


            // -------------------------------------------------
            // Notification users
            // -------------------------------------------------

            const userTypes =
                "2,3,4";


            const users =
                await NotificationRepository
                    .getStoreUsers(
                        vehicle.district_id,
                        trip.source_store_id,
                        userTypes,
                        trip.driver_id ||
                        vehicle.driver_id ||
                        null,
                        vehicle.transporter_id ||
                        null
                    );


            if (!users?.length) {

                console.warn(
                    "[Notification] " +
                    "TRIP_STARTED_NO_RECIPIENTS",
                    {
                        vehicle_no:
                            vehicle.vehicle_no,

                        trip_id:
                            trip.trip_id,

                        source_store_id:
                            trip.source_store_id
                    }
                );

                return true;
            }


            // -------------------------------------------------
            // Notification content
            // -------------------------------------------------

            const title =
                "Trip Started";


            const message =
                `${title}\n\n` +
                `Trip No : ${
                    trip.trip_id || "N/A"
                }\n` +
                `Vehicle : ${
                    vehicle.vehicle_no || "N/A"
                }\n` +
                `Source : ${
                    trip.source_store_name || "N/A"
                }\n` +
                `Destination : ${
                    trip.destination_store_name || "N/A"
                }\n` +
                `Time : ${
                    packetReceiveTime || "N/A"
                }`;


            // -------------------------------------------------
            // Queue
            // -------------------------------------------------

            await this.queueNotifications({

                type:
                    "trip_started",

                users,

                vehicle,

                title,

                message,

                notificationType:
                    3,

                referenceId:
                    trip.id ||
                    null,

                link:
                    "/tripReports"
            });


            console.log(
                "[Notification] " +
                "TRIP_STARTED DELIVERED",
                {
                    vehicle_no:
                        vehicle.vehicle_no,

                    trip_id:
                        trip.trip_id,

                    users:
                        users.length
                }
            );


            return true;


        } catch (error) {

            console.error(
                "TRIP STARTED NOTIFICATION ERROR:",
                error
            );

            return false;
        }

    }


    // =========================================================
    // SUB TRIP COMPLETED NOTIFICATION
    //
    // notification_type = 6
    //
    // Trigger:
    // Vehicle enters subtrip destination.
    //
    // Multiple subtrips with same destination are handled
    // by TripService. NotificationService receives the
    // completed subTrip here.
    // =========================================================

    static async sendSubTripCompletedNotification(
        vehicle,
        trip,
        subTrip,
        payload,
        distance
    ) {

        try {

            if (
                Number(vehicle?.level) !== 2
            ) {
                return true;
            }


            if (
                !trip ||
                !subTrip
            ) {
                return true;
            }


            // -------------------------------------------------
            // Packet time
            // -------------------------------------------------

            const packetReceiveTime = await geoHelper.formatServerTime(
                    payload.gps_timestamp
                );


            // -------------------------------------------------
            // Destination store
            // -------------------------------------------------

            const destinationStoreId =
                subTrip.destination_store_id;


            if (
                destinationStoreId === undefined ||
                destinationStoreId === null
            ) {

                console.warn(
                    "[Notification] " +
                    "SUBTRIP_COMPLETED_MISSING_DESTINATION",
                    {
                        vehicle_no:
                            vehicle.vehicle_no,

                        trip_id:
                            trip.trip_id,

                        subtrip_id:
                            subTrip.id
                    }
                );

                return true;
            }


            // -------------------------------------------------
            // Notification users
            // -------------------------------------------------

            const userTypes =
                "4,7";


            const users =
                await NotificationRepository
                    .getStoreUsers(
                        vehicle.district_id,
                        destinationStoreId,
                        userTypes,
                        trip.driver_id ||
                        vehicle.driver_id ||
                        null,
                        vehicle.transporter_id ||
                        null
                    );


            if (!users?.length) {

                console.warn(
                    "[Notification] " +
                    "SUBTRIP_COMPLETED_NO_RECIPIENTS",
                    {
                        vehicle_no:
                            vehicle.vehicle_no,

                        trip_id:
                            trip.trip_id,

                        subtrip_id:
                            subTrip.id,

                        destination_store_id:
                            destinationStoreId
                    }
                );

                return true;
            }


            // -------------------------------------------------
            // Notification content
            // -------------------------------------------------

            const title =
                "Sub Trip Completed";


            const message =
                `${title}\n\n` +
                `Vehicle : ${
                    vehicle.vehicle_no || "N/A"
                }\n` +
                `Trip No : ${
                    trip.trip_id || "N/A"
                }\n` +
                `Sub Trip : ${
                    subTrip.sequence_no || "N/A"
                }\n` +
                `Source : ${
                    subTrip.source_store_name || "N/A"
                }\n` +
                `Destination : ${
                    subTrip.destination_store_name || "N/A"
                }\n` +
                `Distance : ${Number(
                    distance || 0
                ).toFixed(3)} KM\n` +
                `Time : ${
                    packetReceiveTime || "N/A"
                }`;


            // -------------------------------------------------
            // Queue
            // -------------------------------------------------

            await this.queueNotifications({

                type:
                    "subtrip_completed",

                users,

                vehicle,

                title,

                message,

                notificationType:
                    6,

                referenceId:
                    subTrip.id ||
                    trip.id ||
                    vehicle.id ||
                    null,

                link:
                    "/tripReports"
            });


            console.log(
                "[Notification] " +
                "SUBTRIP_COMPLETED DELIVERED",
                {
                    vehicle_no:
                        vehicle.vehicle_no,

                    trip_id:
                        trip.trip_id,

                    subtrip_id:
                        subTrip.id,

                    destination_store_id:
                        destinationStoreId,

                    users:
                        users.length
                }
            );


            return true;


        } catch (error) {

            console.error(
                "SUBTRIP COMPLETED NOTIFICATION ERROR:",
                error
            );

            return false;
        }

    }


    // =========================================================
    // TRIP COMPLETED NOTIFICATION
    //
    // notification_type = 4
    //
    // IMPORTANT:
    // Main L2 trip completion is NOT based on
    // destination geofence.
    //
    // TripService completes main trip when:
    //
    // remaining subTrips = 0
    //
    // Then this method is called with:
    //
    // tripResult.event = TRIP_COMPLETED
    //
    // =========================================================

    static async sendTripCompletedNotification(
        vehicle,
        trip,
        payload,
        distance
    ) {

        try {

            if (!trip) {
                return true;
            }


            // -------------------------------------------------
            // Packet time
            // -------------------------------------------------

            const packetReceiveTime =await geoHelper.formatServerTime(
                    payload.gps_timestamp
                );


            // -------------------------------------------------
            // Main trip destination
            //
            // This is ONLY used as notification recipient
            // if configured.
            //
            // It is NOT used for trip completion logic.
            // -------------------------------------------------

            if (
                trip.destination_store_id === undefined ||
                trip.destination_store_id === null
            ) {

                console.warn(
                    "[Notification] " +
                    "TRIP_COMPLETED_MISSING_DESTINATION",
                    {
                        vehicle_no:
                            vehicle.vehicle_no,

                        trip_id:
                            trip.trip_id
                    }
                );

                return true;
            }


            // -------------------------------------------------
            // Notification users
            // -------------------------------------------------

            const userTypes =
                "2,3,4";


            const users =
                await NotificationRepository
                    .getStoreUsers(
                        vehicle.district_id,
                        trip.destination_store_id,
                        userTypes,
                        trip.driver_id ||
                        vehicle.driver_id ||
                        null,
                        vehicle.transporter_id ||
                        null
                    );


            if (!users?.length) {

                console.warn(
                    "[Notification] " +
                    "TRIP_COMPLETED_NO_RECIPIENTS",
                    {
                        vehicle_no:
                            vehicle.vehicle_no,

                        trip_id:
                            trip.trip_id,

                        destination_store_id:
                            trip.destination_store_id
                    }
                );

                return true;
            }


            // -------------------------------------------------
            // Notification content
            // -------------------------------------------------

            const title =
                "Trip Completed";


            const message =
                `${title}\n\n` +
                `Trip No : ${
                    trip.trip_id || "N/A"
                }\n` +
                `Vehicle : ${
                    vehicle.vehicle_no || "N/A"
                }\n` +
                `Source : ${
                    trip.source_store_name || "N/A"
                }\n` +
                `Destination : ${
                    trip.destination_store_name || "N/A"
                }\n` +
                `Distance : ${Number(
                    distance || 0
                ).toFixed(3)} KM\n` +
                `Time : ${
                    packetReceiveTime || "N/A"
                }`;


            // -------------------------------------------------
            // Queue
            // -------------------------------------------------

            await this.queueNotifications({

                type:
                    "trip_completed",

                users,

                vehicle,

                title,

                message,

                notificationType:
                    4,

                referenceId:
                    trip.id ||
                    null,

                link:
                    "/tripReports"
            });


            console.log(
                "[Notification] " +
                "TRIP_COMPLETED DELIVERED",
                {
                    vehicle_no:
                        vehicle.vehicle_no,

                    trip_id:
                        trip.trip_id,

                    users:
                        users.length
                }
            );


            return true;


        } catch (error) {

            console.error(
                "TRIP COMPLETED NOTIFICATION ERROR:",
                error
            );

            return false;
        }

    }


    // =========================================================
    // SAVE AND PUBLISH NOTIFICATIONS IMMEDIATELY
    // =========================================================

    static async queueNotifications({
        type,
        users,
        vehicle,
        title,
        message,
        notificationType,
        eventKey = null,
        referenceId = null,
        link = null
    }) {

        try {

            if (
                !users?.length
            ) {
                return 0;
            }


            const notifications =
                users.map(
                    user => ({

                        user_id:
                            Number(
                                user.user_id
                            ),

                        level:
                            vehicle.level != null
                                ? Number(
                                    vehicle.level
                                )
                                : null,

                        event_key: eventKey,

                        title,

                        message,

                        reference_id:
                            referenceId != null
                                ? Number(
                                    referenceId
                                )
                                : null,

                        notification_type:
                            Number(
                                notificationType
                            ),

                        link,

                        c_by:
                            null,

                        district_id:
                            vehicle.district_id != null
                                ? Number(
                                    vehicle.district_id
                                )
                                : null,

                        vehicle_no:
                            vehicle.vehicle_no ||
                            null
                    })
                );


            const result = await NotificationRepository.bulkInsertNotifications(notifications);
            return result.notifications.length;


        } catch (error) {

            console.error(
                `[Notification] QUEUE ERROR | ${type}`,
                error
            );

            throw error;
        }

    }


    // =========================================================
    // PROCESS ONE NOTIFICATION QUEUE
    // =========================================================

    static async processNotificationBatch(
        type,
        limit = NOTIFICATION_BATCH_SIZE
    ) {

        let processingKey = null;


        try {

            const queueLength =
                await RedisRepository
                    .getNotificationQueueLength(
                        type
                    );


            if (!queueLength) {
                return;
            }


            const batch =
                await RedisRepository
                    .moveNotificationBatch(
                        type,
                        limit
                    );


            if (
                !batch ||
                !Array.isArray(
                    batch.notifications
                ) ||
                !batch.notifications.length
            ) {
                return;
            }


            processingKey =
                batch.key;


            // -------------------------------------------------
            // BULK DB INSERT
            // -------------------------------------------------

            const result =
                await NotificationRepository
                    .bulkInsertNotifications(
                        batch.notifications
                    );


            if (!Array.isArray(result?.notifications)) {
                throw new Error('Notification delivery did not return saved payloads');
            }

            // -------------------------------------------------
            // DELETE ONLY AFTER DB SUCCESS
            // -------------------------------------------------

            await RedisRepository
                .deleteNotificationProcessingQueue(
                    processingKey
                );


            processingKey = null;


        } catch (error) {

            console.error(
                `[Notification] BATCH ERROR | ` +
                `${type} | ` +
                `processing=${
                    processingKey ||
                    error.processingKey ||
                    "unknown"
                }`,
                error
            );

            /*
             * Processing queue intentionally delete nahi hoga.
             *
             * DB failure hone par notifications
             * processing Redis key mein safe rahengi.
             */
        }

    }


    // =========================================================
    // PROCESS ALL NOTIFICATION QUEUES
    // =========================================================

    static async processNotificationQueues() {

        try {

            await Promise.all([

                this.processNotificationBatch(
                    "trip_started",
                    500
                ),

                this.processNotificationBatch(
                    "trip_completed",
                    500
                ),

                this.processNotificationBatch(
                    "subtrip_completed",
                    500
                )

            ]);


        } catch (error) {

            console.error(
                "NOTIFICATION QUEUES PROCESS ERROR:",
                error
            );

        }

    }


    // =========================================================
    // START NOTIFICATION CRON
    // =========================================================

    static startNotificationQueueProcessor() {

        if (
            this.notificationTask
        ) {
            return this.notificationTask;
        }


        this.notificationTask =
            cron.schedule(
                NOTIFICATION_CRON,
                async () => {

                    await this
                        .processNotificationQueues();

                }
            );


        return this.notificationTask;
    }

}


// =========================================================
// START CRON
// =========================================================

NotificationService
    .startNotificationQueueProcessor();


// =========================================================
// EXPORT
// =========================================================

module.exports =
    NotificationService;