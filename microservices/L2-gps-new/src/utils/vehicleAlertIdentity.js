'use strict';
const { createHash } = require('node:crypto');

// A later occurrence is a different event; never deduplicate on alert type alone.
module.exports = function vehicleAlertIdentity(alert) {
    const occurrence = alert.event_key ?? alert.last_date_time;
    if (occurrence === undefined || occurrence === null || occurrence === '') return null;
    const identity = [
        String(alert.vehicle_no || '').trim().toUpperCase(),
        String(alert.trip_id ?? ''), String(alert.subtrip_id ?? ''),
        String(alert.alert_id ?? ''), String(alert.in_out_status ?? '').trim().toUpperCase(),
        String(alert.geofence ?? ''),
        occurrence instanceof Date ? occurrence.toISOString() : String(occurrence)
    ];
    return createHash('sha256').update(JSON.stringify(identity)).digest('hex');
};
