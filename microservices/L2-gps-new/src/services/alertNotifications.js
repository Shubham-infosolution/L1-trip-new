'use strict';
const alertIdentity = require('../utils/vehicleAlertIdentity');

module.exports = async function notifyAlerts(alerts) {
  const eligible = alerts.filter(row => Number.isSafeInteger(Number(row.district_id)) && Number(row.district_id) > 0);
  if (!eligible.length) return;
  const repository = require('../repositories/NotificationRepository');
  const service = require('./notification.service');
  // One lookup per alert batch, not one query per recipient.
  const names = await repository.getAlertNames(eligible.map(alert => alert.alert_id));
  const namesById = new Map(names.map(row => [Number(row.id), row.alert_name]));
  const usersByDistrict = new Map();
  for (const alert of eligible) {
    const title = namesById.get(Number(alert.alert_id));
    if (!title) throw new Error(`Missing alert_name for alert ${alert.alert_id}`);
    const districtId = Number(alert.district_id);
    if (!usersByDistrict.has(districtId)) {
      // State users plus district users; no arbitrary store recipient assignment.
      usersByDistrict.set(districtId, await repository.getStoreUsers(districtId, null));
    }
    await service.queueNotifications({
      eventKey: alertIdentity(alert),
      type: 'vehicle-alert', users: usersByDistrict.get(districtId), vehicle: alert,
      title, message: alert.message || `Vehicle ${alert.vehicle_no}: ${title}`,
      notificationType: 9, referenceId: alert.trip_id ?? null, link: '/alertReport'
    });
  }
};
