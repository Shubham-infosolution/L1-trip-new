'use strict';
const TripRepository = require('../repositories/TripRepository');
const DisconnectedService = require('../services/disconnected.service');
const cron = require('../utils/managedCron');
class AlertScheduler {
  static start() {
    if (this.task) return this.task;
    this.task = cron.schedule('* * * * *', async () => {
      const vehicles = await TripRepository.getActiveTripVehicles();
      const seen = new Set();
      for (const vehicle of vehicles || []) {
        // A subtrip join can produce the same vehicle more than once.
        if (seen.has(vehicle.vehicle_no)) continue;
        seen.add(vehicle.vehicle_no);
        try { await DisconnectedService.process(vehicle); }
        catch (error) { console.error('[Disconnected]', vehicle.vehicle_no, error); }
      }
    });
    return this.task;
  }
}
module.exports = AlertScheduler;
