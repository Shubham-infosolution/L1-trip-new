const alertLocation = require('./alertLocation');

module.exports = function filterAlertLocations(alerts) {
    return alerts.flatMap(alert => {
        if (typeof alert?.last_location !== 'string') return [];
        const coordinates = alert.last_location.split(',');
        if (coordinates.length !== 2) return [];
        const location = alertLocation({ latitude: coordinates[0], longitude: coordinates[1] });
        return location === null ? [] : [{ ...alert, last_location: location }];
    });
};
