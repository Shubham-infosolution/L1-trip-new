const mqtt = require("mqtt");

const options = {
    reconnectPeriod: 5000,      // Retry every 5 seconds
    connectTimeout: 30 * 1000,  // 30 seconds
    keepalive: 60,
    clean: true,
};

const client = mqtt.connect(process.env.MQTT_URL, options);

module.exports = client;