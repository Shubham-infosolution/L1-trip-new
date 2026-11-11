const client = require("../config/mqtt");
const packetService = require("../services/packet.service");

const activeMessages = new Set();
let stopping = false;
let started = false;

const TOPIC = "gps/+/data";

const startSubscriber = () => {
    if (started) return;
    started = true;

    client.on("connect", () => {

        console.log("====================================");
        console.log("✅ MQTT Broker Connected");
        console.log("====================================");

        client.subscribe(TOPIC, (err) => {

            if (err) {
                console.error(
                    "❌ Subscribe Error:",
                    err.message
                );
                return;
            }

            console.log(
                `✅ Successfully Subscribed : ${TOPIC}`
            );
        });

    });

    client.on("message", async (topic, message) => {
        if (stopping) return;

        try {

            const payload =
                JSON.parse(message.toString());

            const vehicleNo =
                payload.vehicle_number;

            // if (vehicleNo !== "JH03S6200") {
            //     return;
            // }

            // console.log(
            //     "🎯 TARGET VEHICLE PACKET:",
            //     payload
            // );

            const work = packetService.processPacket(topic, payload);
            activeMessages.add(work);
            try { await work; } finally { activeMessages.delete(work); }

        } catch (error) {

            console.error(
                "MQTT MESSAGE ERROR:",
                error
            );

        }

    });

    client.on("reconnect", () => {
        console.log("🔄 Reconnecting MQTT...");
    });

    client.on("offline", () => {
        console.log("⚠️ MQTT Client Offline");
    });

    client.on("close", () => {
        console.log("❌ MQTT Connection Closed");
    });

    client.on("error", (error) => {
        console.error(
            "❌ MQTT Error:",
            error.message
        );
    });

};

module.exports = startSubscriber;
startSubscriber.stop = async () => {
    stopping = true;
    await new Promise(resolve => client.end(false, {}, resolve));
    await Promise.allSettled([...activeMessages]);
};
