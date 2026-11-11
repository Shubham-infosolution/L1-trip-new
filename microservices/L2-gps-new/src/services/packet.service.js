const RedisRepository = require("../repositories/RedisRepository");
const vehicleRepository = require("../repositories/VehicleRepository");

// Services
const longHaltService = require("./longHaltService");
const overSpeedService = require("./overspeed.service");
const searchingNetworkService = require("./searching-network.service");
const offlineService = require("./offline.service");
const offRouteService = require("./off-route.service");
const districtDeviationService = require("./district-deviation.service");
const IdleDurationService = require("./idealDuration.service");
const IgnitionOffService = require("./ignitionOff.service");

const { getVehicleQueue } = require("../utils/vehicleQueue");

const processPacket = async (topic, payload) => {

    if (
        !payload ||
        !payload.vehicle_number ||
        !payload.gps_timestamp
    ) {
        return;
    }

    /*
    |--------------------------------------------------------------------------
    | Normalize Vehicle Number
    |--------------------------------------------------------------------------
    */

    const vehicleNo =
        String(payload.vehicle_number)
            .trim()
            .toUpperCase()
            .replace(/[^A-Z0-9]/g, "");

    if (!vehicleNo) {
        return;
    }

    const queue =
        getVehicleQueue(vehicleNo);

    return queue.add(async () => {

        try {
            
            const vehicle =
                await vehicleRepository.getVehicleByNumber(
                    vehicleNo
                );
          
            if (!vehicle) {
                return;
            }

            const latestPacket =
                await RedisRepository.isLatestPacket(
                    vehicleNo,
                    payload.gps_timestamp
                );
            if (!latestPacket) {

                // console.log(
                //     `${vehicleNo} Old Packet Ignored`
                // );

                return;
            }

            

            // if (!latestPacket) {
            //     console.log(
            //         `[OLD PACKET IGNORED] ${vehicleNo} | GPS: ${payload.gps_timestamp}`
            //     );
            //     return;
            // }

            /*
            |--------------------------------------------------------------------------
            | Services
            |--------------------------------------------------------------------------
            */

            await longHaltService.process(
                vehicle,
                payload
            );

            await overSpeedService.process(
                vehicle,
                payload
            );

            await searchingNetworkService.process(
                vehicle,
                payload
            );

            await offlineService.process(
                vehicle,
                payload
            );

            await offRouteService.process(
                vehicle,
                payload
            );

            await districtDeviationService.process(
                vehicle,
                payload
            );

            await IdleDurationService.process(
                vehicle,
                payload
            );

            await IgnitionOffService.process(
                vehicle,
                payload
            );

            /*
            |--------------------------------------------------------------------------
            | Save Latest Packet
            |--------------------------------------------------------------------------
            */

            await RedisRepository.saveLatestPacket(
                vehicleNo,
                payload
            );

        } catch (error) {

            console.error(
                `${vehicleNo} Packet Processing Error:`,
                error
            );

        }

    });
};

module.exports = {
    processPacket
};
