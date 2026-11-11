"use strict";

const db = require("../model/dbquery");
const RedisRepository = require("./RedisRepository");

class VehicleRepository {

    static vehicleRequests = new Map();

    static async getVehicleByNumber(vehicleNo) {

        const normalizedVehicleNo =
            String(vehicleNo || "")
                .trim()
                .toUpperCase();

        if (!normalizedVehicleNo) {
            return null;
        }

const cachedVehicle =
            await RedisRepository.getVehicleCache(
                normalizedVehicleNo
            );

        if (
            cachedVehicle &&
            Number(cachedVehicle.level) === 2
        ) {

            return cachedVehicle;
        }

const existingRequest =
            this.vehicleRequests.get(
                normalizedVehicleNo
            );

        if (existingRequest) {

            return await existingRequest;
        }

const request =
            this.fetchVehicleFromDatabase(
                normalizedVehicleNo
            );

this.vehicleRequests.set(
            normalizedVehicleNo,
            request
        );

try {

            return await request;

        }
        finally {

this.vehicleRequests.delete(
                normalizedVehicleNo
            );

        }

    }

    static async fetchVehicleFromDatabase(
        vehicleNo
    ) {

        const result =
            await db.joinSelect(
                `
                SELECT
                    mp.id,
                    mp.assigned_by AS transporter_id,
                    mp.imei_no,
                    mp.vehicle_no,

                    vm.district_id,
                    vm.level,

                    dm.district_name

                FROM tbl_device_vehicle_mapping mp

                INNER JOIN tbl_vehicle_master vm
                    ON vm.vehicle_no = mp.vehicle_no
                    AND vm.status = 1

                LEFT JOIN tbl_district_master dm
                    ON dm.id = vm.district_id

                WHERE
                    mp.vehicle_no = ?
                    AND mp.device_status = 1
                    AND mp.status = 1
                    AND vm.level = 2

                LIMIT 1
                `,
                [vehicleNo]
            );

const vehicle =
            Array.isArray(result) &&
            result.length > 0
                ? result[0]
                : null;

if (vehicle) {

            await RedisRepository.setVehicleCache(
                vehicleNo,
                vehicle
            );

        }

return vehicle;
    }

}

module.exports = VehicleRepository;
