const alertLocation = require("../utils/alertLocation");
"use strict";

const TripRepository = require("../repositories/TripRepository");
const RedisRepository = require("../repositories/RedisRepository");
const geoHelper = require("../helpers/geoHelper");

const fs = require("fs");
const path = require("path");
const topojson = require("topojson-client");

/*
|--------------------------------------------------------------------------
| Load Jharkhand Topology
|--------------------------------------------------------------------------
*/

const topology = JSON.parse(
    fs.readFileSync(
        path.join(__dirname, "../utils/jharkhand.json"),
        "utf8"
    )
);

const geojson = topojson.feature(
    topology,
    topology.objects.Jharkhand
);

const districtFeatures = geojson.features;


/*
|--------------------------------------------------------------------------
| District Deviation Service
|--------------------------------------------------------------------------
*/

class DistrictDeviationService {

    static ALERT_ID = 10;


    /*
    |--------------------------------------------------------------------------
    | Main Process
    |--------------------------------------------------------------------------
    */

    static async process(vehicle, payload) {

        try {

            if (!vehicle || !vehicle.vehicle_no) {
                return null;
            }

            /*
            |--------------------------------------------------------------------------
            | GPS Validation
            |--------------------------------------------------------------------------
            */

            if (!payload || !payload.gps_timestamp) {
                return null;
            }

            const currentLat = Number(payload.latitude);
            const currentLng = Number(payload.longitude);

            if (
                !Number.isFinite(currentLat) ||
                !Number.isFinite(currentLng)
            ) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Active Trip
            |--------------------------------------------------------------------------
            |
            | getActiveTrip() returns ARRAY.
            |
            */

            const activeTrips =
                await TripRepository.getActiveTrip(
                    vehicle.vehicle_no
                );

            if (
                !Array.isArray(activeTrips) ||
                activeTrips.length === 0
            ) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Select Running Trip
            |--------------------------------------------------------------------------
            */

            const activeTrip = activeTrips.find(
                trip =>
                    Number(trip.trip_status) === 1
            );

            if (!activeTrip) {
                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | Check Vehicle Inside Jharkhand
            |--------------------------------------------------------------------------
            */

            let isInsideJharkhand = false;
            let currentDistrict = null;

            for (const district of districtFeatures) {

                if (
                    !district.geometry ||
                    !district.geometry.coordinates
                ) {
                    continue;
                }

                const coordinates =
                    district.geometry.coordinates;

                /*
                |----------------------------------------------------------------------
                | Polygon / MultiPolygon support
                |----------------------------------------------------------------------
                */

                let polygons = [];

                if (
                    district.geometry.type === "Polygon"
                ) {
                    polygons = [coordinates];
                }

                else if (
                    district.geometry.type === "MultiPolygon"
                ) {
                    polygons = coordinates;
                }

                for (const polygonCoordinates of polygons) {

                    if (!polygonCoordinates?.[0]) {
                        continue;
                    }

                    const polygon =
                        polygonCoordinates[0];

                    const inside =
                        geoHelper.isPointInsidePolygon(
                            currentLat,
                            currentLng,
                            polygon
                        );

                    if (inside) {

                        isInsideJharkhand = true;

                        currentDistrict =
                            district.properties?.Dist_Name ||
                            null;

                        break;
                    }
                }

                if (isInsideJharkhand) {
                    break;
                }
            }


            /*
            |--------------------------------------------------------------------------
            | Previous District State
            |--------------------------------------------------------------------------
            */

            let districtState =
                await RedisRepository.getDistrictDeviationState(
                    vehicle.vehicle_no
                );


            /*
            |--------------------------------------------------------------------------
            | First Packet
            |--------------------------------------------------------------------------
            */

            if (!districtState) {

                await RedisRepository.saveDistrictDeviationState(
                    vehicle.vehicle_no,
                    {
                        status:
                            isInsideJharkhand
                                ? "IN"
                                : "OUT",

                        district: currentDistrict,

                        latitude: currentLat,
                        longitude: currentLng,

                        gps_timestamp:
                            payload.gps_timestamp
                    }
                );

                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | DISTRICT OUT
            |--------------------------------------------------------------------------
            */

            if (
                !isInsideJharkhand &&
                districtState.status === "IN"
            ) {

                await this.queueDistrictAlert(
                    vehicle,
                    activeTrip,
                    payload,
                    "OUT",
                    districtState.district,
                    null
                );


                await RedisRepository.saveDistrictDeviationState(
                    vehicle.vehicle_no,
                    {
                        status: "OUT",

                        district: null,

                        latitude: currentLat,
                        longitude: currentLng,

                        gps_timestamp:
                            payload.gps_timestamp
                    }
                );

                return "OUT";
            }


            /*
            |--------------------------------------------------------------------------
            | Already OUT
            |--------------------------------------------------------------------------
            */

            if (
                !isInsideJharkhand &&
                districtState.status === "OUT"
            ) {

                return null;
            }


            /*
            |--------------------------------------------------------------------------
            | DISTRICT IN
            |--------------------------------------------------------------------------
            */

            if (
                isInsideJharkhand &&
                districtState.status === "OUT"
            ) {

                await this.queueDistrictAlert(
                    vehicle,
                    activeTrip,
                    payload,
                    "IN",
                    null,
                    currentDistrict
                );


                await RedisRepository.saveDistrictDeviationState(
                    vehicle.vehicle_no,
                    {
                        status: "IN",

                        district: currentDistrict,

                        latitude: currentLat,
                        longitude: currentLng,

                        gps_timestamp:
                            payload.gps_timestamp
                    }
                );

                return "IN";
            }


            /*
            |--------------------------------------------------------------------------
            | Already IN
            |--------------------------------------------------------------------------
            */

            if (
                isInsideJharkhand &&
                districtState.status === "IN"
            ) {

                /*
                | District changed while vehicle remains
                | inside Jharkhand.
                |
                | State update only.
                */

                if (
                    currentDistrict &&
                    currentDistrict !== districtState.district
                ) {

                    await RedisRepository.saveDistrictDeviationState(
                        vehicle.vehicle_no,
                        {
                            status: "IN",

                            district: currentDistrict,

                            latitude: currentLat,
                            longitude: currentLng,

                            gps_timestamp:
                                payload.gps_timestamp
                        }
                    );
                }

                return null;
            }


            return null;

        } catch (error) {

            console.error(
                "[DistrictDeviation] ERROR:",
                error
            );

            return null;
        }
    }


    /*
    |--------------------------------------------------------------------------
    | Queue District Alert
    |--------------------------------------------------------------------------
    */

    static async queueDistrictAlert(
        vehicle,
        activeTrip,
        payload,
        status,
        previousDistrict,
        currentDistrict
    ) {

        try {

            const packetTime =
                geoHelper.formatServerTime
                    ? geoHelper.formatServerTime(
                        payload.gps_timestamp
                    )
                    : null;


            const message =
                status === "OUT"
                    ? `${vehicle.vehicle_no} vehicle moved outside Jharkhand`
                    : `${vehicle.vehicle_no} vehicle entered Jharkhand`;


            const alert = {

                trip_id:
                    activeTrip?.id ??
                    activeTrip?.trip_id ??
                    null,

                subtrip_id:
                    activeTrip?.sub_trip_id ??
                    activeTrip?.subtrip_id ??
                    activeTrip?.subtripId ??
                    null,

                vehicle_no:
                    vehicle.vehicle_no,

                alert_id:
                    this.ALERT_ID,

                message,

                
                last_location: alertLocation(payload),

                last_date_time:
                    packetTime,

                /*
                | District deviation does not use
                | long halt.
                */
                long_halt_minutes: null,

                speed:
                    payload.speed != null
                        ? Number(payload.speed)
                        : null,

                off_route_in_km: null,

                district_id:
                    vehicle.district_id ??
                    activeTrip?.district_id ??
                    null,

                status: 1,

                in_out_status:
                    status,

                geofence:
                    currentDistrict ||
                    previousDistrict ||
                    null
            };


            /*
            |--------------------------------------------------------------------------
            | Common Vehicle Alert Queue
            |--------------------------------------------------------------------------
            */

            await RedisRepository.addVehicleAlert(
                alert
            );


            // console.log(
            //     `[DistrictDeviation] ALERT QUEUED | ` +
            //     `Vehicle: ${vehicle.vehicle_no} | ` +
            //     `Trip: ${alert.trip_id} | ` +
            //     `Status: ${status} | ` +
            //     `District: ${currentDistrict || previousDistrict || "-"}`
            // );

            return true;

        } catch (error) {

            console.error(
                "[DistrictDeviation] QUEUE ALERT ERROR:",
                error
            );

            throw error;
        }
    }
}


module.exports = DistrictDeviationService;