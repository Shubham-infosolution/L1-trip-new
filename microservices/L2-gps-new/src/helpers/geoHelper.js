const axios = require("axios");
class geoHelper {

    static getDistanceInKM(
        lat1,
        lon1,
        lat2,
        lon2
    ) {

        const R = 6371;

        const dLat =
            ((lat2 - lat1) * Math.PI) / 180;

        const dLon =
            ((lon2 - lon1) * Math.PI) / 180;

        const a =
            Math.sin(dLat / 2) ** 2 +
            Math.cos(lat1 * Math.PI / 180) *
            Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLon / 2) ** 2;

        const c =
            2 *
            Math.atan2(
                Math.sqrt(a),
                Math.sqrt(1 - a)
            );

        return R * c;
    }

    static formatServerTime(timestamp) {

        if (!timestamp) return null;

        const correctedTimestamp =
            Number(timestamp) -
            (5.5 * 60 * 60 * 1000);

        const formatter =
            new Intl.DateTimeFormat(
                "en-CA",
                {
                    timeZone: "Asia/Kolkata",
                    year: "numeric",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                    hour12: false
                }
            );

        const parts =
            formatter.formatToParts(
                new Date(correctedTimestamp)
            );

        const get = type =>
            parts.find(
                p => p.type === type
            )?.value;

        const period = get("dayPeriod")
                ?.replace(/\./g, "")
                .toUpperCase();

        return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
        // return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")} ${period}`;
    }

    static async reverseGeocode(
        lat,
        lng,
        retries = 3
    ) {

        try {

            if (!lat || !lng) {
                return "Invalid Location";
            }

            const apiKey =
                process.env.GOOGLE_API_KEY;

            const url =
                `https://maps.googleapis.com/maps/api/geocode/json?latlng=${lat},${lng}&key=${apiKey}`;

            const res =
                await axios.get(url);

            if (
                res.data.status === "OK"
            ) {
                return (
                    res.data.results[0]
                        ?.formatted_address ||
                    "Unknown"
                );
            }

            return "Unknown";

        } catch (err) {

            if (retries > 0) {
                return this.reverseGeocode(
                    lat,
                    lng,
                    retries - 1
                );
            }

            return "Unknown";
        }
    }

    /*
    |--------------------------------------------------------------------------
    | Degree To Radian
    |--------------------------------------------------------------------------
    */

    static toRad(value) {

        return value * Math.PI / 180;

    }

    /*
    |--------------------------------------------------------------------------
    | Decode Google Polyline
    |--------------------------------------------------------------------------
    */

    static decodePolyline(encoded) {

        if (!encoded) {

            return [];

        }

        let index = 0;

        let lat = 0;

        let lng = 0;

        const coordinates = [];

        while (index < encoded.length) {

            let b;

            let shift = 0;

            let result = 0;

            do {

                b = encoded.charCodeAt(index++) - 63;

                result |= (b & 0x1f) << shift;

                shift += 5;

            }

            while (b >= 0x20);

            const dLat =
                (result & 1)
                    ? ~(result >> 1)
                    : (result >> 1);

            lat += dLat;

            shift = 0;

            result = 0;

            do {

                b = encoded.charCodeAt(index++) - 63;

                result |= (b & 0x1f) << shift;

                shift += 5;

            }

            while (b >= 0x20);

            const dLng =
                (result & 1)
                    ? ~(result >> 1)
                    : (result >> 1);

            lng += dLng;

            coordinates.push({

                lat: lat / 100000,

                lng: lng / 100000

            });

        }

        return coordinates;

    }

    /*
    |--------------------------------------------------------------------------
    | Project Lat/Lng To KM
    |--------------------------------------------------------------------------
    */

    static projectToKm(lat, lng) {

        const R = 6371;

        const x =
            this.toRad(lng) *
            R *
            Math.cos(this.toRad(lat));

        const y =
            this.toRad(lat) *
            R;

        return {

            x,

            y

        };

    }

    /*
    |--------------------------------------------------------------------------
    | Distance Point To Line Segment (KM)
    |--------------------------------------------------------------------------
    */

    static distancePointToSegment(

        point,

        start,

        end

    ) {

        const A =
            this.projectToKm(
                start.lat,
                start.lng
            );

        const B =
            this.projectToKm(
                end.lat,
                end.lng
            );

        const P =
            this.projectToKm(
                point.lat,
                point.lng
            );

        const ABx =
            B.x - A.x;

        const ABy =
            B.y - A.y;

        const APx =
            P.x - A.x;

        const APy =
            P.y - A.y;

        const ab2 =
            ABx * ABx +
            ABy * ABy;

        if (ab2 === 0) {

            return Math.sqrt(

                APx * APx +

                APy * APy

            );

        }

        let t =

            (APx * ABx +

                APy * ABy)

            / ab2;

        t =

            Math.max(

                0,

                Math.min(

                    1,

                    t

                )

            );

        const nearest = {

            x:
                A.x + ABx * t,

            y:
                A.y + ABy * t

        };

        const dx =
            P.x - nearest.x;

        const dy =
            P.y - nearest.y;

        return Math.sqrt(

            dx * dx +

            dy * dy

        );

    }

    /*
    |--------------------------------------------------------------------------
    | Nearest Route Deviation (KM)
    |--------------------------------------------------------------------------
    */

    static getNearestRouteDeviation(

        currentLat,

        currentLng,

        routePoints

    ) {

        if (

            !routePoints ||

            routePoints.length < 2

        ) {

            return Number.MAX_VALUE;

        }

        const point = {

            lat: currentLat,

            lng: currentLng

        };

        let minDistance =
            Number.MAX_VALUE;

        for (

            let i = 0;

            i < routePoints.length - 1;

            i++

        ) {

            const distance =

                this.distancePointToSegment(

                    point,

                    routePoints[i],

                    routePoints[i + 1]

                );

            if (

                distance < minDistance

            ) {

                minDistance = distance;

            }

        }

        return minDistance;

    }

    /*
    |--------------------------------------------------------------------------
    | Point Inside Polygon
    |--------------------------------------------------------------------------
    */

    static isPointInsidePolygon(

        lat,

        lng,

        polygon

    ) {

        let inside = false;

        for (

            let i = 0,
                j = polygon.length - 1;

            i < polygon.length;

            j = i++

        ) {

            const xi = polygon[i][0]; // lng
            const yi = polygon[i][1]; // lat

            const xj = polygon[j][0]; // lng
            const yj = polygon[j][1]; // lat

            const intersect =

                (

                    (yi > lat) !== (yj > lat)

                ) &&

                (

                    lng <

                    ((xj - xi) *

                        (lat - yi)) /

                        (yj - yi) +

                        xi

                );

            if (intersect) {

                inside = !inside;

            }

        }

        return inside;

    }

    /*
    |--------------------------------------------------------------------------
    | Calculate Total Route Distance
    | KM
    |--------------------------------------------------------------------------
    */

    static calculateRouteDistance(routePoints) {

        if (
            !Array.isArray(routePoints) ||
            routePoints.length < 2
        ) {
            return 0;
        }

        let totalDistance = 0;

        for (
            let i = 1;
            i < routePoints.length;
            i++
        ) {

            totalDistance +=
                this.getDistanceInKM(

                    routePoints[i - 1].lat,
                    routePoints[i - 1].lng,

                    routePoints[i].lat,
                    routePoints[i].lng

                );
        }

        return Number(
            totalDistance.toFixed(2)
        );
    }


    /*
    |--------------------------------------------------------------------------
    | Get Current Position On Route
    |--------------------------------------------------------------------------
    |
    | Returns distance from route start in KM
    |--------------------------------------------------------------------------
    */

    static getRoutePosition(
        currentLat,
        currentLng,
        routePoints
    ) {

        if (
            !Array.isArray(routePoints) ||
            routePoints.length < 2
        ) {
            return 0;
        }

        const point = {
            lat: Number(currentLat),
            lng: Number(currentLng)
        };

        let cumulativeDistance = 0;

        let nearestDistance =
            Number.MAX_VALUE;

        let nearestRoutePosition = 0;


        for (
            let i = 0;
            i < routePoints.length - 1;
            i++
        ) {

            const start =
                routePoints[i];

            const end =
                routePoints[i + 1];


            /*
            |--------------------------------------------------------------------------
            | Segment Distance
            |--------------------------------------------------------------------------
            */

            const segmentDistance =
                this.getDistanceInKM(

                    start.lat,
                    start.lng,

                    end.lat,
                    end.lng

                );


            /*
            |--------------------------------------------------------------------------
            | Project point to KM
            |--------------------------------------------------------------------------
            */

            const A =
                this.projectToKm(
                    start.lat,
                    start.lng
                );

            const B =
                this.projectToKm(
                    end.lat,
                    end.lng
                );

            const P =
                this.projectToKm(
                    point.lat,
                    point.lng
                );


            const ABx =
                B.x - A.x;

            const ABy =
                B.y - A.y;

            const APx =
                P.x - A.x;

            const APy =
                P.y - A.y;


            const ab2 =
                ABx * ABx +
                ABy * ABy;


            let t = 0;


            if (ab2 > 0) {

                t =
                    (
                        APx * ABx +
                        APy * ABy
                    ) / ab2;

                t =
                    Math.max(
                        0,
                        Math.min(
                            1,
                            t
                        )
                    );
            }


            /*
            |--------------------------------------------------------------------------
            | Nearest Point On Segment
            |--------------------------------------------------------------------------
            */

            const nearestX =
                A.x + ABx * t;

            const nearestY =
                A.y + ABy * t;


            const dx =
                P.x - nearestX;

            const dy =
                P.y - nearestY;


            const distanceFromRoute =
                Math.sqrt(
                    dx * dx +
                    dy * dy
                );


            /*
            |--------------------------------------------------------------------------
            | Find Nearest Segment
            |--------------------------------------------------------------------------
            */

            if (
                distanceFromRoute <
                nearestDistance
            ) {

                nearestDistance =
                    distanceFromRoute;


                /*
                |--------------------------------------------------------------------------
                | Position on route
                |--------------------------------------------------------------------------
                */

                nearestRoutePosition =
                    cumulativeDistance +
                    (
                        segmentDistance * t
                    );
            }


            cumulativeDistance +=
                segmentDistance;
        }


        return Number(
            nearestRoutePosition.toFixed(2)
        );
    }

    static splitRouteAtVehiclePosition(
        currentLat,
        currentLng,
        routePoints
    ) {

        if (
            !Array.isArray(routePoints) ||
            routePoints.length < 2
        ) {
            return {
                travelledPoints: [],
                remainingPoints: []
            };
        }

        const point = {
            lat: Number(currentLat),
            lng: Number(currentLng)
        };

        let nearestDistance =
            Number.MAX_VALUE;

        let nearestIndex = 0;

        let nearestPoint = null;


        /*
        |--------------------------------------------------------------------------
        | Find Nearest Point On Route
        |--------------------------------------------------------------------------
        */

        for (
            let i = 0;
            i < routePoints.length - 1;
            i++
        ) {

            const start =
                routePoints[i];

            const end =
                routePoints[i + 1];


            const A =
                this.projectToKm(
                    start.lat,
                    start.lng
                );

            const B =
                this.projectToKm(
                    end.lat,
                    end.lng
                );

            const P =
                this.projectToKm(
                    point.lat,
                    point.lng
                );


            const ABx =
                B.x - A.x;

            const ABy =
                B.y - A.y;

            const APx =
                P.x - A.x;

            const APy =
                P.y - A.y;


            const ab2 =
                ABx * ABx +
                ABy * ABy;


            let t = 0;


            if (ab2 > 0) {

                t =
                    (
                        APx * ABx +
                        APy * ABy
                    ) / ab2;

                t =
                    Math.max(
                        0,
                        Math.min(1, t)
                    );
            }


            const nearestX =
                A.x + ABx * t;

            const nearestY =
                A.y + ABy * t;


            const dx =
                P.x - nearestX;

            const dy =
                P.y - nearestY;


            const distance =
                Math.sqrt(
                    dx * dx +
                    dy * dy
                );


            if (
                distance <
                nearestDistance
            ) {

                nearestDistance =
                    distance;

                nearestIndex =
                    i;

                nearestPoint = {

                    lat:
                        start.lat +
                        (
                            end.lat -
                            start.lat
                        ) * t,

                    lng:
                        start.lng +
                        (
                            end.lng -
                            start.lng
                        ) * t
                };
            }
        }


        /*
        |--------------------------------------------------------------------------
        | Covered Route
        |--------------------------------------------------------------------------
        */

        const travelledPoints = [];


        for (
            let i = 0;
            i <= nearestIndex;
            i++
        ) {

            travelledPoints.push(
                routePoints[i]
            );
        }


        if (nearestPoint) {

            travelledPoints.push(
                nearestPoint
            );
        }


        /*
        |--------------------------------------------------------------------------
        | Remaining Route
        |--------------------------------------------------------------------------
        */

        const remainingPoints = [];


        if (nearestPoint) {

            remainingPoints.push(
                nearestPoint
            );
        }


        for (
            let i = nearestIndex + 1;
            i < routePoints.length;
            i++
        ) {

            remainingPoints.push(
                routePoints[i]
            );
        }


        return {

            travelledPoints,

            remainingPoints

        };
    }
}

module.exports = geoHelper;