// Store GPS coordinates as latitude,longitude.
module.exports = function alertLocation(packet) {
    const latitude = packet?.latitude ?? packet?.lat;
    const longitude = packet?.longitude ?? packet?.lng;
    if (latitude == null || longitude == null || String(latitude).trim() === "" || String(longitude).trim() === "") return null;
    const lat = Number(latitude);
    const lng = Number(longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
    return lat + "," + lng;
};
