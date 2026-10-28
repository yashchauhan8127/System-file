
const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50kb' }));

const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// Keep the existing Android API compatible.
let locationHistory = [];
const MAX_HISTORY = 200;

// Existing Safe Zone: Kanpur, 1 km radius.
const SAFE_ZONE = {
    latitude: 26.4499,
    longitude: 80.3319,
    radiusKm: 1.0
};

function getDistanceKm(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const toRad = value => value * Math.PI / 180;

    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);

    const a =
        Math.sin(dLat / 2) ** 2 +
        Math.cos(toRad(lat1)) *
        Math.cos(toRad(lat2)) *
        Math.sin(dLon / 2) ** 2;

    return 2 * R * Math.atan2(
        Math.sqrt(a),
        Math.sqrt(1 - a)
    );
}

function validCoordinate(value, min, max) {
    const number = Number(value);
    return Number.isFinite(number) && number >= min && number <= max;
}

// Android sends location here. Endpoint and event names stay unchanged.
app.post('/location', (req, res) => {
    const body = req.body || {};
    const { latitude, longitude } = body;

    if (
        !validCoordinate(latitude, -90, 90) ||
        !validCoordinate(longitude, -180, 180)
    ) {
        return res.status(400).json({
            message: 'Valid latitude and longitude are required'
        });
    }

    const data = {
        ...body,
        latitude: Number(latitude),
        longitude: Number(longitude)
    };

    const distance = getDistanceKm(
        data.latitude,
        data.longitude,
        SAFE_ZONE.latitude,
        SAFE_ZONE.longitude
    );

    data.outsideSafeZone = distance > SAFE_ZONE.radiusKm;
    data.distanceKm = distance.toFixed(2);

    if (data.timestamp == null) {
        data.timestamp = Date.now();
    }

    locationHistory.push(data);

    if (locationHistory.length > MAX_HISTORY) {
        locationHistory.shift();
    }

    console.log(
        'Location received:',
        data.latitude,
        data.longitude,
        '| Safe Zone:',
        data.outsideSafeZone ? 'OUTSIDE' : 'INSIDE'
    );

    io.emit('locationUpdate', data);

    return res.status(200).json({
        message: 'Location received successfully'
    });
});

app.get('/history', (req, res) => {
    res.json(locationHistory);
});

app.get('/dashboard', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#101827">
<title>Live Location | Control Center</title>

<link rel="stylesheet"
 href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">

<style>
:root {
    color-scheme: dark;
    --bg: #0b1120;
    --panel: #111b2e;
    --panel2: #17243a;
    --border: #293750;
    --text: #edf4ff;
    --muted: #91a4c1;
    --blue: #4f8cff;
    --green: #35d49a;
    --red: #ff647c;
}

* { box-sizing: border-box; }

body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: Inter, system-ui, -apple-system, sans-serif;
}

button, input { font: inherit; }

button {
    cursor: pointer;
    color: var(--text);
    background: var(--panel2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 10px 12px;
    transition: .2s;
}

button:hover {
    border-color: var(--blue);
    transform: translateY(-1px);
}

.topbar {
    min-height: 76px;
    padding: 14px 22px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    flex-wrap: wrap;
    background: #101a2c;
    border-bottom: 1px solid var(--border);
}

.brand {
    display: flex;
    align-items: center;
    gap: 12px;
}

.logo {
    width: 44px;
    height: 44px;
    border-radius: 14px;
    display: grid;
    place-items: center;
    background: linear-gradient(135deg, #397bff, #795bff);
    font-size: 23px;
}

h1 { font-size: 19px; margin: 0; }
.subtitle { color: var(--muted); font-size: 12px; margin-top: 4px; }

.status {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--muted);
    font-size: 13px;
}

.dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: #8491a7;
}

.dot.online {
    background: var(--green);
    box-shadow: 0 0 12px #35d49a88;
}

.layout {
    display: grid;
    grid-template-columns: 320px minmax(0, 1fr);
    min-height: calc(100vh - 76px);
}

.sidebar {
    padding: 20px;
    border-right: 1px solid var(--border);
    background: #0e1728;
}

.section-label {
    font-size: 11px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: var(--muted);
    margin: 4px 0 12px;
}

.card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 16px;
    margin-bottom: 14px;
}

.card-title {
    font-size: 13px;
    color: var(--muted);
    margin-bottom: 12px;
}

.big-value {
    font-size: 20px;
    font-weight: 700;
    overflow-wrap: anywhere;
}

.small-value {
    font-size: 12px;
    line-height: 1.7;
    color: var(--muted);
    margin-top: 8px;
    overflow-wrap: anywhere;
}

.pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 9px;
    border-radius: 20px;
    font-size: 11px;
    background: #243047;
    color: var(--muted);
}

.pill.green {
    background: #12372f;
    color: var(--green);
}

.pill.red {
    background: #401e2b;
    color: var(--red);
}

.search-row {
    display: flex;
    gap: 7px;
}

.search-row input {
    width: 100%;
    min-width: 0;
    background: #0b1424;
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 11px;
    outline: none;
}

.search-row input:focus { border-color: var(--blue); }

.primary {
    background: var(--blue);
    border-color: var(--blue);
    font-weight: 600;
}

.full { width: 100%; }

.map-area {
    min-width: 0;
    padding: 18px;
    display: flex;
    flex-direction: column;
    gap: 12px;
}

.map-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    flex-wrap: wrap;
}

.toolbar-group {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
}

#map {
    height: calc(100vh - 155px);
    min-height: 430px;
    width: 100%;
    border: 1px solid var(--border);
    border-radius: 18px;
    overflow: hidden;
    background: #17243a;
}

.map-note {
    color: var(--muted);
    font-size: 12px;
    line-height: 1.5;
}

.leaflet-popup-content-wrapper,
.leaflet-popup-tip {
    background: #111b2e;
    color: #edf4ff;
}

.leaflet-control-zoom a {
    background: #111b2e !important;
    color: #edf4ff !important;
    border-color: #293750 !important;
}

.leaflet-control-attribution {
    font-size: 10px !important;
}

.alert-box {
    display: none;
    padding: 12px;
    margin-bottom: 14px;
    border-radius: 12px;
    border: 1px solid #713043;
    background: #351925;
    color: #ff9bac;
    font-size: 13px;
    line-height: 1.5;
}

.alert-box.show { display: block; }

.footer {
    font-size: 11px;
    color: var(--muted);
    margin-top: 18px;
    line-height: 1.6;
}

@media (max-width: 850px) {
    .layout { grid-template-columns: 1fr; }
    .sidebar {
        border-right: 0;
        border-bottom: 1px solid var(--border);
    }
    .map-area { padding: 12px; }
    #map { height: 58vh; min-height: 350px; }
    .topbar { padding: 13px; }
}

@media (max-width: 430px) {
    .big-value { font-size: 17px; }
    .card { padding: 13px; }
}
</style>
</head>

<body>
<header class="topbar">
    <div class="brand">
        <div class="logo">⌖</div>
        <div>
            <h1>Live Location</h1>
            <div class="subtitle">Location tracking control center</div>
        </div>
    </div>
    <div class="status">
        <span class="dot" id="connectionDot"></span>
        <span id="connectionText">Connecting...</span>
    </div>
</header>

<div class="layout">
<aside class="sidebar">
    <div class="section-label">Live overview</div>

    <div id="alertBox" class="alert-box">
        ⚠ Safe Zone alert: device is outside the configured 1 km zone.
    </div>

    <div class="card">
        <div class="card-title">Current location status</div>
        <div class="big-value" id="zoneStatus">Waiting for GPS</div>
        <div style="margin-top:12px">
            <span class="pill" id="zonePill">● Waiting for data</span>
        </div>
    </div>

    <div class="card">
        <div class="card-title">Latitude</div>
        <div class="big-value" id="latitude">—</div>
        <div class="small-value">North / South coordinate</div>
    </div>

    <div class="card">
        <div class="card-title">Longitude</div>
        <div class="big-value" id="longitude">—</div>
        <div class="small-value">East / West coordinate</div>
    </div>

    <div class="card">
        <div class="card-title">Distance from Safe Zone center</div>
        <div class="big-value" id="distance">—</div>
        <div class="small-value">Configured radius: 1.00 km</div>
    </div>

    <div class="card">
        <div class="card-title">Last GPS update</div>
        <div class="big-value" id="lastUpdate" style="font-size:16px">Waiting...</div>
        <div class="small-value" id="historyCount">0 saved points in current session</div>
    </div>

    <div class="section-label" style="margin-top:22px">Find a place</div>
    <div class="card">
        <div class="search-row">
            <input id="placeSearch" type="search"
                   placeholder="Search city or place..."
                   aria-label="Search for a place">
            <button class="primary" id="searchBtn">Go</button>
        </div>
        <div class="small-value" id="searchMessage">
            Search a place to view it on the map.
        </div>
    </div>

    <button class="full" id="centerBtn">◎ Center on latest location</button>

    <div class="footer">
        Map tiles © OpenStreetMap contributors.<br>
        Safe Zone is calculated by the server. GPS accuracy depends on the device.
    </div>
</aside>

<main class="map-area">
    <div class="map-toolbar">
        <div>
            <strong>Live map</strong>
            <div class="subtitle">Latest position and location trail</div>
        </div>
        <div class="toolbar-group">
            <button id="lightBtn">☀ Light map</button>
            <button id="darkBtn">☾ Dark map</button>
            <button id="fitBtn">↗ Fit trail</button>
        </div>
    </div>
    <div id="map"></div>
    <div class="map-note" id="mapStatus">
        Waiting for location data from your Android app.
    </div>
</main>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="/socket.io/socket.io.js"></script>
<script>
const SAFE_LAT = ${SAFE_ZONE.latitude};
const SAFE_LNG = ${SAFE_ZONE.longitude};
const SAFE_RADIUS_METERS = ${SAFE_ZONE.radiusKm * 1000};

const map = L.map('map', {
    zoomControl: true
}).setView([SAFE_LAT, SAFE_LNG], 12);

const lightLayer = L.tileLayer(
    'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    {
        maxZoom: 19,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }
);

const darkLayer = L.tileLayer(
    'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    {
        maxZoom: 20,
        attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
    }
);

lightLayer.addTo(map);

L.circle([SAFE_LAT, SAFE_LNG], {
    radius: SAFE_RADIUS_METERS,
    color: '#35d49a',
    weight: 2,
    fillColor: '#35d49a',
    fillOpacity: 0.12
}).addTo(map).bindPopup('Safe Zone · 1 km radius');

const safeCenter = L.circleMarker([SAFE_LAT, SAFE_LNG], {
    radius: 5,
    color: '#35d49a',
    fillColor: '#35d49a',
    fillOpacity: 1
}).addTo(map).bindPopup('Safe Zone center');

let marker = null;
let latestLocation = null;
let hasReceivedLocation = false;
let trail = [];
let activeLayer = 'light';

const pathLine = L.polyline([], {
    color: '#4f8cff',
    weight: 4,
    opacity: 0.9,
    smoothFactor: 1
}).addTo(map);

const connectionDot = document.getElementById('connectionDot');
const connectionText = document.getElementById('connectionText');

function setConnection(online) {
    connectionDot.classList.toggle('online', online);
    connectionText.textContent = online ? 'Connected' : 'Reconnecting...';
}

function formatTime(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'Time unavailable';
    return date.toLocaleString();
}

function setZone(outside) {
    const status = document.getElementById('zoneStatus');
    const pill = document.getElementById('zonePill');
    const alertBox = document.getElementById('alertBox');

    if (outside) {
        status.textContent = 'Outside Safe Zone';
        pill.textContent = '● Outside zone';
        pill.className = 'pill red';
        alertBox.classList.add('show');
    } else {
        status.textContent = 'Inside Safe Zone';
        pill.textContent = '● Inside zone';
        pill.className = 'pill green';
        alertBox.classList.remove('show');
    }
}

function showLocation(data, fromHistory) {
    const lat = Number(data.latitude);
    const lng = Number(data.longitude);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return;

    latestLocation = data;
    hasReceivedLocation = true;

    const point = [lat, lng];

    document.getElementById('latitude').textContent = lat.toFixed(6);
    document.getElementById('longitude').textContent = lng.toFixed(6);
    document.getElementById('distance').textContent =
        data.distanceKm != null ? Number(data.distanceKm).toFixed(2) + ' km' : '—';

    document.getElementById('lastUpdate').textContent =
        formatTime(data.timestamp);

    if (data.outsideSafeZone != null) {
        setZone(Boolean(data.outsideSafeZone));
    } else {
        // Fallback for old history points without Safe Zone fields.
        const distance = map.distance(point, [SAFE_LAT, SAFE_LNG]) / 1000;
        setZone(distance > SAFE_RADIUS_METERS / 1000);
    }

    if (!marker) {
        marker = L.circleMarker(point, {
            radius: 9,
            color: '#ffffff',
            weight: 3,
            fillColor: '#397bff',
            fillOpacity: 1
        }).addTo(map);
    } else {
        marker.setLatLng(point);
    }

    marker.bindPopup(
        '<b>Latest location</b><br>' +
        'Lat: ' + lat.toFixed(6) + '<br>' +
        'Lng: ' + lng.toFixed(6) + '<br>' +
        'Updated: ' + formatTime(data.timestamp)
    );

    if (!fromHistory) {
        const last = trail[trail.length - 1];
        if (!last || last[0] !== lat || last[1] !== lng) {
            trail.push(point);
        }
        if (trail.length > 200) trail.shift();
        pathLine.setLatLngs(trail);
        map.panTo(point, { animate: true, duration: 0.5 });
    }

    document.getElementById('historyCount').textContent =
        trail.length + ' points shown on the trail';

    document.getElementById('mapStatus').textContent =
        'Last position: ' + lat.toFixed(5) + ', ' + lng.toFixed(5);
}

async function loadHistory() {
    try {
        const response = await fetch('/history');
        if (!response.ok) throw new Error('History request failed');

        const history = await response.json();
        if (!Array.isArray(history) || history.length === 0) {
            document.getElementById('mapStatus').textContent =
                'No saved locations yet. Start location sharing on Android.';
            return;
        }

        trail = history
            .filter(d =>
                Number.isFinite(Number(d.latitude)) &&
                Number.isFinite(Number(d.longitude))
            )
            .map(d => [Number(d.latitude), Number(d.longitude)])
            .slice(-200);

        pathLine.setLatLngs(trail);

        const last = history[history.length - 1];
        showLocation(last, true);

        const bounds = L.latLngBounds(trail);
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [35, 35], maxZoom: 15 });
        }
    } catch (error) {
        console.error('Could not load history:', error);
        document.getElementById('mapStatus').textContent =
            'Could not load history. Waiting for live updates.';
    }
}

const socket = io();

socket.on('connect', () => setConnection(true));
socket.on('disconnect', () => setConnection(false));

socket.on('locationUpdate', data => {
    if (!data) return;
    showLocation(data, false);
});

// Map style controls.
document.getElementById('lightBtn').addEventListener('click', () => {
    if (activeLayer !== 'light') {
        map.removeLayer(darkLayer);
        lightLayer.addTo(map);
        activeLayer = 'light';
    }
});

document.getElementById('darkBtn').addEventListener('click', () => {
    if (activeLayer !== 'dark') {
        map.removeLayer(lightLayer);
        darkLayer.addTo(map);
        activeLayer = 'dark';
    }
});

document.getElementById('centerBtn').addEventListener('click', () => {
    if (latestLocation) {
        map.setView([
            Number(latestLocation.latitude),
            Number(latestLocation.longitude)
        ], 16);
        marker.openPopup();
    } else {
        map.setView([SAFE_LAT, SAFE_LNG], 12);
        document.getElementById('mapStatus').textContent =
            'No GPS position received yet.';
    }
});

document.getElementById('fitBtn').addEventListener('click', () => {
    const bounds = pathLine.getBounds();
    if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [35, 35], maxZoom: 16 });
    } else if (latestLocation) {
        map.setView([
            Number(latestLocation.latitude),
            Number(latestLocation.longitude)
        ], 15);
    }
});

// Search one place only when the user submits the search.
async function searchPlace() {
    const input = document.getElementById('placeSearch');
    const message = document.getElementById('searchMessage');
    const query = input.value.trim();

    if (!query) {
        message.textContent = 'Enter a city or place name first.';
        return;
    }

    message.textContent = 'Searching...';

    try {
        const url = 'https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q='
            + encodeURIComponent(query);

        const response = await fetch(url);
        if (!response.ok) throw new Error('Search service unavailable');

        const results = await response.json();

        if (!results.length) {
            message.textContent = 'Place not found. Try a more specific name.';
            return;
        }

        const place = results[0];
        const lat = Number(place.lat);
        const lng = Number(place.lon);

        map.setView([lat, lng], 14);
        L.marker([lat, lng])
            .addTo(map)
            .bindPopup(place.display_name)
            .openPopup();

        message.textContent = place.display_name;
    } catch (error) {
        console.error('Place search failed:', error);
        message.textContent =
            'Search failed. Check your connection and try again later.';
    }
}

document.getElementById('searchBtn')
    .addEventListener('click', searchPlace);

document.getElementById('placeSearch')
    .addEventListener('keydown', event => {
        if (event.key === 'Enter') searchPlace();
    });

loadHistory();
</script>
</body>
</html>
    `);
});

server.listen(PORT, '0.0.0.0', () => {
    console.log('Server running on port ' + PORT);
});
