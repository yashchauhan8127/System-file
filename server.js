
'use strict';

const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' }
});

const PORT = Number(process.env.PORT) || 3000;
const MAX_MEMORY_POINTS = 2000;
const MAX_HISTORY_LIMIT = 5000;

app.use(cors());
app.use(express.json({ limit: '50kb' }));

// PostgreSQL is persistent storage. Without DATABASE_URL,
// the server uses temporary in-memory history.
let pool = null;
let dbReady = false;
let dbInitPromise = null;
const memoryHistory = [];

if (process.env.DATABASE_URL) {
    pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_SSL === 'false'
            ? false
            : { rejectUnauthorized: false },
        max: 5,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 10000
    });

    pool.on('error', err => {
        console.error('PostgreSQL pool error:', err.message);
    });

    dbInitPromise = initializeDatabase();
} else {
    console.warn(
        'DATABASE_URL is not set. History will be lost when the server restarts.'
    );
}

async function initializeDatabase() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS location_points (
                id BIGSERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL,
                accuracy DOUBLE PRECISION,
                speed DOUBLE PRECISION,
                bearing DOUBLE PRECISION,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        `);

        await pool.query(`
            CREATE INDEX IF NOT EXISTS location_points_recorded_at_idx
            ON location_points (recorded_at DESC)
        `);

        dbReady = true;
        console.log('PostgreSQL connected and table is ready.');
        return true;
    } catch (err) {
        dbReady = false;
        console.error('Database initialization failed:', err.message);
        return false;
    }
}

async function ensureDatabase() {
    if (!pool) return false;
    if (dbReady) return true;
    if (dbInitPromise) {
        await dbInitPromise;
        if (dbReady) return true;
    }
    dbInitPromise = initializeDatabase();
    return await dbInitPromise;
}

function validCoordinate(value, min, max) {
    if (value === null || value === undefined || value === '') {
        return false;
    }
    const n = Number(value);
    return Number.isFinite(n) && n >= min && n <= max;
}

function finiteOptional(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function parseTimestamp(value) {
    if (value === null || value === undefined || value === '') {
        return new Date();
    }

    // Android normally sends milliseconds since Unix epoch.
    if (typeof value === 'number' || /^\d+$/.test(String(value))) {
        let n = Number(value);

        // Accept seconds as well as milliseconds.
        if (n > 0 && n < 100000000000) n *= 1000;

        const date = new Date(n);
        if (!Number.isNaN(date.getTime())) return date;
    }

    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? new Date() : date;
}

function normalizeLocation(body) {
    const latitude = Number(body.latitude);
    const longitude = Number(body.longitude);
    const recordedAt = parseTimestamp(body.timestamp);

    return {
        ...body,
        latitude,
        longitude,
        timestamp: recordedAt.getTime(),
        accuracy: finiteOptional(body.accuracy),
        speed: finiteOptional(body.speed),
        bearing: finiteOptional(body.bearing)
    };
}

function asPublicPoint(row) {
    const payload = row.payload || {};
    const recordedAt = row.recorded_at
        ? new Date(row.recorded_at).getTime()
        : Number(payload.timestamp) || Date.now();

    return {
        ...payload,
        id: row.id,
        latitude: Number(row.latitude),
        longitude: Number(row.longitude),
        timestamp: recordedAt,
        accuracy: row.accuracy == null
            ? finiteOptional(payload.accuracy)
            : Number(row.accuracy),
        speed: row.speed == null
            ? finiteOptional(payload.speed)
            : Number(row.speed),
        bearing: row.bearing == null
            ? finiteOptional(payload.bearing)
            : Number(row.bearing)
    };
}

async function saveLocation(data) {
    if (!pool) {
        memoryHistory.push(data);
        if (memoryHistory.length > MAX_MEMORY_POINTS) {
            memoryHistory.splice(
                0,
                memoryHistory.length - MAX_MEMORY_POINTS
            );
        }
        return data;
    }

    const ready = await ensureDatabase();
    if (!ready) {
        throw new Error('Database is configured but unavailable.');
    }

    const result = await pool.query(
        `INSERT INTO location_points
            (latitude, longitude, recorded_at, accuracy, speed, bearing, payload)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
         RETURNING id, latitude, longitude, recorded_at,
                   accuracy, speed, bearing, payload`,
        [
            data.latitude,
            data.longitude,
            new Date(data.timestamp),
            data.accuracy,
            data.speed,
            data.bearing,
            JSON.stringify(data)
        ]
    );

    return asPublicPoint(result.rows[0]);
}

function parseDateParam(value, endOfDay = false) {
    if (!value) return null;

    // Accept YYYY-MM-DD date filters.
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        const date = new Date(
            value + (endOfDay ? 'T23:59:59.999' : 'T00:00:00.000')
        );
        return Number.isNaN(date.getTime()) ? null : date;
    }

    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
}

async function getHistory({ from, to, limit }) {
    const safeLimit = Math.min(
        MAX_HISTORY_LIMIT,
        Math.max(1, Number(limit) || 1000)
    );

    if (!pool) {
        let rows = memoryHistory.slice();

        const fromDate = parseDateParam(from);
        const toDate = parseDateParam(to, true);

        if (fromDate) {
            rows = rows.filter(
                p => Number(p.timestamp) >= fromDate.getTime()
            );
        }

        if (toDate) {
            rows = rows.filter(
                p => Number(p.timestamp) <= toDate.getTime()
            );
        }

        return rows.slice(-safeLimit);
    }

    const ready = await ensureDatabase();
    if (!ready) {
        throw new Error('Database is unavailable.');
    }

    const fromDate = parseDateParam(from);
    const toDate = parseDateParam(to, true);

    const result = await pool.query(
        `SELECT id, latitude, longitude, recorded_at,
                accuracy, speed, bearing, payload
         FROM location_points
         WHERE ($1::timestamptz IS NULL OR recorded_at >= $1)
           AND ($2::timestamptz IS NULL OR recorded_at <= $2)
         ORDER BY recorded_at DESC
         LIMIT $3`,
        [fromDate, toDate, safeLimit]
    );

    return result.rows.reverse().map(asPublicPoint);
}

function haversineMeters(a, b) {
    const R = 6371000;
    const rad = value => value * Math.PI / 180;

    const dLat = rad(b.latitude - a.latitude);
    const dLon = rad(b.longitude - a.longitude);
    const lat1 = rad(a.latitude);
    const lat2 = rad(b.latitude);

    const h =
        Math.sin(dLat / 2) ** 2 +
        Math.cos(lat1) * Math.cos(lat2) *
        Math.sin(dLon / 2) ** 2;

    return 2 * R * Math.atan2(
        Math.sqrt(h),
        Math.sqrt(Math.max(0, 1 - h))
    );
}

function calculateDistanceMeters(points) {
    let total = 0;

    for (let i = 1; i < points.length; i++) {
        total += haversineMeters(points[i - 1], points[i]);
    }

    return total;
}

function simplifyPoints(points, maxPoints = 100) {
    if (points.length <= maxPoints) return points;

    const step = (points.length - 1) / (maxPoints - 1);
    const output = [];

    for (let i = 0; i < maxPoints; i++) {
        output.push(points[Math.round(i * step)]);
    }

    return output;
}

// Android compatibility: do not rename this endpoint.
app.post('/location', async (req, res) => {
    try {
        const body = req.body || {};

        if (
            !validCoordinate(body.latitude, -90, 90) ||
            !validCoordinate(body.longitude, -180, 180)
        ) {
            return res.status(400).json({
                message: 'Valid latitude and longitude are required.'
            });
        }

        const normalized = normalizeLocation(body);
        const saved = await saveLocation(normalized);

        // Android compatibility: do not rename this event.
        io.emit('locationUpdate', saved);

        return res.status(200).json({
            message: 'Location received successfully.',
            point: saved
        });
    } catch (err) {
        console.error('Failed to save location:', err.message);

        return res.status(503).json({
            message: 'Could not save location. Check server/database logs.'
        });
    }
});

// Latest saved GPS point.
app.get('/latest', async (req, res) => {
    try {
        const history = await getHistory({
            limit: 1,
            from: null,
            to: null
        });

        return res.json(history[history.length - 1] || null);
    } catch (err) {
        console.error('Latest location error:', err.message);
        return res.status(503).json({ message: 'Could not load latest location.' });
    }
});

// Saved location history. Optional filters: from, to, limit.
app.get('/history', async (req, res) => {
    try {
        const history = await getHistory({
            from: req.query.from,
            to: req.query.to,
            limit: req.query.limit
        });

        return res.json(history);
    } catch (err) {
        console.error('History error:', err.message);
        return res.status(503).json({ message: 'Could not load location history.' });
    }
});

// Best-effort road matching. Public OSRM can be unavailable or rate-limited.
app.get('/matched-history', async (req, res) => {
    try {
        const history = await getHistory({
            from: req.query.from,
            to: req.query.to,
            limit: req.query.limit || 1000
        });

        const points = history
            .filter(p =>
                Number.isFinite(Number(p.latitude)) &&
                Number.isFinite(Number(p.longitude))
            )
            .map(p => ({
                latitude: Number(p.latitude),
                longitude: Number(p.longitude),
                timestamp: Number(p.timestamp)
            }));

        if (points.length < 2) {
            return res.json({
                matched: false,
                reason: 'At least two GPS points are required.',
                points,
                distanceMeters: 0
            });
        }

        // OSRM match requests have URL-length limits. Keep a recent subset.
        const selected = simplifyPoints(points, 100);
        const coordinates = selected
            .map(p => `${p.longitude},${p.latitude}`)
            .join(';');

        const timestamps = selected
            .map(p => Math.floor(p.timestamp / 1000))
            .join(';');

        const url =
            'https://router.project-osrm.org/match/v1/driving/' +
            coordinates +
            '?overview=full&geometries=geojson&steps=false&timestamps=' +
            timestamps;

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 12000);

        let response;
        try {
            response = await fetch(url, {
                signal: controller.signal,
                headers: { Accept: 'application/json' }
            });
        } finally {
            clearTimeout(timeout);
        }

        if (!response.ok) {
            return res.status(502).json({
                matched: false,
                reason: `Road matching service returned HTTP ${response.status}.`,
                points: selected,
                distanceMeters: calculateDistanceMeters(selected)
            });
        }

        const result = await response.json();

        if (
            result.code !== 'Ok' ||
            !result.matchings ||
            !result.matchings.length
        ) {
            return res.json({
                matched: false,
                reason: 'No matching road route was found.',
                points: selected,
                distanceMeters: calculateDistanceMeters(selected)
            });
        }

        const geometry = result.matchings
            .filter(m => m.geometry && Array.isArray(m.geometry.coordinates))
            .flatMap(m => m.geometry.coordinates);

        return res.json({
            matched: true,
            geometry,
            points: selected,
            distanceMeters: result.matchings.reduce(
                (sum, m) => sum + Number(m.distance || 0),
                0
            ),
            durationSeconds: result.matchings.reduce(
                (sum, m) => sum + Number(m.duration || 0),
                0
            )
        });
    } catch (err) {
        console.error('Road matching error:', err.message);

        return res.status(502).json({
            matched: false,
            reason: 'Road matching is temporarily unavailable.'
        });
    }
});

app.get('/health', async (req, res) => {
    let database = 'not-configured';

    if (pool) {
        database = await ensureDatabase() ? 'connected' : 'unavailable';
    }

    return res.json({
        status: 'ok',
        database,
        storage: pool ? 'postgresql' : 'memory',
        uptimeSeconds: Math.round(process.uptime()),
        time: new Date().toISOString()
    });
});

app.get('/dashboard', (req, res) => {
    res.type('html').send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#0b1120">
<title>Live GPS Control Center</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
:root {
  color-scheme:dark;--bg:#0b1120;--panel:#111b2e;--panel2:#17243a;
  --border:#293750;--text:#edf4ff;--muted:#91a4c1;
  --blue:#4f8cff;--green:#35d49a;--purple:#a78bfa
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
font-family:Inter,system-ui,-apple-system,sans-serif}
button,input{font:inherit}
button{cursor:pointer;color:var(--text);background:var(--panel2);
border:1px solid var(--border);border-radius:10px;padding:10px 12px}
button:hover{border-color:var(--blue)}
input{color:var(--text);background:#0b1424;border:1px solid var(--border);
border-radius:9px;padding:10px;min-width:0}
.topbar{padding:17px 22px;display:flex;justify-content:space-between;
align-items:center;gap:12px;flex-wrap:wrap;background:#101a2c;
border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;
background:linear-gradient(135deg,#397bff,#795bff);font-size:24px}
h1{font-size:19px;margin:0}
.sub,.muted{color:var(--muted);font-size:12px;margin-top:4px}
.status{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted)}
.dot{width:9px;height:9px;border-radius:50%;background:#8491a7}
.dot.on{background:var(--green);box-shadow:0 0 12px #35d49a88}
.layout{display:grid;grid-template-columns:320px minmax(0,1fr);min-height:calc(100vh - 76px)}
.sidebar{padding:18px;background:#0e1728;border-right:1px solid var(--border)}
.label{font-size:11px;letter-spacing:1.4px;text-transform:uppercase;
color:var(--muted);margin:5px 0 12px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:15px;
padding:15px;margin-bottom:12px}
.cardtitle{font-size:12px;color:var(--muted);margin-bottom:9px}
.value{font-size:21px;font-weight:700;overflow-wrap:anywhere}
.small{font-size:12px;color:var(--muted);line-height:1.6;margin-top:7px;overflow-wrap:anywhere}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.search{display:flex;gap:7px}
.search input{width:100%}
.primary{background:var(--blue);border-color:var(--blue)}
.full{width:100%}
.maparea{padding:17px;min-width:0;display:flex;flex-direction:column;gap:12px}
.toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.actions{display:flex;gap:7px;flex-wrap:wrap}
#map{height:calc(100vh - 180px);min-height:420px;border:1px solid var(--border);
border-radius:17px;overflow:hidden;background:var(--panel2)}
.note{font-size:12px;color:var(--muted);line-height:1.6}
.tablewrap{overflow:auto;border:1px solid var(--border);border-radius:12px}
table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}
th,td{text-align:left;padding:10px;border-bottom:1px solid var(--border)}
th{color:var(--muted);font-weight:600;background:#111b2e}
tr:last-child td{border-bottom:0}
.green{color:var(--green)}.purple{color:var(--purple)}
@media(max-width:850px){.layout{grid-template-columns:1fr}
.sidebar{border-right:0;border-bottom:1px solid var(--border)}
#map{height:55vh;min-height:340px}.maparea{padding:12px}}
</style>
</head>
<body>
<header class="topbar">
 <div class="brand"><div class="logo">⌖</div><div>
  <h1>Live GPS Control Center</h1>
  <div class="sub">Real-time location and route history</div>
 </div></div>
 <div class="status"><span class="dot" id="dot"></span><span id="connection">Connecting...</span></div>
</header>
<div class="layout">
<aside class="sidebar">
 <div class="label">Live overview</div>
 <div class="card"><div class="cardtitle">Tracking status</div>
  <div class="value" id="tracking">Waiting for GPS</div>
  <div class="small" id="lastUpdate">No location received yet</div>
 </div>
 <div class="grid2">
  <div class="card"><div class="cardtitle">Latitude</div><div class="value" id="lat">—</div></div>
  <div class="card"><div class="cardtitle">Longitude</div><div class="value" id="lng">—</div></div>
  <div class="card"><div class="cardtitle">GPS accuracy</div><div class="value" id="accuracy">—</div></div>
  <div class="card"><div class="cardtitle">Speed</div><div class="value" id="speed">—</div></div>
 </div>
 <div class="card"><div class="cardtitle">Trail distance estimate</div>
  <div class="value" id="distance">0 m</div>
  <div class="small">Calculated from recorded GPS points. Road matching is an estimate.</div>
 </div>
 <div class="card"><div class="cardtitle">History filter</div>
  <div class="small">From date</div><input id="fromDate" type="date" style="width:100%">
  <div class="small">To date</div><input id="toDate" type="date" style="width:100%;margin-top:6px">
  <div class="actions" style="margin-top:10px">
   <button class="primary" id="applyHistory">Load history</button>
   <button id="liveBtn">Return to live</button>
  </div>
  <div class="small" id="historyStatus">Showing saved GPS history.</div>
 </div>
 <div class="card"><div class="cardtitle">Find a place</div>
  <div class="search"><input id="place" placeholder="Search city or place">
   <button class="primary" id="searchBtn">Go</button></div>
  <div class="small" id="searchStatus">Search to view a place on the map.</div>
 </div>
 <button class="full" id="centerBtn">◎ Center on latest location</button>
 <div class="small">Map tiles © OpenStreetMap contributors. GPS quality depends on device, signal, and network.</div>
</aside>
<main class="maparea">
 <div class="toolbar"><div><strong>Live map and route</strong>
  <div class="sub">Blue: recorded GPS trail · Green: road-matched route when available</div>
 </div><div class="actions">
  <button id="lightBtn">☀ Light map</button><button id="darkBtn">☾ Dark map</button>
  <button id="fitBtn">↗ Fit trail</button><button id="matchBtn">⌁ Match roads</button>
 </div></div>
 <div id="map"></div>
 <div class="note" id="mapStatus">Waiting for Android GPS data...</div>
 <div class="card"><div class="cardtitle">Recent location points</div>
  <div class="tablewrap"><table><thead><tr><th>Time</th><th>Latitude</th>
  <th>Longitude</th><th>Accuracy</th><th>Speed</th></tr></thead>
  <tbody id="rows"><tr><td colspan="5">No points yet</td></tr></tbody></table></div>
 </div>
</main>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="/socket.io/socket.io.js"></script>
<script>
const $ = id => document.getElementById(id);
const map = L.map('map').setView([20.5937,78.9629],5);

const lightLayer = L.tileLayer(
 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
 {maxZoom:19,attribution:'&copy; OpenStreetMap contributors'}
);
const darkLayer = L.tileLayer(
 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
 {maxZoom:20,attribution:'&copy; OpenStreetMap contributors &copy; CARTO'}
);
lightLayer.addTo(map);
let activeLayer = 'light';

const trailLine = L.polyline([],{
 color:'#4f8cff',weight:4,opacity:.9
}).addTo(map);
const matchedLine = L.polyline([],{
 color:'#35d49a',weight:5,opacity:.9
}).addTo(map);

let marker = null;
let allPoints = [];
let visiblePoints = [];
let latest = null;
let historyMode = false;
let searchMarker = null;
let matchRequestId = 0;

const socket = io();

socket.on('connect',()=>{
 $('dot').classList.add('on');
 $('connection').textContent='Connected';
});
socket.on('disconnect',()=>{
 $('dot').classList.remove('on');
 $('connection').textContent='Reconnecting...';
});
socket.on('connect_error',()=>{
 $('dot').classList.remove('on');
 $('connection').textContent='Connection error';
});

function dateOf(p){
 let n = Number(p.timestamp);
 if(!Number.isFinite(n)) return new Date(p.timestamp);
 if(n>0 && n<100000000000) n*=1000;
 return new Date(n);
}
function formatDate(value){
 const d = value instanceof Date ? value : dateOf(value);
 return Number.isNaN(d.getTime()) ? 'Unknown time' : d.toLocaleString();
}
function distanceMeters(points){
 let total=0;
 for(let i=1;i<points.length;i++){
  total+=map.distance(
   [points[i-1].latitude,points[i-1].longitude],
   [points[i].latitude,points[i].longitude]
  );
 }
 return total;
}
function distanceLabel(m){
 if(!Number.isFinite(m)) return '—';
 return m>=1000 ? (m/1000).toFixed(2)+' km' : Math.round(m)+' m';
}
function validPoint(p){
 const lat=Number(p.latitude),lng=Number(p.longitude);
 return Number.isFinite(lat)&&Number.isFinite(lng)&&lat>=-90&&lat<=90&&lng>=-180&&lng<=180;
}
function normalizePoint(p){
 return {...p,latitude:Number(p.latitude),longitude:Number(p.longitude)};
}
function drawTrail(points){
 visiblePoints=points.filter(validPoint).map(normalizePoint);
 trailLine.setLatLngs(visiblePoints.map(p=>[p.latitude,p.longitude]));
 $('distance').textContent=distanceLabel(distanceMeters(visiblePoints));
 renderTable(visiblePoints);
 if(visiblePoints.length===0){
  matchedLine.setLatLngs([]);
  $('mapStatus').textContent='No GPS points in this view.';
  return;
 }
 const bounds=trailLine.getBounds();
 if(bounds.isValid()) map.fitBounds(bounds,{padding:[30,30],maxZoom:16});
 $('mapStatus').textContent=visiblePoints.length+' GPS points shown.';
}
function updateMarker(p){
 if(!validPoint(p)) return;
 const pos=[Number(p.latitude),Number(p.longitude)];
 if(!marker){
  marker=L.circleMarker(pos,{
   radius:9,color:'#fff',weight:3,fillColor:'#4f8cff',fillOpacity:1
  }).addTo(map);
 }else marker.setLatLng(pos);
 marker.bindPopup(
  '<b>Latest GPS location</b><br>Lat: '+Number(p.latitude).toFixed(6)+
  '<br>Lng: '+Number(p.longitude).toFixed(6)+'<br>'+formatDate(p)
 );
}
function updateMetrics(p){
 $('tracking').textContent='GPS location received';
 $('lat').textContent=Number(p.latitude).toFixed(6);
 $('lng').textContent=Number(p.longitude).toFixed(6);
 const acc=Number(p.accuracy);
 $('accuracy').textContent=p.accuracy==null||!Number.isFinite(acc)?'—':Math.round(acc)+' m';
 const speed=Number(p.speed);
 $('speed').textContent=p.speed==null||!Number.isFinite(speed)?'—':(speed*3.6).toFixed(1)+' km/h';
 $('lastUpdate').textContent='Updated '+formatDate(p);
}
function renderTable(points){
 const recent=points.slice(-12).reverse();
 if(!recent.length){
  $('rows').innerHTML='<tr><td colspan="5">No points yet</td></tr>';
  return;
 }
 $('rows').innerHTML=recent.map(p=>{
  const acc=Number(p.accuracy),speed=Number(p.speed);
  return '<tr><td>'+escapeHtml(formatDate(p))+'</td><td>'+
   p.latitude.toFixed(6)+'</td><td>'+p.longitude.toFixed(6)+'</td><td>'+
   (p.accuracy==null||!Number.isFinite(acc)?'—':Math.round(acc)+' m')+'</td><td>'+
   (p.speed==null||!Number.isFinite(speed)?'—':(speed*3.6).toFixed(1)+' km/h')+
   '</td></tr>';
 }).join('');
}
function escapeHtml(value){
 return String(value).replace(/[&<>"']/g,c=>({
  '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
 }[c]));
}
function mergePoint(p){
 if(!validPoint(p)) return;
 const point=normalizePoint(p);
 latest=point;
 updateMetrics(point);
 updateMarker(point);
 const index=allPoints.findIndex(x=>
  Number(x.timestamp)===Number(point.timestamp)&&
  x.latitude===point.latitude&&x.longitude===point.longitude
 );
 if(index<0) allPoints.push(point);
 allPoints.sort((a,b)=>Number(a.timestamp)-Number(b.timestamp));
 if(allPoints.length>5000) allPoints=allPoints.slice(-5000);
 if(!historyMode){
  drawTrail(allPoints);
  map.panTo([point.latitude,point.longitude],{animate:true,duration:.35});
 }
}
async function loadHistory(){
 historyMode=true;
 matchedLine.setLatLngs([]);
 $('historyStatus').textContent='Loading saved history...';
 const params=new URLSearchParams({limit:'5000'});
 if($('fromDate').value) params.set('from',$('fromDate').value);
 if($('toDate').value) params.set('to',$('toDate').value);
 try{
  const response=await fetch('/history?'+params.toString());
  if(!response.ok) throw new Error('History request failed');
  const data=await response.json();
  if(!Array.isArray(data)) throw new Error('Invalid history response');
  drawTrail(data);
  if(data.length){
   updateMarker(data[data.length-1]);
   $('historyStatus').textContent=data.length+' points loaded.';
  }else{
   $('historyStatus').textContent='No saved points for this date range.';
  }
 }catch(err){
  console.error(err);
  $('historyStatus').textContent='Could not load history. Check server status.';
 }
}
async function returnToLive(){
 historyMode=false;
 matchedLine.setLatLngs([]);
 $('historyStatus').textContent='Live tracking enabled.';
 try{
  const response=await fetch('/history?limit=2000');
  if(response.ok){
   const data=await response.json();
   if(Array.isArray(data)){
    allPoints=data.filter(validPoint).map(normalizePoint);
   }
  }
 }catch(err){console.error('Could not reload live trail:',err);}
 if(latest && validPoint(latest)){
  const i=allPoints.findIndex(p=>Number(p.timestamp)===Number(latest.timestamp));
  if(i<0) allPoints.push(latest);
 }
 allPoints.sort((a,b)=>Number(a.timestamp)-Number(b.timestamp));
 drawTrail(allPoints);
 if(latest){
  updateMarker(latest);
  map.setView([latest.latitude,latest.longitude],16);
 }
}
socket.on('locationUpdate',p=>{
 if(!p)return;
 mergePoint(p);
});

async function matchRoads(){
 const requestId=++matchRequestId;
 $('mapStatus').textContent='Matching GPS points to roads...';
 const params=new URLSearchParams({limit:'1000'});
 if(historyMode && $('fromDate').value) params.set('from',$('fromDate').value);
 if(historyMode && $('toDate').value) params.set('to',$('toDate').value);
 try{
  const response=await fetch('/matched-history?'+params.toString());
  const data=await response.json();
  if(requestId!==matchRequestId)return;
  if(!response.ok) throw new Error(data.reason||'Road matching failed');
  if(data.matched && Array.isArray(data.geometry) && data.geometry.length){
   matchedLine.setLatLngs(data.geometry.map(c=>[c[1],c[0]]));
   $('mapStatus').textContent='Road-matched route: '+
    distanceLabel(Number(data.distanceMeters)||0)+'. OSRM result is approximate.';
  }else{
   matchedLine.setLatLngs([]);
   $('mapStatus').textContent=(data.reason||'No road match available')+
    ' Showing the raw GPS trail.';
  }
 }catch(err){
  if(requestId!==matchRequestId)return;
  matchedLine.setLatLngs([]);
  $('mapStatus').textContent='Road matching unavailable. Raw GPS trail remains visible.';
 }
}

$('lightBtn').addEventListener('click',()=>{
 if(activeLayer==='light')return;
 map.removeLayer(darkLayer);lightLayer.addTo(map);activeLayer='light';
});
$('darkBtn').addEventListener('click',()=>{
 if(activeLayer==='dark')return;
 map.removeLayer(lightLayer);darkLayer.addTo(map);activeLayer='dark';
});
$('fitBtn').addEventListener('click',()=>{
 const bounds=trailLine.getBounds();
 if(bounds.isValid())map.fitBounds(bounds,{padding:[30,30],maxZoom:16});
 else if(latest)map.setView([latest.latitude,latest.longitude],16);
});
$('centerBtn').addEventListener('click',()=>{
 if(latest){
  map.setView([latest.latitude,latest.longitude],16);
  if(marker)marker.openPopup();
 }else{
  $('mapStatus').textContent='No GPS location received yet.';
 }
});
$('applyHistory').addEventListener('click',loadHistory);
$('liveBtn').addEventListener('click',returnToLive);
$('matchBtn').addEventListener('click',matchRoads);

async function searchPlace(){
 const query=$('place').value.trim();
 if(!query){$('searchStatus').textContent='Enter a place name first.';return;}
 $('searchStatus').textContent='Searching...';
 try{
  const url='https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q='+
   encodeURIComponent(query);
  const response=await fetch(url);
  if(!response.ok)throw new Error('Search unavailable');
  const results=await response.json();
  if(!results.length){$('searchStatus').textContent='Place not found.';return;}
  const place=results[0],lat=Number(place.lat),lng=Number(place.lon);
  if(searchMarker)map.removeLayer(searchMarker);
  searchMarker=L.marker([lat,lng]).addTo(map)
   .bindPopup(escapeHtml(place.display_name)).openPopup();
  map.setView([lat,lng],14);
  $('searchStatus').textContent=place.display_name;
 }catch(err){
  $('searchStatus').textContent='Search failed. Try again later.';
 }
}
$('searchBtn').addEventListener('click',searchPlace);
$('place').addEventListener('keydown',e=>{if(e.key==='Enter')searchPlace();});

// Load full saved history on first page load, not just the latest point.
(async()=>{
 try{
  const response=await fetch('/history?limit=2000');
  if(response.ok){
   const data=await response.json();
   if(Array.isArray(data)){
    allPoints=data.filter(validPoint).map(normalizePoint);
    drawTrail(allPoints);
    if(allPoints.length){
     latest=allPoints[allPoints.length-1];
     updateMetrics(latest);
     updateMarker(latest);
     map.setView([latest.latitude,latest.longitude],15);
    }
   }
  }
 }catch(err){
  $('mapStatus').textContent='History could not be loaded; waiting for live GPS.';
 }
})();
</script>
</body>
</html>`);
});

app.get('/', (req, res) => {
    res.redirect('/dashboard');
});

app.use((err, req, res, next) => {
    console.error('Unhandled request error:', err.message);

    if (res.headersSent) return next(err);

    res.status(500).json({ message: 'Internal server error.' });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log('Server running on port ' + PORT);
});

async function shutdown() {
    console.log('Shutting down server...');

    server.close(async () => {
        try {
            if (pool) await pool.end();
        } catch (err) {
            console.error('Database shutdown error:', err.message);
        }
        process.exit(0);
    });

    setTimeout(() => process.exit(1), 10000).unref();
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
