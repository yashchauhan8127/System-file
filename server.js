const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server);

let locationHistory = [];

app.post('/location', (req, res) => {
    const data = req.body;
    console.log('Location received:', data);
    locationHistory.push(data);
    if (locationHistory.length > 100) locationHistory.shift();
    io.emit('locationUpdate', data);
    res.status(200).json({ message: 'Location received successfully' });
});

app.get('/history', (req, res) => {
    res.json(locationHistory);
});

app.get('/dashboard', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>Parent Dashboard</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <style>
        #map { height: 500px; width: 100%; }
        body { font-family: sans-serif; padding: 20px; }
    </style>
</head>
<body>
    <h2>Child's Live Location</h2>
    <p id="info">Waiting for data...</p>
    <div id="map"></div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="/socket.io/socket.io.js"></script>
    <script>
        const map = L.map('map').setView([20.5937, 78.9629], 5);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
        let marker = null;
        let pathLine = L.polyline([], { color: 'blue' }).addTo(map);

        fetch('/history').then(r => r.json()).then(history => {
            history.forEach(d => pathLine.addLatLng([d.latitude, d.longitude]));
        });

        const socket = io();
        socket.on('locationUpdate', (data) => {
            document.getElementById('info').innerText =
                'Latitude: ' + data.latitude + ', Longitude: ' + data.longitude;
            if (marker) {
                marker.setLatLng([data.latitude, data.longitude]);
            } else {
                marker = L.marker([data.latitude, data.longitude]).addTo(map);
            }
            pathLine.addLatLng([data.latitude, data.longitude]);
            map.setView([data.latitude, data.longitude], 13);
        });
    </script>
</body>
</html>
    `);
});

server.listen(3000, '0.0.0.0', () => {
    console.log('Server running on port 3000');
});