const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "http://localhost:5173", // Default port for Vite React app
    methods: ["GET", "POST"]
  }
});

// Initialize SQLite Database
const db = new sqlite3.Database('./radiotherapy.db', (err) => {
  if (err) console.error(err.message);
  console.log('Connected to the SQLite database.');
});

// Create our logs table if it doesn't exist
db.run(`CREATE TABLE IF NOT EXISTS machine_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_name TEXT,
    status TEXT,
    fault_note TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)`, () => {
  // Safely attempt to add the new column if the table already existed before this update
  db.run(`ALTER TABLE machine_status ADD COLUMN fault_note TEXT`, () => {});
});

io.on('connection', (socket) => {
  console.log('A user connected:', socket.id);

  // When a user connects, send them the last 50 status logs
  db.all(`SELECT * FROM machine_status ORDER BY timestamp DESC LIMIT 50`, [], (err, rows) => {
    if (!err) socket.emit('initial_data', rows);
  });

  // Send the absolute latest status for every machine to populate the summary board
  db.all(`SELECT machine_name, status FROM machine_status WHERE id IN (SELECT MAX(id) FROM machine_status GROUP BY machine_name)`, [], (err, rows) => {
    if (!err) {
      const latest = {};
      rows.forEach(r => latest[r.machine_name] = r.status);
      socket.emit('latest_statuses', latest);
    }
  });

  // Listen for a status update from a user
  socket.on('update_status', (data) => {
    const { machine_name, status, fault_note } = data;
    
    db.run(`INSERT INTO machine_status (machine_name, status, fault_note) VALUES (?, ?, ?)`, [machine_name, status, fault_note || null], function(err) {
        if (!err) {
          const newRecord = { id: this.lastID, machine_name, status, fault_note: fault_note || null, timestamp: new Date().toISOString() };
          // Broadcast the new status to EVERY connected user instantly
          io.emit('status_updated', newRecord);
        }
    });
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Live Status Server running on port ${PORT}`);
});