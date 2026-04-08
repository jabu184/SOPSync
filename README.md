# SOPSync & Radiotherapy Status Dashboard

<img width="2495" height="1325" alt="image" src="https://github.com/user-attachments/assets/fd27c1f3-3795-4760-ae37-228b8a2b0c36" />

This repository contains two integrated applications designed for clinical and physics department use:

1. **SOPSync**: A Python Flask web application that automatically extracts metadata from standard operating procedure (SOP) PDFs, stores them in an SQLite database, and provides a dashboard to track upcoming document expirations.
2. **Radiotherapy Status**: A real-time dashboard built with React and Node.js (Socket.io) to track and broadcast the live status and faults of clinical machines (e.g., Linacs, CT Simulators).

---

## Features

### SOPSync (Python/Flask)
- **Automated PDF Extraction**: Uses `pdfplumber` to read tables and text from PDFs to intelligently extract titles, authors, keywords, and review dates.
- **Smart Sync**: Detects changes and only updates the database if new versions or review dates are found.
- **Dashboard & Export**: View documents approaching expiration, download archived backups, or export database views to CSV.

### Radiotherapy Status (React/Node.js)
- **Live Updates**: Instant status synchronization across all connected clients using WebSocket (`socket.io`).
- **Machine Tracking**: Monitor different states (Clinical, Service/QA, Switched On, Breakdown, Off).
- **Fault Logging**: Log specific machine faults and maintain a historical activity log.

---

## Prerequisites

- **Python 3.8+** (for SOPSync)
- **Node.js & npm** (for the Radiotherapy dashboard)

---

## Setup and Installation

### 1. SOPSync (Python Backend)

Install the required Python dependencies. You may want to use a virtual environment:
```bash
pip install flask pandas pdfplumber sqlite3
```

Run the Flask application:
```bash
python SOPSyncWebApp.py
```
The SOPSync web app will be available at `http://localhost:5000`.

### 2. Radiotherapy Status (Node.js/React)

First, install the Node dependencies:
```bash
npm install
```

Start the WebSocket and Database server:
```bash
node server.js
```
The socket server will run on `http://localhost:3001`. 

*(Note: If you have a Vite or Create React App frontend configured for `App.jsx`, run your respective dev command like `npm run dev` to launch the frontend, which will likely open on `http://localhost:5173`).*
