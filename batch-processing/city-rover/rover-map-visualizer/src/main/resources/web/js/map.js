// ------------------------------------------------------------
// Leaflet map initialization
// ------------------------------------------------------------

let map = L.map('map', {
    zoomControl: true
}).setView([25.2048, 55.2708], 12); // Dubai default view

// ------------------------------------------------------------
// Base tile layer (OpenStreetMap)
// ------------------------------------------------------------

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// ------------------------------------------------------------
// Layer groups for different visualizations
// ------------------------------------------------------------

const trajectoryLayer = L.layerGroup().addTo(map);
const replayLayer = L.layerGroup().addTo(map);
const graphLayer = L.layerGroup().addTo(map);

// ------------------------------------------------------------
// Utility functions for other modules
// ------------------------------------------------------------

// ❗ IMPORTANT: Do NOT clear all trajectories globally anymore.
// Multi‑rover trajectories are managed individually inside trajectory.js.

function clearTrajectoryLayer() {
    // Keep this function for compatibility, but do NOT use it for multi‑rover mode.
    // It now only clears the group if someone explicitly calls it.
    trajectoryLayer.clearLayers();
}

function clearReplayLayer() {
    replayLayer.clearLayers();
}

function clearGraphLayer() {
    graphLayer.clearLayers();
}

// ------------------------------------------------------------
// Export layer groups for other modules
// ------------------------------------------------------------

window.CityRoverMap = {
    map,
    trajectoryLayer,
    replayLayer,
    graphLayer,
    clearTrajectoryLayer,
    clearReplayLayer,
    clearGraphLayer
};
