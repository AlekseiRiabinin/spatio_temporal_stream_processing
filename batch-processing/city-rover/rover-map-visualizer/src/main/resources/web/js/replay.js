// ------------------------------------------------------------
// Replay state (single-rover replay)
// ------------------------------------------------------------

let replayMarker = null;
let replayTimer = null;
let replayPositions = [];
let replayIndex = 0;

// ------------------------------------------------------------
// Start replay animation
// ------------------------------------------------------------

async function startReplay(roverId) {
    try {
        // Stop any existing replay
        stopReplay();

        const res = await fetch(`/api/replay/${roverId}`);
        const data = await res.json();

        replayPositions = data.positions || [];

        if (replayPositions.length === 0) {
            console.warn(`No replay positions available for rover ${roverId}.`);
            return;
        }

        // First position
        const first = replayPositions[0];

        replayMarker = L.circleMarker([first.lat, first.lon], {
            radius: 6,
            color: "#ff5722",
            fillColor: "#ff5722",
            fillOpacity: 1,
            className: "replay-marker"
        }).addTo(CityRoverMap.replayLayer);

        replayIndex = 0;

        // Center map on replay start
        CityRoverMap.map.setView([first.lat, first.lon], 15);

        // Start animation
        replayTimer = setInterval(stepReplay, 120);

    } catch (err) {
        console.error(`Failed to start replay for rover ${roverId}:`, err);
    }
}

// ------------------------------------------------------------
// Replay animation step
// ------------------------------------------------------------

function stepReplay() {
    if (!replayMarker || replayPositions.length === 0) return;

    replayIndex++;

    if (replayIndex >= replayPositions.length) {
        stopReplay();
        return;
    }

    const p = replayPositions[replayIndex];
    replayMarker.setLatLng([p.lat, p.lon]);
}

// ------------------------------------------------------------
// Stop replay animation
// ------------------------------------------------------------

function stopReplay() {
    if (replayTimer) {
        clearInterval(replayTimer);
        replayTimer = null;
    }

    CityRoverMap.clearReplayLayer();
    replayMarker = null;
    replayPositions = [];
    replayIndex = 0;
}
