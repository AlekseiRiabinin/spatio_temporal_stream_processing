// ------------------------------------------------------------
// Global state
// ------------------------------------------------------------

let selectedRoverId = null;

// ------------------------------------------------------------
// Load rover list from backend
// ------------------------------------------------------------

async function loadRovers() {
    try {
        const res = await fetch('/api/rovers');
        const rovers = await res.json();
        renderRoverList(rovers);
    } catch (err) {
        console.error('Failed to load rovers:', err);
    }
}

// ------------------------------------------------------------
// Render rover list in sidebar
// ------------------------------------------------------------

function renderRoverList(rovers) {
    const list = document.getElementById('rover-list');
    list.innerHTML = '';

    rovers.forEach(rover => {
        const item = document.createElement('div');
        item.className = 'rover-item';
        item.textContent = rover.id;

        item.onclick = () => {
            selectedRoverId = rover.id;
            highlightSelectedRover(item);
        };

        list.appendChild(item);
    });
}

// ------------------------------------------------------------
// Highlight selected rover in sidebar
// ------------------------------------------------------------

function highlightSelectedRover(selectedItem) {
    document.querySelectorAll('.rover-item').forEach(item => {
        item.style.background = '#eee';
    });

    selectedItem.style.background = '#d0d0d0';
}

// ------------------------------------------------------------
// Bind UI buttons
// ------------------------------------------------------------

function bindControls() {
    document.getElementById('btn-show-trajectory').onclick = () => {
        if (!selectedRoverId) {
            alert('Select a rover first.');
            return;
        }
        showTrajectory(selectedRoverId); // from trajectory.js
    };

    document.getElementById('btn-start-replay').onclick = () => {
        if (!selectedRoverId) {
            alert('Select a rover first.');
            return;
        }
        startReplay(selectedRoverId); // from replay.js
    };

    document.getElementById('btn-stop-replay').onclick = () => {
        stopReplay(); // from replay.js
    };

    document.getElementById('btn-show-graph').onclick = () => {
        showGraph(); // from graph.js
    };
}

// ------------------------------------------------------------
// Initialize application
// ------------------------------------------------------------

window.onload = () => {
    loadRovers();
    bindControls();
};
