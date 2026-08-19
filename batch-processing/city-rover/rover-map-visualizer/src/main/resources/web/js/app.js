// ------------------------------------------------------------
// Global state
// ------------------------------------------------------------

let selectedRovers = new Set();


// ------------------------------------------------------------
// Load rover list from backend
// ------------------------------------------------------------

async function loadRovers() {

    console.log("loadRovers()");

    try {

        const res = await fetch('/api/rovers');

        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }

        const rovers = await res.json();

        console.log("Rovers received:", rovers);


        // ----------------------------------------------------
        // Numerical sorting
        // ----------------------------------------------------

        rovers.sort((a, b) => {

            const aNumber = parseInt(
                a.id.replace("rover-", ""),
                10
            );

            const bNumber = parseInt(
                b.id.replace("rover-", ""),
                10
            );

            return aNumber - bNumber;
        });


        console.log(
            "Sorted rovers:",
            rovers.map(rover => rover.id)
        );


        renderRoverList(rovers);

    } catch (err) {

        console.error(
            'Failed to load rovers:',
            err
        );
    }
}


// ------------------------------------------------------------
// Render rover list
// ------------------------------------------------------------

function renderRoverList(rovers) {

    const list =
        document.getElementById('rover-list');

    list.innerHTML = '';


    rovers.forEach(rover => {

        const button =
            document.createElement('button');

        button.type = 'button';

        button.className = 'rover-button';

        button.textContent = rover.id;

        button.dataset.roverId = rover.id;


        button.classList.toggle(
            'selected',
            selectedRovers.has(rover.id)
        );


        button.addEventListener('click', function () {

            const roverId =
                this.dataset.roverId;


            if (selectedRovers.has(roverId)) {

                selectedRovers.delete(roverId);

                this.classList.remove(
                    'selected'
                );

                this.setAttribute(
                    'aria-pressed',
                    'false'
                );

            } else {

                selectedRovers.add(roverId);

                this.classList.add(
                    'selected'
                );

                this.setAttribute(
                    'aria-pressed',
                    'true'
                );
            }


            console.log(
                'Selected rovers:',
                Array.from(selectedRovers)
            );
        });


        button.setAttribute(
            'aria-pressed',
            selectedRovers.has(rover.id)
                ? 'true'
                : 'false'
        );


        list.appendChild(button);
    });
}


// ------------------------------------------------------------
// Show ALL rover trajectories
// ------------------------------------------------------------

async function showAllTrajectories() {

    console.log(
        "========================================"
    );

    console.log(
        "SHOW ALL TRAJECTORIES CLICKED"
    );

    console.log(
        "========================================"
    );


    try {

        console.log(
            "Requesting /api/rovers ..."
        );


        const res =
            await fetch('/api/rovers');


        console.log(
            "Response received:",
            res.status
        );


        if (!res.ok) {

            throw new Error(
                `HTTP ${res.status}`
            );
        }


        const rovers =
            await res.json();


        console.log(
            "Number of rovers:",
            rovers.length
        );


        // ----------------------------------------------------
        // Numerical sorting
        // ----------------------------------------------------

        rovers.sort((a, b) => {

            const aNumber = parseInt(
                a.id.replace("rover-", ""),
                10
            );

            const bNumber = parseInt(
                b.id.replace("rover-", ""),
                10
            );

            return aNumber - bNumber;
        });


        // ----------------------------------------------------
        // Load trajectories
        // ----------------------------------------------------

        for (const rover of rovers) {

            console.log(
                "Loading trajectory:",
                rover.id
            );


            await showTrajectory(
                rover.id,
                false
            );
        }


        // ----------------------------------------------------
        // Fit map once
        // ----------------------------------------------------

        if (
            typeof trajectoryLayers !==
            'undefined'
        ) {

            const layers =
                Array.from(
                    trajectoryLayers.values()
                );


            console.log(
                "Trajectory layers:",
                layers.length
            );


            if (layers.length > 0) {

                const group =
                    L.featureGroup(layers);

                const bounds =
                    group.getBounds();


                if (bounds.isValid()) {

                    CityRoverMap.map.fitBounds(
                        bounds,
                        {
                            padding: [30, 30]
                        }
                    );
                }
            }
        }


        console.log(
            "ALL TRAJECTORIES LOADED"
        );

    } catch (err) {

        console.error(
            "Failed to show all trajectories:",
            err
        );
    }
}


// ------------------------------------------------------------
// Bind UI buttons
// ------------------------------------------------------------

function bindControls() {

    console.log(
        "bindControls()"
    );


    // --------------------------------------------------------
    // Show ALL trajectories
    // --------------------------------------------------------

    const showAllButton =
        document.getElementById(
            'btn-show-all-trajectories'
        );


    console.log(
        "Show All button:",
        showAllButton
    );


    if (!showAllButton) {

        console.error(
            "ERROR: #btn-show-all-trajectories not found!"
        );

    } else {

        showAllButton.addEventListener(
            'click',
            showAllTrajectories
        );

        console.log(
            "Show All Trajectories handler attached."
        );
    }


    // --------------------------------------------------------
    // Show selected trajectory
    // --------------------------------------------------------

    const showTrajectoryButton =
        document.getElementById(
            'btn-show-trajectory'
        );


    if (showTrajectoryButton) {

        showTrajectoryButton.onclick = () => {

            if (
                selectedRovers.size === 0
            ) {

                alert(
                    'Select at least one rover.'
                );

                return;
            }


            selectedRovers.forEach(
                roverId => {

                    showTrajectory(
                        roverId
                    );
                }
            );
        };
    }


    // --------------------------------------------------------
    // Start replay
    // --------------------------------------------------------

    const startReplayButton =
        document.getElementById(
            'btn-start-replay'
        );


    if (startReplayButton) {

        startReplayButton.onclick = () => {

            if (
                selectedRovers.size === 0
            ) {

                alert(
                    'Select at least one rover.'
                );

                return;
            }


            const first =
                [...selectedRovers][0];


            startReplay(first);
        };
    }


    // --------------------------------------------------------
    // Stop replay
    // --------------------------------------------------------

    const stopReplayButton =
        document.getElementById(
            'btn-stop-replay'
        );


    if (stopReplayButton) {

        stopReplayButton.onclick = () => {

            stopReplay();
        };
    }


    // --------------------------------------------------------
    // Show graph
    // --------------------------------------------------------

    const showGraphButton =
        document.getElementById(
            'btn-show-graph'
        );


    if (showGraphButton) {

        showGraphButton.onclick = () => {

            showGraph();
        };
    }
}


// ------------------------------------------------------------
// Initialize application
// ------------------------------------------------------------

window.onload = () => {

    console.log(
        "========================================"
    );

    console.log(
        "CITY ROVER APP INITIALIZING"
    );

    console.log(
        "========================================"
    );


    loadRovers();

    bindControls();

};
