// ------------------------------------------------------------
// Multi-rover trajectory layers
// ------------------------------------------------------------

// roverId -> Leaflet layer
const trajectoryLayers = new Map();


// ------------------------------------------------------------
// Deterministic color palette
// ------------------------------------------------------------

const COLORS = [
    "#ff0000",
    "#007bff",
    "#28a745",
    "#ff8c00",
    "#800080",
    "#00bcd4",
    "#795548",
    "#ff1493"
];


function colorFor(roverId) {

    let hash = 0;

    for (let i = 0; i < roverId.length; i++) {
        hash = (hash * 31 + roverId.charCodeAt(i)) & 0xffffffff;
    }

    return COLORS[Math.abs(hash) % COLORS.length];
}


// ------------------------------------------------------------
// Show rover trajectory
// ------------------------------------------------------------

async function showTrajectory(roverId) {

    try {

        // If this rover already has a trajectory,
        // remove the old layer before drawing a fresh one.
        if (trajectoryLayers.has(roverId)) {

            const oldLayer = trajectoryLayers.get(roverId);

            CityRoverMap.map.removeLayer(oldLayer);

            trajectoryLayers.delete(roverId);
        }


        // ----------------------------------------------------
        // Request trajectory from backend
        // ----------------------------------------------------

        const res = await fetch(`/api/trajectory/${roverId}`);

        if (!res.ok) {
            throw new Error(
                `HTTP ${res.status} while loading ${roverId}`
            );
        }

        const data = await res.json();


        if (!data || !data.geojson) {

            console.error(
                `Invalid trajectory data for ${roverId}:`,
                data
            );

            return;
        }


        // ----------------------------------------------------
        // Create GeoJSON layer
        // ----------------------------------------------------

        const layer = L.geoJSON(data.geojson, {

            style: {
                color: colorFor(roverId),
                weight: 4,
                opacity: 0.85
            }

        });


        // ----------------------------------------------------
        // Rover ID label
        // ----------------------------------------------------

        layer.bindTooltip(roverId, {

            permanent: true,

            direction: "center",

            className: "trajectory-label"

        });


        // ----------------------------------------------------
        // Add trajectory to map
        // ----------------------------------------------------

        layer.addTo(CityRoverMap.map);


        // ----------------------------------------------------
        // Store layer
        // ----------------------------------------------------

        trajectoryLayers.set(
            roverId,
            layer
        );


        // ----------------------------------------------------
        // Fit map to trajectory
        // ----------------------------------------------------

        try {

            const bounds = layer.getBounds();

            if (bounds.isValid()) {

                CityRoverMap.map.fitBounds(
                    bounds,
                    {
                        padding: [20, 20]
                    }
                );
            }

        } catch (err) {

            console.warn(
                `Could not fit map to ${roverId}:`,
                err
            );
        }


    } catch (err) {

        console.error(
            `Failed to load trajectory for ${roverId}:`,
            err
        );
    }
}
