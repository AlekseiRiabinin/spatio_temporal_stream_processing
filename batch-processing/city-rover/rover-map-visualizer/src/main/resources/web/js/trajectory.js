// ------------------------------------------------------------
// Fetch and display rover trajectory
// ------------------------------------------------------------

async function showTrajectory(roverId) {
    try {
        // Clear previous trajectory
        CityRoverMap.clearTrajectoryLayer();

        const res = await fetch(`/api/rovers/${roverId}/trajectory`);
        const data = await res.json();

        if (!data || !data.geojson) {
            console.error("Invalid trajectory data:", data);
            return;
        }

        // Render GeoJSON LineString
        const layer = L.geoJSON(data.geojson, {
            style: {
                color: "#ff0000",
                weight: 4,
                opacity: 0.8
            }
        });

        layer.addTo(CityRoverMap.trajectoryLayer);

        // Auto-fit map to trajectory bounds
        try {
            const bounds = layer.getBounds();
            if (bounds.isValid()) {
                CityRoverMap.map.fitBounds(bounds, { padding: [20, 20] });
            }
        } catch (err) {
            console.warn("Could not fit map to trajectory:", err);
        }

    } catch (err) {
        console.error(`Failed to load trajectory for rover ${roverId}:`, err);
    }
}
