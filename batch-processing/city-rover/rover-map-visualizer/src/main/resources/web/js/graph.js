// ------------------------------------------------------------
// Fetch and display road graph (nodes + edges)
// ------------------------------------------------------------

async function showGraph() {
    try {
        // Clear previous graph overlay
        CityRoverMap.clearGraphLayer();

        const res = await fetch("/api/graph");
        const graph = await res.json();

        const nodes = graph.nodes || [];
        const edges = graph.edges || [];

        if (nodes.length === 0 && edges.length === 0) {
            console.warn("Graph is empty.");
            return;
        }

        // Build node lookup by id
        const nodeById = new Map();
        for (const n of nodes) {
            nodeById.set(n.id, n);

            // Render node as small circle
            L.circleMarker([n.lat, n.lon], {
                radius: 3,
                color: "#2196f3",
                fillColor: "#2196f3",
                fillOpacity: 0.9
            }).addTo(CityRoverMap.graphLayer);
        }

        // Render edges as polylines
        for (const e of edges) {
            const from = nodeById.get(e.from);
            const to = nodeById.get(e.to);
            if (!from || !to) continue;

            L.polyline(
                [
                    [from.lat, from.lon],
                    [to.lat, to.lon]
                ],
                {
                    color: "#4caf50",
                    weight: 2,
                    opacity: 0.7
                }
            ).addTo(CityRoverMap.graphLayer);
        }

        // Fit map to graph bounds if possible
        try {
            const bounds = CityRoverMap.graphLayer.getBounds();
            if (bounds && bounds.isValid()) {
                CityRoverMap.map.fitBounds(bounds, { padding: [20, 20] });
            }
        } catch (err) {
            console.warn("Could not fit map to graph bounds:", err);
        }

    } catch (err) {
        console.error("Failed to load graph:", err);
    }
}
