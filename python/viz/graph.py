# graph.py
from graphviz import Digraph
import os

# ---------------------------------------------------------------------
# Hard-coded RoadGraph sample (same structure as Scala RoadGraphBuilder)
# ---------------------------------------------------------------------

road_graph = {
    "nodes": {
        "1000": {"id": "1000", "lat": 25.204, "lon": 55.270, "outgoingEdges": ["10-f"]},
        "1001": {"id": "1001", "lat": 25.205, "lon": 55.271, "outgoingEdges": ["11-f"]},
        "1002": {"id": "1002", "lat": 25.206, "lon": 55.272, "outgoingEdges": []}
    },
    "edges": {
        "10-f": {
            "id": "10-f",
            "from": "1000",
            "to": "1001",
            "geometry": [(25.204, 55.270), (25.205, 55.271)],
            "tags": {"highway": "residential", "oneway": "true"}
        },
        "11-f": {
            "id": "11-f",
            "from": "1001",
            "to": "1002",
            "geometry": [(25.205, 55.271), (25.206, 55.272)],
            "tags": {"highway": "primary", "oneway": "false"}
        }
    }
}

# ---------------------------------------------------------------------
# Graphviz setup — vertical layout (TB = top-to-bottom)
# ---------------------------------------------------------------------

dot = Digraph("CityRover Road Graph", format="png")
dot.attr(rankdir="TB", splines="true", nodesep="0.6", ranksep="0.8")

# Node styling
dot.attr("node",
         shape="box",
         style="filled,rounded",
         fontname="Helvetica",
         fontsize="12",
         color="#4A90E2",
         fillcolor="#E8F1FB",
         margin="0.25")

# Edge styling
dot.attr("edge",
         fontname="Helvetica",
         fontsize="10",
         arrowsize="0.7")

# ---------------------------------------------------------------------
# Add nodes with HTML formatting
# ---------------------------------------------------------------------

for node_id, node in road_graph["nodes"].items():
    outgoing = ", ".join(node["outgoingEdges"]) if node["outgoingEdges"] else "-"
    label = f"""<
    <b><font color="#003366">Node {node_id}</font></b><br/>
    <i>lat:</i> {node['lat']}<br/>
    <i>lon:</i> {node['lon']}<br/>
    <i>outgoing:</i> {outgoing}
    >"""
    dot.node(node_id, label=label)

# ---------------------------------------------------------------------
# Add edges with HTML formatting + colors
# ---------------------------------------------------------------------

for edge_id, edge in road_graph["edges"].items():
    highway = edge["tags"].get("highway", "")
    oneway = edge["tags"].get("oneway", "")

    # Color by highway type
    if highway.startswith("motorway"):
        color = "#D0021B"   # red
    elif highway.startswith("primary"):
        color = "#4A90E2"   # blue
    elif highway.startswith("residential"):
        color = "#7B8A8B"   # gray
    else:
        color = "black"

    geom_str = " → ".join([f"({lat:.3f},{lon:.3f})" for lat, lon in edge["geometry"]])

    label = f"""<
    <b><font color="#003366">Edge {edge_id}</font></b><br/>
    <i>from:</i> {edge['from']}<br/>
    <i>to:</i> {edge['to']}<br/>
    <i>highway:</i> {highway}<br/>
    <i>oneway:</i> {oneway}<br/>
    <i>geometry:</i> {geom_str}
    >"""

    dot.edge(edge["from"], edge["to"], label=label, color=color)

# ---------------------------------------------------------------------
# Ensure /output directory exists
# ---------------------------------------------------------------------

os.makedirs("output", exist_ok=True)

# ---------------------------------------------------------------------
# Render diagram
# ---------------------------------------------------------------------

output_path = "output/cityrover_graph"
dot.render(output_path, cleanup=True)

print(f"Generated {output_path}.png")
