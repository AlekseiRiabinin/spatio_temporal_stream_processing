# rtree_actual.py
from graphviz import Digraph
import os

# ---------------------------------------------------------------------
# Actual-like edge data (your synthetic but realistic geometry)
# ---------------------------------------------------------------------

edges = {
    "10-f": {
        "geometry": [(25.204, 55.270), (25.205, 55.271)],
        "tags": {"highway": "residential"}
    },
    "11-f": {
        "geometry": [(25.205, 55.271), (25.206, 55.272)],
        "tags": {"highway": "primary"}
    },
    "20-f": {
        "geometry": [(25.210, 55.280), (25.211, 55.281)],
        "tags": {"highway": "residential"}
    },
    "21-f": {
        "geometry": [(25.211, 55.281), (25.212, 55.282)],
        "tags": {"highway": "primary"}
    }
}

# ---------------------------------------------------------------------
# Compute envelopes (minX, maxX, minY, maxY)
# ---------------------------------------------------------------------

def compute_envelope(geometry):
    lats = [lat for lat, lon in geometry]
    lons = [lon for lat, lon in geometry]
    return {
        "minX": min(lons),
        "maxX": max(lons),
        "minY": min(lats),
        "maxY": max(lats)
    }

envelopes = {eid: compute_envelope(e["geometry"]) for eid, e in edges.items()}

# ---------------------------------------------------------------------
# Build a simple R-tree hierarchy (2 internal nodes + root)
# ---------------------------------------------------------------------
# Group edges spatially:
#   Group 1: 10-f, 11-f  (close together)
#   Group 2: 20-f, 21-f  (close together)
#
# Internal nodes B and C cover each group.
# Root A covers both.

rtree = {
    "A": {
        "children": ["B", "C"],
        "bbox": "Root Envelope A"
    },
    "B": {
        "children": ["10-f", "11-f"],
        "bbox": "Envelope B (cluster 1)"
    },
    "C": {
        "children": ["20-f", "21-f"],
        "bbox": "Envelope C (cluster 2)"
    }
}

# ---------------------------------------------------------------------
# Graphviz setup — vertical tree layout
# ---------------------------------------------------------------------

dot = Digraph("R-tree", format="png")
dot.attr(rankdir="TB", splines="true", nodesep="0.7", ranksep="0.9")

dot.attr("node",
         shape="box",
         style="filled,rounded",
         fontname="Helvetica",
         fontsize="12",
         color="#1F618D",
         fillcolor="#EBF5FB",
         margin="0.3")

dot.attr("edge",
         fontname="Helvetica",
         fontsize="10",
         arrowsize="0.8",
         color="#34495E")

# ---------------------------------------------------------------------
# Add internal nodes
# ---------------------------------------------------------------------

for node_id, node in rtree.items():
    label = f"""<
    <b><font color="#154360">{node_id}</font></b><br/>
    <i>{node['bbox']}</i>
    >"""
    dot.node(node_id, label=label)

# ---------------------------------------------------------------------
# Add leaf nodes (actual edges with envelopes)
# ---------------------------------------------------------------------

for eid, env in envelopes.items():
    label = f"""<
    <b><font color="#154360">{eid}</font></b><br/>
    <i>Envelope:</i><br/>
    minX={env['minX']:.3f}<br/>
    maxX={env['maxX']:.3f}<br/>
    minY={env['minY']:.3f}<br/>
    maxY={env['maxY']:.3f}
    >"""
    dot.node(eid, label=label)

# ---------------------------------------------------------------------
# Add edges (parent → children)
# ---------------------------------------------------------------------

for node_id, node in rtree.items():
    for child in node["children"]:
        dot.edge(node_id, child)

# ---------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------

os.makedirs("output", exist_ok=True)

# ---------------------------------------------------------------------
# Render diagram
# ---------------------------------------------------------------------

output_path = "output/rtree_actual"
dot.render(output_path, cleanup=True)

print(f"Generated {output_path}.png")
