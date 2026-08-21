# route.py
from graphviz import Digraph
import os

# ---------------------------------------------------------------------
# Graphviz setup — vertical layout (TB = top-to-bottom)
# ---------------------------------------------------------------------

dot = Digraph("Dijkstra Shortest Path", format="png")
dot.attr(rankdir="TB", splines="true", nodesep="0.7", ranksep="0.9")

# Node styling
dot.attr("node",
         shape="box",
         style="rounded,filled",
         fontname="Helvetica",
         fontsize="12",
         color="#1F618D",
         fillcolor="#EBF5FB",
         margin="0.3")

# Edge styling
dot.attr("edge",
         fontname="Helvetica",
         fontsize="10",
         arrowsize="0.8",
         color="#34495E")

# ---------------------------------------------------------------------
# Nodes (NO HTML labels — Graphviz-safe)
# ---------------------------------------------------------------------

dot.node("adj",
         "Build adjacency list\n"
         "node → (neighbor, edgeId, weight)\n"
         "weight = travel time")

dot.node("init",
         "Initialize\n"
         "dist(start)=0\n"
         "dist(other)=∞\n"
         "prev = empty\n"
         "pq.enqueue(start)")

dot.node("pop",
         "Pop smallest-distance node\n"
         "from priority queue")

dot.node("relax",
         "Relax edges\n"
         "alt = dist(node) + weight\n"
         "if alt < dist(nbr): update\n"
         "prev(nbr) = edgeId\n"
         "pq.enqueue(nbr)")

dot.node("check",
         "Check goal\n"
         "if node == end:\n"
         "stop and reconstruct path")

dot.node("reconstruct",
         "Reconstruct shortest path\n"
         "follow prev(edgeId)\n"
         "return Seq(edgeIds)")

# ---------------------------------------------------------------------
# Edges (flow)
# ---------------------------------------------------------------------

dot.edge("adj", "init", label="prepare graph")
dot.edge("init", "pop", label="start search")
dot.edge("pop", "relax", label="for each neighbor")
dot.edge("relax", "check", label="update distances")
dot.edge("check", "pop", label="continue search")
dot.edge("check", "reconstruct", label="end reached")

# ---------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------

os.makedirs("output", exist_ok=True)

# ---------------------------------------------------------------------
# Render diagram
# ---------------------------------------------------------------------

output_path = "output/route_planning"
dot.render(output_path, cleanup=True)

print(f"Generated {output_path}.png")
