# kafka_producer_graph.py
from graphviz import Digraph
import os

# ---------------------------------------------------------------------
# Graphviz setup — vertical layout (TB = top-to-bottom)
# ---------------------------------------------------------------------

dot = Digraph("Kafka Telemetry Producer", format="png")
dot.attr(rankdir="TB", splines="true", nodesep="0.7", ranksep="0.9")

# Node styling
dot.attr("node",
         shape="box",
         style="filled,rounded",
         fontname="Helvetica",
         fontsize="12",
         color="#2C3E50",
         fillcolor="#ECF0F1",
         margin="0.3")

# Edge styling
dot.attr("edge",
         fontname="Helvetica",
         fontsize="10",
         arrowsize="0.8",
         color="#34495E")

# ---------------------------------------------------------------------
# Nodes (each step in the telemetry → Kafka pipeline)
# ---------------------------------------------------------------------

dot.node("telemetry", """<
<b><font color="#1A5276">TelemetryEvent</font></b><br/>
<i>roverId, timestamp, lat, lon, speed, battery...</i>
>""")

dot.node("json", """<
<b><font color="#1A5276">JSON Serialization</font></b><br/>
<i>event.asJson.noSpaces</i>
>""")

dot.node("record", """<
<b><font color="#1A5276">ProducerRecord</font></b><br/>
<i>(topic, roverId, json)</i>
>""")

dot.node("producer", """<
<b><font color="#1A5276">KafkaTelemetryProducer</font></b><br/>
<i>enable.idempotence=true</i><br/>
<i>acks=all</i><br/>
<i>compression=lz4</i><br/>
<i>linger.ms=5</i><br/>
<i>batch.size=32768</i><br/>
<i>max.in.flight=1</i>
>""")

dot.node("topic", """<
<b><font color="#1A5276">Kafka Topic</font></b><br/>
<i>telemetry-stream</i>
>""")

dot.node("stream", """<
<b><font color="#1A5276">Telemetry Stream</font></b><br/>
<i>continuous JSON messages</i>
>""")

# ---------------------------------------------------------------------
# Edges (flow)
# ---------------------------------------------------------------------

dot.edge("telemetry", "json", label="serialize")
dot.edge("json", "record", label="wrap into record")
dot.edge("record", "producer", label="producer.send(record)")
dot.edge("producer", "topic", label="flush → low latency")
dot.edge("topic", "stream", label="append to log")

# ---------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------

os.makedirs("output", exist_ok=True)

# ---------------------------------------------------------------------
# Render diagram
# ---------------------------------------------------------------------

output_path = "output/kafka_producer"
dot.render(output_path, cleanup=True)

print(f"Generated {output_path}.png")
