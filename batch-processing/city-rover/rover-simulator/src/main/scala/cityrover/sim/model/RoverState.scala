package cityrover.sim.model


/**
  * Internal simulation state of a rover at a given tick.
  * This is NOT sent to Kafka directly — it is converted into TelemetryEvent.
  */
case class RoverState(
  roverId: String,
  edgeId: String,
  positionOnEdge: Double,   // 0.0–1.0 normalized position along the edge
  speedMps: Double,         // meters per second
  heading: Double,          // degrees (0–360)
  lat: Double,              // computed from graph geometry
  lon: Double,              // computed from graph geometry
  routeId: String,          // identifier for the route the rover follows
  timestamp: Long           // simulation timestamp
)
