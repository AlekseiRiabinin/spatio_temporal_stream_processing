package cityrover.sim.telemetry

import cityrover.sim.model.{RoverState, TelemetryEvent}


/**
  * TelemetryGenerator converts RoverState (internal simulation state)
  * into TelemetryEvent (Kafka-ready JSON payload).
  *
  * Responsibilities:
  *   - ensure lat/lon are consistent with graph geometry (already done in RoverController)
  *   - ensure heading is normalized
  *   - produce a stable, Spark-friendly event schema
  */
class TelemetryGenerator {

  /** Convert RoverState → TelemetryEvent */
  def toTelemetry(state: RoverState, timestamp: Long): TelemetryEvent = {

    // Normalize heading to [0, 360)
    val headingNorm =
      if (state.heading < 0) state.heading + 360
      else if (state.heading >= 360) state.heading - 360
      else state.heading

    TelemetryEvent(
      roverId = state.roverId,
      ts = timestamp,
      lat = state.lat,
      lon = state.lon,
      speed = state.speedMps,
      heading = headingNorm,
      edgeId = state.edgeId,
      routeId = state.routeId
    )
  }
}
