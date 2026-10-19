package cityrover.windows

import java.net.InetAddress

import cityrover.model.{FeatureEnvelope, TelemetryEvent}
import cityrover.util.GeoUtils.{computeGridCell, computeRegion}


/** Builds the shared `FeatureEnvelope` for a window firing from the
  * window's materialized events.
  *
  * Every `Window*Function` calls this once per firing, then wraps the
  * result with its own `Window*Payload` to produce an `EnrichedEvent*`.
  *
  * `windowEndTs` MUST be the event-time window boundary in ms
  * (`ctx.window.getEnd`), not wall clock and not firing time.
  */
object FeatureEnvelopeBuilder:

  @transient private lazy val hostname: String =
    try InetAddress.getLocalHost.getHostName
    catch case _: Throwable => "unknown"

  def build(
    events: java.util.List[TelemetryEvent],
    windowEndTs: Long,
    processingStartMs: Long
  ): FeatureEnvelope =
    val last      = events.get(events.size() - 1)
    val nowMs     = System.currentTimeMillis()
    val processingTimeMs = nowMs - processingStartMs

    FeatureEnvelope(
      // identity
      roverId = last.roverId,
      edgeId  = last.edgeId,
      routeId = last.routeId,

      // raw telemetry snapshot
      lat     = last.lat,
      lon     = last.lon,
      ts      = last.ts,
      speed   = last.speed,
      heading = last.heading,

      // envelope / discriminator
      windowEndTs = windowEndTs,

      // geospatial — computed from the latest event in the window
      gridCellId    = computeGridCell(last.lat, last.lon),
      regionId      = computeRegion(last.lat, last.lon),
      snappedEdgeId = last.edgeId,
      snappedLat    = last.lat,
      snappedLon    = last.lon,

      // route — not implemented yet in the current functions; keep 0.0
      routeProgress  = 0.0,
      routeDeviation = 0.0,
      expectedSpeed  = 0.0,
      speedRatio     = 0.0,

      // behavioral — not implemented yet
      drivingStyleScore = 0.0,
      anomalyScore      = 0.0,

      // ML metadata
      featureVersion   = "v1",
      featureTimestamp = windowEndTs,       // event-time, replay-stable
      featureLatencyMs = nowMs - last.ts,   // wall-clock processing latency

      // quality — not implemented yet
      isOutlier        = false,
      isGpsJump        = false,
      isSpeedAnomaly   = false,
      isHeadingAnomaly = false,

      // debug
      rawEventHash     = last.hashCode().toHexString,
      processingNode   = hostname,
      processingTimeMs = processingTimeMs
    )
