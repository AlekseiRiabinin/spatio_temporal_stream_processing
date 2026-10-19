package cityrover.model

import cityrover.telemetry.{
  EnrichedTelemetryEvent,
  WindowType,
  Window5sFeatures  => Proto5sFeatures,
  Window30sFeatures => Proto30sFeatures,
  Window1mFeatures  => Proto1mFeatures,
  Window5mFeatures  => Proto5mFeatures
}

// ============================================================
// Shared envelope — populated by every window firing
// ============================================================

/** Fields common to every window firing.
  *
  * `windowEndTs` is the EVENT-TIME window boundary in MILLISECONDS
  * since epoch (replay-stable; not firing time, not wall clock).
  * The writer multiplies by 1000 for Cassandra USING TIMESTAMP (µs).
  */
final case class FeatureEnvelope(
  // identity
  roverId: String,
  edgeId: String,
  routeId: String,

  // raw telemetry snapshot (latest event in the window)
  lat: Double,
  lon: Double,
  ts: Long,
  speed: Double,
  heading: Double,

  // envelope / discriminator
  windowEndTs: Long,

  // geospatial
  gridCellId: Long,
  regionId: String,
  snappedEdgeId: String,
  snappedLat: Double,
  snappedLon: Double,

  // route / navigation
  routeProgress: Double,
  routeDeviation: Double,
  expectedSpeed: Double,
  speedRatio: Double,

  // behavioral
  drivingStyleScore: Double,
  anomalyScore: Double,

  // ML metadata
  featureVersion: String,
  featureTimestamp: Long,
  featureLatencyMs: Long,

  // quality / anomaly
  isOutlier: Boolean,
  isGpsJump: Boolean,
  isSpeedAnomaly: Boolean,
  isHeadingAnomaly: Boolean,

  // debug / observability
  rawEventHash: String,
  processingNode: String,
  processingTimeMs: Long
)

// ============================================================
// Per-window payloads
// ============================================================

final case class Window5sPayload(
  speedAvg5s: Double,
  speedMax5s: Double,
  speedMin5s: Double,
  accelerationAvg5s: Double,
  headingChange5s: Double,
  distanceTraveled5s: Double
)

final case class Window30sPayload(
  speedAvg30s: Double,
  speedStd30s: Double,
  accelerationAvg30s: Double,
  jerkAvg30s: Double,
  turnRateAvg30s: Double,
  stopsCount30s: Int,
  distanceTraveled30s: Double
)

final case class Window1mPayload(
  speedAvg1m: Double,
  speedStd1m: Double,
  accelerationAvg1m: Double,
  jerkAvg1m: Double,
  turnRateAvg1m: Double,
  idleRatio1m: Double,
  distanceTraveled1m: Double,
  congestionLevel1m: Double
)

final case class Window5mPayload(
  speedAvg5m: Double,
  speedStd5m: Double,
  accelerationAvg5m: Double,
  jerkAvg5m: Double,
  turnRateAvg5m: Double,
  idleRatio5m: Double,
  distanceTraveled5m: Double,
  congestionLevel5m: Double
)

// ============================================================
// Partial enriched event — exactly one window variant
// ============================================================

sealed trait EnrichedEvent {
  def envelope: FeatureEnvelope
  def windowType: WindowType
}

final case class EnrichedEvent5s(
  envelope: FeatureEnvelope,
  features: Window5sPayload
) extends EnrichedEvent {
  val windowType: WindowType = WindowType.WINDOW_5S
}

final case class EnrichedEvent30s(
  envelope: FeatureEnvelope,
  features: Window30sPayload
) extends EnrichedEvent {
  val windowType: WindowType = WindowType.WINDOW_30S
}

final case class EnrichedEvent1m(
  envelope: FeatureEnvelope,
  features: Window1mPayload
) extends EnrichedEvent {
  val windowType: WindowType = WindowType.WINDOW_1M
}

final case class EnrichedEvent5m(
  envelope: FeatureEnvelope,
  features: Window5mPayload
) extends EnrichedEvent {
  val windowType: WindowType = WindowType.WINDOW_5M
}

// ============================================================
// Conversion to protobuf
// ============================================================

extension (e: EnrichedEvent)
  def toProtobuf: EnrichedTelemetryEvent =
    val protoFeatures: EnrichedTelemetryEvent.Features = e match
      case e5s: EnrichedEvent5s =>
        EnrichedTelemetryEvent.Features.W5S(
          Proto5sFeatures(
            speedAvg5S         = e5s.features.speedAvg5s,
            speedMax5S         = e5s.features.speedMax5s,
            speedMin5S         = e5s.features.speedMin5s,
            accelerationAvg5S  = e5s.features.accelerationAvg5s,
            headingChange5S    = e5s.features.headingChange5s,
            distanceTraveled5S = e5s.features.distanceTraveled5s,
            updatedAt5S        = e.envelope.windowEndTs
          )
        )
      case e30s: EnrichedEvent30s =>
        EnrichedTelemetryEvent.Features.W30S(
          Proto30sFeatures(
            speedAvg30S         = e30s.features.speedAvg30s,
            speedStd30S         = e30s.features.speedStd30s,
            accelerationAvg30S  = e30s.features.accelerationAvg30s,
            jerkAvg30S          = e30s.features.jerkAvg30s,
            turnRateAvg30S      = e30s.features.turnRateAvg30s,
            stopsCount30S       = e30s.features.stopsCount30s,
            distanceTraveled30S = e30s.features.distanceTraveled30s,
            updatedAt30S        = e.envelope.windowEndTs
          )
        )
      case e1m: EnrichedEvent1m =>
        EnrichedTelemetryEvent.Features.W1M(
          Proto1mFeatures(
            speedAvg1M         = e1m.features.speedAvg1m,
            speedStd1M         = e1m.features.speedStd1m,
            accelerationAvg1M  = e1m.features.accelerationAvg1m,
            jerkAvg1M          = e1m.features.jerkAvg1m,
            turnRateAvg1M      = e1m.features.turnRateAvg1m,
            idleRatio1M        = e1m.features.idleRatio1m,
            distanceTraveled1M = e1m.features.distanceTraveled1m,
            congestionLevel1M  = e1m.features.congestionLevel1m,
            updatedAt1M        = e.envelope.windowEndTs
          )
        )
      case e5m: EnrichedEvent5m =>
        EnrichedTelemetryEvent.Features.W5M(
          Proto5mFeatures(
            speedAvg5M         = e5m.features.speedAvg5m,
            speedStd5M         = e5m.features.speedStd5m,
            accelerationAvg5M  = e5m.features.accelerationAvg5m,
            jerkAvg5M          = e5m.features.jerkAvg5m,
            turnRateAvg5M      = e5m.features.turnRateAvg5m,
            idleRatio5M        = e5m.features.idleRatio5m,
            distanceTraveled5M = e5m.features.distanceTraveled5m,
            congestionLevel5M  = e5m.features.congestionLevel5m,
            updatedAt5M        = e.envelope.windowEndTs
          )
        )

    EnrichedTelemetryEvent(
      windowType  = e.windowType,
      windowEndTs = e.envelope.windowEndTs,

      // identity
      roverId = e.envelope.roverId,
      edgeId  = e.envelope.edgeId,
      routeId = e.envelope.routeId,

      // raw snapshot
      lat     = e.envelope.lat,
      lon     = e.envelope.lon,
      ts      = e.envelope.ts,
      speed   = e.envelope.speed,
      heading = e.envelope.heading,

      // oneof — exactly one variant set above
      features = protoFeatures,

      // geospatial
      gridCellId    = e.envelope.gridCellId,
      regionId      = e.envelope.regionId,
      snappedEdgeId = e.envelope.snappedEdgeId,
      snappedLat    = e.envelope.snappedLat,
      snappedLon    = e.envelope.snappedLon,

      // route
      routeProgress  = e.envelope.routeProgress,
      routeDeviation = e.envelope.routeDeviation,
      expectedSpeed  = e.envelope.expectedSpeed,
      speedRatio     = e.envelope.speedRatio,

      // behavioral
      drivingStyleScore = e.envelope.drivingStyleScore,
      anomalyScore      = e.envelope.anomalyScore,

      // metadata
      featureVersion   = e.envelope.featureVersion,
      featureTimestamp = e.envelope.featureTimestamp,
      featureLatencyMs = e.envelope.featureLatencyMs,

      // quality
      isOutlier        = e.envelope.isOutlier,
      isGpsJump        = e.envelope.isGpsJump,
      isSpeedAnomaly   = e.envelope.isSpeedAnomaly,
      isHeadingAnomaly = e.envelope.isHeadingAnomaly,

      // debug
      rawEventHash     = e.envelope.rawEventHash,
      processingNode   = e.envelope.processingNode,
      processingTimeMs = e.envelope.processingTimeMs
    )
