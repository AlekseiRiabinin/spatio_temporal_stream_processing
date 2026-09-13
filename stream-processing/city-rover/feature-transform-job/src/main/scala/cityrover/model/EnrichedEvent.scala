package cityrover.model

import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * EnrichedEvent
  *
  * ML‑ready feature representation derived from raw TelemetryEvent.
  *
  * This version replaces raw temporal fields with rolling window
  * aggregates commonly used in online feature stores:
  *
  *   - 5‑second window
  *   - 30‑second window
  *   - 1‑minute window
  *   - 5‑minute window
  *
  * These windows capture short‑term and mid‑term dynamics of rover
  * movement, enabling real‑time ML models to react to behavior changes.
  */
final case class EnrichedEvent(

  // ============================================================
  // Identity / Keys
  // ============================================================
  roverId: String,
  edgeId: String,
  routeId: String,

  // ============================================================
  // Raw telemetry (copied from TelemetryEvent)
  // ============================================================
  lat: Double,
  lon: Double,
  ts: Long,
  speed: Double,
  heading: Double,

  // ============================================================
  // 5‑second window features (ultra‑short term)
  // ============================================================
  speedAvg5s: Double,
  speedMax5s: Double,
  speedMin5s: Double,
  accelerationAvg5s: Double,
  headingChange5s: Double,
  distanceTraveled5s: Double,

  // ============================================================
  // 30‑second window features (short term)
  // ============================================================
  speedAvg30s: Double,
  speedStd30s: Double,
  accelerationAvg30s: Double,
  jerkAvg30s: Double,
  turnRateAvg30s: Double,
  stopsCount30s: Int,
  distanceTraveled30s: Double,

  // ============================================================
  // 1‑minute window features (medium term)
  // ============================================================
  speedAvg1m: Double,
  speedStd1m: Double,
  accelerationAvg1m: Double,
  jerkAvg1m: Double,
  turnRateAvg1m: Double,
  idleRatio1m: Double,
  distanceTraveled1m: Double,
  congestionLevel1m: Double,

  // ============================================================
  // 5‑minute window features (longer term)
  // ============================================================
  speedAvg5m: Double,
  speedStd5m: Double,
  accelerationAvg5m: Double,
  jerkAvg5m: Double,
  turnRateAvg5m: Double,
  idleRatio5m: Double,
  distanceTraveled5m: Double,
  congestionLevel5m: Double,

  // ============================================================
  // Geospatial features
  // ============================================================
  gridCellId: Long,
  regionId: String,
  snappedEdgeId: String,
  snappedLat: Double,
  snappedLon: Double,

  // ============================================================
  // Route / navigation features
  // ============================================================
  routeProgress: Double,
  routeDeviation: Double,
  expectedSpeed: Double,
  speedRatio: Double,

  // ============================================================
  // Behavioral features
  // ============================================================
  drivingStyleScore: Double,
  anomalyScore: Double,

  // ============================================================
  // ML feature store metadata
  // ============================================================
  featureVersion: String,
  featureTimestamp: Long,
  featureLatencyMs: Long,

  // ============================================================
  // Quality / anomaly detection
  // ============================================================
  isOutlier: Boolean,
  isGpsJump: Boolean,
  isSpeedAnomaly: Boolean,
  isHeadingAnomaly: Boolean,

  // ============================================================
  // Debug / observability
  // ============================================================
  rawEventHash: String,
  processingNode: String,
  processingTimeMs: Long
)

/**
  * Convert Flink EnrichedEvent → Protobuf EnrichedTelemetryEvent
  *
  * This is used by the Kafka sink to serialize protobuf bytes.
  */
extension (e: EnrichedEvent)
  def toProtobuf: EnrichedTelemetryEvent =
    EnrichedTelemetryEvent(
      roverId = e.roverId,
      edgeId = e.edgeId,
      routeId = e.routeId,

      lat = e.lat,
      lon = e.lon,
      ts = e.ts,
      speed = e.speed,
      heading = e.heading,

      // 5s
      speedAvg5S = e.speedAvg5s,
      speedMax5S = e.speedMax5s,
      speedMin5S = e.speedMin5s,
      accelerationAvg5S = e.accelerationAvg5s,
      headingChange5S = e.headingChange5s,
      distanceTraveled5S = e.distanceTraveled5s,

      // 30s
      speedAvg30S = e.speedAvg30s,
      speedStd30S = e.speedStd30s,
      accelerationAvg30S = e.accelerationAvg30s,
      jerkAvg30S = e.jerkAvg30s,
      turnRateAvg30S = e.turnRateAvg30s,
      stopsCount30S = e.stopsCount30s,
      distanceTraveled30S = e.distanceTraveled30s,

      // 1m
      speedAvg1M = e.speedAvg1m,
      speedStd1M = e.speedStd1m,
      accelerationAvg1M = e.accelerationAvg1m,
      jerkAvg1M = e.jerkAvg1m,
      turnRateAvg1M = e.turnRateAvg1m,
      idleRatio1M = e.idleRatio1m,
      distanceTraveled1M = e.distanceTraveled1m,
      congestionLevel1M = e.congestionLevel1m,

      // 5m
      speedAvg5M = e.speedAvg5m,
      speedStd5M = e.speedStd5m,
      accelerationAvg5M = e.accelerationAvg5m,
      jerkAvg5M = e.jerkAvg5m,
      turnRateAvg5M = e.turnRateAvg5m,
      idleRatio5M = e.idleRatio5m,
      distanceTraveled5M = e.distanceTraveled5m,
      congestionLevel5M = e.congestionLevel5m,

      // geospatial
      gridCellId = e.gridCellId,
      regionId = e.regionId,
      snappedEdgeId = e.snappedEdgeId,
      snappedLat = e.snappedLat,
      snappedLon = e.snappedLon,

      // route
      routeProgress = e.routeProgress,
      routeDeviation = e.routeDeviation,
      expectedSpeed = e.expectedSpeed,
      speedRatio = e.speedRatio,

      // behavioral
      drivingStyleScore = e.drivingStyleScore,
      anomalyScore = e.anomalyScore,

      // metadata
      featureVersion = e.featureVersion,
      featureTimestamp = e.featureTimestamp,
      featureLatencyMs = e.featureLatencyMs,

      // quality
      isOutlier = e.isOutlier,
      isGpsJump = e.isGpsJump,
      isSpeedAnomaly = e.isSpeedAnomaly,
      isHeadingAnomaly = e.isHeadingAnomaly,

      // debug
      rawEventHash = e.rawEventHash,
      processingNode = e.processingNode,
      processingTimeMs = e.processingTimeMs
    )
