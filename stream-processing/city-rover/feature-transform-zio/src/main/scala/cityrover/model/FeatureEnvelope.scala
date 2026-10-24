package cityrover.model


/** Fields common to every window firing.
  *
  * The four window accumulators each emit one `EnrichedEvent*` per firing,
  * and every firing wraps its window-specific payload in a `FeatureEnvelope`
  * built by `FeatureEnvelopeBuilder`. This is what lets the wire format
  * carry a single "partial record" per window instead of a wide flat row
  * with zeros for the other three windows.
  *
  * `windowEndTs` is the EVENT-TIME window boundary in MILLISECONDS
  * since epoch (replay-stable; not firing time, not wall clock). The
  * writer job multiplies by 1000 for Cassandra USING TIMESTAMP (µs).
  *
  * `featureTimestamp` mirrors `windowEndTs` so downstream consumers can
  * read the freshness of the row without knowing about windowEndTs.
  * `processingTimeMs` is the transform duration (wall clock), not a
  * timestamp — it's kept for observability and is allowed to differ
  * between replays.
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
