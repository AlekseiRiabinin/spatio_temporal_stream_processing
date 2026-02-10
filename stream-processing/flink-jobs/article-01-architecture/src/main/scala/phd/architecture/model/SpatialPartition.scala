package phd.architecture.model


/**
 * Logical spatial partition used as Flink key.
 *
 * @param geohash geohash string at a given precision
 * @param precision geohash precision (for traceability / experiments)
 */
final case class SpatialPartition(
  geohash: String,
  precision: Int
)
