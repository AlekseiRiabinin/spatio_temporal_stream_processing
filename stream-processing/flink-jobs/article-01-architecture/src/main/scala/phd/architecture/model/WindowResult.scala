package phd.architecture.model

import org.locationtech.jts.geom.Geometry

/**
 * Result of windowed aggregation:
 *
 * r = { p, [t_start, t_end), v }
 *
 * p        : spatial partition
 * t_start  : window start (event-time)
 * t_end    : window end (event-time)
 * v        : aggregated value
 * _        : processing time (optional)
 */
case class WindowResult(
  partition: SpatialPartition,
  windowStart: Long,
  windowEnd: Long,
  value: Long,
  processingTime: Option[Long] = None
)
