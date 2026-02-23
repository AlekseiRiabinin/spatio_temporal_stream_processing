package phd.streammodels.model


/**
 * Result of windowed aggregation:
 *
 * r = { p, [t_start, t_end), v }
 *
 * p        : partition key (generic)
 * t_start  : window start (event-time)
 * t_end    : window end (event-time)
 * v        : aggregated value
 * _        : processing time (optional)
 */
case class WindowResult[K](
  partition: K,
  windowStart: Long,
  windowEnd: Long,
  value: Long,
  processingTime: Option[Long] = None
)
