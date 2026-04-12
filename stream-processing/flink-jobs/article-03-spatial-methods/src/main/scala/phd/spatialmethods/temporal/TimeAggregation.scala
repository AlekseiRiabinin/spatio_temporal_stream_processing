package phd.spatialmethods.temporal

import scala.collection.mutable
import phd.spatialmethods.model.{GeoEvent, Trajectory}


/**
 * TimeAggregation provides temporal aggregation methods
 * over GeoEvents and Trajectories for real-time analytics.
 *
 * All timestamps are epoch milliseconds (Long).
 */
object TimeAggregation {

  /**
   * Count events within a time window [windowStartMs, windowEndMs]
   */
  def countEvents(
    events: Seq[GeoEvent],
    windowStartMs: Long,
    windowEndMs: Long
  ): Int =
    events.count(e =>
      e.timestamp >= windowStartMs &&
      e.timestamp <= windowEndMs
    )

  /**
   * Group events into fixed-size tumbling windows.
   *
   * @param windowSizeMs window size in milliseconds
   * @return Map(windowStartMs → events)
   */
  def tumblingWindow(
    events: Seq[GeoEvent],
    windowSizeMs: Long
  ): Map[Long, Seq[GeoEvent]] = {

    events.groupBy { e =>
      val ts = e.timestamp
      ts - (ts % windowSizeMs)
    }
  }

  /**
   * Sliding window aggregation.
   *
   * @param windowSizeMs window size in milliseconds
   * @param slideMs      slide interval in milliseconds
   * @return Map(windowStartMs → events)
   */
  def slidingWindow(
    events: Seq[GeoEvent],
    windowSizeMs: Long,
    slideMs: Long
  ): Map[Long, Seq[GeoEvent]] = {

    if (events.isEmpty) return Map.empty

    val sorted = events.sortBy(_.timestamp)
    val minTime = sorted.head.timestamp
    val maxTime = sorted.last.timestamp

    val windows = mutable.Map[Long, Seq[GeoEvent]]()

    var start = minTime

    while (start <= maxTime) {
      val end = start + windowSizeMs

      val windowEvents = sorted.filter(e =>
        e.timestamp >= start && e.timestamp < end
      )

      windows.put(start, windowEvents)
      start += slideMs
    }

    windows.toMap
  }

  /**
   * Average speed over trajectories active in a time window.
   *
   * Trajectory.startTime and endTime must also be epoch millis.
   */
  def averageSpeed(
    trajectories: Seq[Trajectory],
    windowStartMs: Long,
    windowEndMs: Long
  ): Double = {

    val active = trajectories.filter { t =>
      t.startTime.exists(_ <= windowEndMs) &&
      t.endTime.exists(_ >= windowStartMs)
    }

    if (active.isEmpty) 0.0
    else active.map(_.averageSpeed).sum / active.size
  }

  /**
   * Event rate (events per second)
   */
  def eventRate(
    events: Seq[GeoEvent],
    windowStartMs: Long,
    windowEndMs: Long
  ): Double = {

    val count = countEvents(events, windowStartMs, windowEndMs)
    val durationSec = (windowEndMs - windowStartMs) / 1000.0

    if (durationSec <= 0) 0.0
    else count / durationSec
  }

  /**
   * Compute inter-arrival times (useful for burst detection)
   */
  def interArrivalTimes(events: Seq[GeoEvent]): Seq[Long] = {
    val sorted = events.sortBy(_.timestamp)

    sorted.sliding(2).collect {
      case Seq(a, b) =>
        b.timestamp - a.timestamp
    }.toSeq
  }
}
