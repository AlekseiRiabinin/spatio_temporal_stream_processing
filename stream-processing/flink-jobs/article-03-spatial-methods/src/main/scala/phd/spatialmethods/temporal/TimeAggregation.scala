package phd.spatialmethods.temporal

import java.time.{Duration, Instant}
import phd.spatialmethods.model.{GeoEvent, Trajectory}


/**
 * TimeAggregation provides temporal aggregation methods
 * over GeoEvents and Trajectories for real-time analytics.
 */
object TimeAggregation {

  /**
   * Count events within a time window
   */
  def countEvents(
    events: Seq[GeoEvent],
    windowStart: Instant,
    windowEnd: Instant
  ): Int =
    events.count(e =>
      !e.timestamp.isBefore(windowStart) &&
      !e.timestamp.isAfter(windowEnd)
    )

  /**
   * Group events into fixed-size time windows
   */
  def tumblingWindow(
    events: Seq[GeoEvent],
    windowSize: Duration
  ): Map[Long, Seq[GeoEvent]] = {

    events.groupBy { e =>
      val ts = e.timestamp.toEpochMilli
      ts - (ts % windowSize.toMillis)
    }
  }

  /**
   * Sliding window aggregation (returns window start → events)
   */
  def slidingWindow(
    events: Seq[GeoEvent],
    windowSize: Duration,
    slide: Duration
  ): Map[Long, Seq[GeoEvent]] = {

    val sorted = events.sortBy(_.timestamp.toEpochMilli)
    val minTime = sorted.headOption.map(_.timestamp.toEpochMilli).getOrElse(0L)
    val maxTime = sorted.lastOption.map(_.timestamp.toEpochMilli).getOrElse(0L)

    val windows = scala.collection.mutable.Map[Long, Seq[GeoEvent]]()

    var start = minTime

    while (start <= maxTime) {
      val end = start + windowSize.toMillis

      val windowEvents = sorted.filter(e => {
        val ts = e.timestamp.toEpochMilli
        ts >= start && ts < end
      })

      windows.put(start, windowEvents)
      start += slide.toMillis
    }

    windows.toMap
  }

  /**
   * Average speed over trajectories in a time window
   */
  def averageSpeed(
    trajectories: Seq[Trajectory],
    windowStart: Instant,
    windowEnd: Instant
  ): Double = {

    val filtered = trajectories.filter(t =>
      t.startTime.exists(!_.isAfter(windowEnd)) &&
      t.endTime.exists(!_.isBefore(windowStart))
    )

    if (filtered.isEmpty) 0.0
    else filtered.map(_.averageSpeed).sum / filtered.size
  }

  /**
   * Event rate (events per second)
   */
  def eventRate(
    events: Seq[GeoEvent],
    windowStart: Instant,
    windowEnd: Instant
  ): Double = {

    val count = countEvents(events, windowStart, windowEnd)
    val durationSec =
      (windowEnd.toEpochMilli - windowStart.toEpochMilli) / 1000.0

    if (durationSec <= 0) 0.0
    else count / durationSec
  }

  /**
   * Compute inter-arrival times (useful for burst detection)
   */
  def interArrivalTimes(events: Seq[GeoEvent]): Seq[Long] = {
    val sorted = events.sortBy(_.timestamp.toEpochMilli)

    sorted.sliding(2).collect {
      case Seq(a, b) =>
        b.timestamp.toEpochMilli - a.timestamp.toEpochMilli
    }.toSeq
  }

}
