package phd.spatialmethods.temporal

import java.time.Instant
import phd.spatialmethods.model.{GeoEvent, SpatialRegion}


/**
 * DensityEstimator computes spatial-temporal density metrics
 * for GeoEvents, supporting swarm analysis and hotspot detection.
 */
object DensityEstimator {

  /**
   * Count number of events per region within a time window
   *
   * NOTE: Spatial predicate is abstracted via `isInside`
   */
  def countPerRegion(
    events: Seq[GeoEvent],
    regions: Seq[SpatialRegion],
    windowStart: Instant,
    windowEnd: Instant,
    isInside: (GeoEvent, SpatialRegion) => Boolean
  ): Map[String, Int] = {

    val filtered = events.filter(e =>
      !e.timestamp.isBefore(windowStart) &&
      !e.timestamp.isAfter(windowEnd)
    )

    regions.map { region =>
      val count = filtered.count(e => isInside(e, region))
      region.id -> count
    }.toMap
  }

  /**
   * Compute density (events per unit area)
   *
   * @param regionAreas Map(regionId -> area in square meters)
   */
  def densityPerRegion(
    counts: Map[String, Int],
    regionAreas: Map[String, Double]
  ): Map[String, Double] = {

    counts.map { case (regionId, count) =>
      val area = regionAreas.getOrElse(regionId, 1.0) // avoid division by zero
      regionId -> (count.toDouble / area)
    }
  }

  /**
   * Detect high-density regions (hotspots)
   */
  def detectHotspots(
    density: Map[String, Double],
    threshold: Double
  ): Seq[String] =
    density.collect {
      case (regionId, d) if d >= threshold => regionId
    }.toSeq

  /**
   * Global density (events per unit area over all regions)
   */
  def globalDensity(
    events: Seq[GeoEvent],
    totalArea: Double,
    windowStart: Instant,
    windowEnd: Instant
  ): Double = {

    val count = events.count(e =>
      !e.timestamp.isBefore(windowStart) &&
      !e.timestamp.isAfter(windowEnd)
    )

    if (totalArea <= 0) 0.0
    else count.toDouble / totalArea
  }

  /**
   * Density trend over time (windowed)
   *
   * Returns: windowStart -> density
   */
  def densityOverTime(
    events: Seq[GeoEvent],
    windowSizeMillis: Long
  ): Map[Long, Double] = {

    val windows = events.groupBy { e =>
      val ts = e.timestamp.toEpochMilli
      ts - (ts % windowSizeMillis)
    }

    windows.map { case (windowStart, evs) =>
      val durationSec = windowSizeMillis / 1000.0
      val density = if (durationSec == 0) 0.0 else evs.size / durationSec
      windowStart -> density
    }
  }

}
