package phd.architecture.operators

import org.apache.flink.api.java.functions.KeySelector
import phd.architecture.model.{Event, SpatialPartition}
import phd.architecture.util.GeometryUtils
import phd.architecture.metrics.MetricsRegistry


object SpatialPartitionFunction {

  /**
   * π(e) → p
   * Spatial partitioning using geohash over event geometry.
   */
  def byGeohash(precision: Int): KeySelector[Event, SpatialPartition] =
    new KeySelector[Event, SpatialPartition] {

      override def getKey(event: Event): SpatialPartition = {
        val geohash = GeometryUtils.toGeohash(event.geometry, precision)

        MetricsRegistry.recordPartition(
          s"partition geohash=$geohash precision=$precision eventId=${event.id}"
        )

        SpatialPartition(geohash, precision)
      }
    }
}
