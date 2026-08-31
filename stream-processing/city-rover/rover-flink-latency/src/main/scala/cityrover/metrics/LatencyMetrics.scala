package cityrover.metrics

import org.apache.flink.metrics.{Gauge, MetricGroup}


/**
  * Operator‑chain latency metrics exposed to Prometheus.
  *
  * Latency is computed externally:
  *
  *   latency = System.nanoTime() - processingStartNs
  *
  * GeoEvent is a ScalaPB message and does not carry timestamps.
  */
object LatencyMetrics {

  @volatile private var lastLatencyNs: Long = 0L

  /**
    * Register Prometheus‑visible gauges.
    */
  def register(group: MetricGroup): Unit = {

    // Nanoseconds gauge (boxed type required)
    group.gauge(
      "operator_chain_latency_ns",
      new Gauge[java.lang.Long] {
        override def getValue: java.lang.Long =
          java.lang.Long.valueOf(lastLatencyNs)
      }
    )

    // Milliseconds gauge (boxed Double)
    group.gauge(
      "operator_chain_latency_ms",
      new Gauge[java.lang.Double] {
        override def getValue: java.lang.Double =
          java.lang.Double.valueOf(lastLatencyNs / 1_000_000.0)
      }
    )
  }

  /**
    * Update latency using externally provided processingStartNs.
    *
    * @param processingStartNs Optional timestamp from LatencyProfiler
    */
  def update(processingStartNs: Option[Long]): Unit = {
    processingStartNs.foreach { start =>
      lastLatencyNs = System.nanoTime() - start
    }
  }
}
