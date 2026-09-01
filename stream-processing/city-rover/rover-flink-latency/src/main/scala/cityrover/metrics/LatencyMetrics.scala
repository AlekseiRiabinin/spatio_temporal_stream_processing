package cityrover.metrics

import org.apache.flink.metrics.{Gauge, MetricGroup}


/**
  * Operator-chain latency metrics exposed to Prometheus.
  *
  * Latency is measured internally using System.nanoTime()
  * and exposed to Prometheus in milliseconds.
  *
  * The pipeline provides processingStartNs via LatencyProfiler.
  */
object LatencyMetrics {

  /**
    * Register Prometheus-visible operator latency metric.
    *
    * Metric state belongs to this registration instance,
    * i.e. to one Flink operator/subtask.
    */
  def register(group: MetricGroup): Updater = {

    val state = new LatencyState

    group.gauge(
      "operator_chain_latency_ms",
      new Gauge[java.lang.Double] {
        override def getValue: java.lang.Double =
          java.lang.Double.valueOf(state.lastLatencyNs / 1_000_000.0)
      }
    )

    new Updater {
      override def update(processingStartNs: Option[Long]): Unit = {
        processingStartNs.foreach { start =>
          state.lastLatencyNs =
            System.nanoTime() - start
        }
      }
    }
  }

  private final class LatencyState {

    @volatile
    var lastLatencyNs: Long = 0L
  }

  trait Updater {
    def update(processingStartNs: Option[Long]): Unit
  }
}
