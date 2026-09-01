package cityrover.metrics

import org.apache.flink.metrics.{Gauge, MetricGroup}
import org.apache.flink.streaming.api.datastream.{
  DataStream,
  SingleOutputStreamOperator
}
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

import cityrover.telemetry.TelemetryEvent


object WindowMetrics {

  /**
    * Build processing-time latency windows.
    *
    * Output:
    *
    *   roverId
    *   event count
    *   minimum latency [ms]
    *   maximum latency [ms]
    *   average latency [ms]
    */
  def build(
    stream: DataStream[(TelemetryEvent, Option[Long])],
    windowSizeMs: Long
  ): SingleOutputStreamOperator[
    (String, Long, Double, Double, Double)
  ] = {

    stream
      .keyBy { case (event, _) => event.roverId }
      .window(
        TumblingProcessingTimeWindows.of(
          Duration.ofMillis(windowSizeMs)
        )
      )
      .apply(
        new RichWindowFunction[
          (TelemetryEvent, Option[Long]),
          (String, Long, Double, Double, Double),
          String,
          TimeWindow
        ] {

          @transient
          private var metrics: WindowMetricState = null

          override def apply(
            key: String,
            window: TimeWindow,
            input: java.lang.Iterable[(TelemetryEvent, Option[Long])],
            out: Collector[(String, Long, Double, Double, Double)]
          ): Unit = {

            if (metrics == null) {
              metrics =
                new WindowMetricState(
                  getRuntimeContext.getMetricGroup
                )
            }

            var count = 0L
            var minNs = Long.MaxValue
            var maxNs = Long.MinValue
            var sumNs = 0L

            val it = input.iterator()

            while (it.hasNext) {

              val (_, tsOpt) = it.next()

              tsOpt.foreach { start =>

                val latencyNs = System.nanoTime() - start

                count += 1
                sumNs += latencyNs

                if (latencyNs < minNs) {
                  minNs = latencyNs
                }

                if (latencyNs > maxNs) {
                  maxNs = latencyNs
                }
              }
            }

            // ------------------------------------------------------------
            // Convert latency to milliseconds.
            // ------------------------------------------------------------

            val minMs =
              if (count == 0L)
                0.0
              else
                minNs / 1_000_000.0

            val maxMs =
              if (count == 0L)
                0.0
              else
                maxNs / 1_000_000.0

            val avgMs =
              if (count == 0L)
                0.0
              else
                sumNs.toDouble /
                  count.toDouble /
                  1_000_000.0

            // ------------------------------------------------------------
            // Update Prometheus gauges.
            // ------------------------------------------------------------

            metrics.update(
              count = count,
              minMs = minMs,
              maxMs = maxMs,
              avgMs = avgMs
            )

            // ------------------------------------------------------------
            // Emit window result.
            //
            // All latency values are milliseconds.
            // ------------------------------------------------------------

            out.collect(
              (
                key,
                count,
                minMs,
                maxMs,
                avgMs
              )
            )
          }
        }
      )
  }


  /**
    * Per-window Prometheus metric state.
    */
  private final class WindowMetricState(
    group: MetricGroup
  ) {

    @volatile
    private var eventCount: Long = 0L

    @volatile
    private var minLatencyMs: Double = 0.0

    @volatile
    private var maxLatencyMs: Double = 0.0

    @volatile
    private var avgLatencyMs: Double = 0.0


    group.gauge(
      "window_event_count",
      new Gauge[java.lang.Long] {
        override def getValue: java.lang.Long =
          java.lang.Long.valueOf(eventCount)
      }
    )

    group.gauge(
      "window_latency_min_ms",
      new Gauge[java.lang.Double] {
        override def getValue: java.lang.Double =
          java.lang.Double.valueOf(minLatencyMs)
      }
    )

    group.gauge(
      "window_latency_max_ms",
      new Gauge[java.lang.Double] {
        override def getValue: java.lang.Double =
          java.lang.Double.valueOf(maxLatencyMs)
      }
    )

    group.gauge(
      "window_latency_avg_ms",
      new Gauge[java.lang.Double] {
        override def getValue: java.lang.Double =
          java.lang.Double.valueOf(avgLatencyMs)
      }
    )


    def update(
      count: Long,
      minMs: Double,
      maxMs: Double,
      avgMs: Double
    ): Unit = {

      eventCount = count
      minLatencyMs = minMs
      maxLatencyMs = maxMs
      avgLatencyMs = avgMs
    }
  }
}
