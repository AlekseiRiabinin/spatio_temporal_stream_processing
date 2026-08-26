package cityrover.metrics

import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.util.Collector
import java.time.Duration
import cityrover.model.GeoEvent


object WindowMetrics {

  /**
    * Build a windowed metrics stream.
    *
    * Output tuple:
    *   (roverId, count, minNs, maxNs, avgNs)
    */
  def build(
    stream: DataStream[GeoEvent],
    windowSizeMs: Long
  ): SingleOutputStreamOperator[(String, Long, Long, Long, Double)] = {

    stream
      .keyBy(_.roverId)
      .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(windowSizeMs)))
      .apply(
        new WindowFunction[
          GeoEvent,
          (String, Long, Long, Long, Double),
          String,
          TimeWindow
        ] {

          override def apply(
            key: String,
            window: TimeWindow,
            input: java.lang.Iterable[GeoEvent],
            out: Collector[(String, Long, Long, Long, Double)]
          ): Unit = {

            var count = 0L
            var minNs = Long.MaxValue
            var maxNs = Long.MinValue
            var sumNs = 0L

            val it = input.iterator()
            while (it.hasNext) {
              val event = it.next()
              event.processingStartNs.foreach { start =>
                val latency = System.nanoTime() - start
                count += 1
                sumNs += latency
                if (latency < minNs) minNs = latency
                if (latency > maxNs) maxNs = latency
              }
            }

            val avgNs =
              if (count == 0L) 0.0
              else sumNs.toDouble / count.toDouble

            out.collect((key, count, minNs, maxNs, avgNs))
          }
        }
      )
  }
}
