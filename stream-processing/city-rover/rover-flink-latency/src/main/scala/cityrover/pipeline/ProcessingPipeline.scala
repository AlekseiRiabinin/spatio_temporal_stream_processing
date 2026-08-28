package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.util.Collector

import java.time.Duration

import cityrover.model.GeoEvent
import cityrover.util.ConfigLoader
import cityrover.metrics.{LatencyMetrics, WindowMetrics}


object ProcessingPipeline {

  def build(env: StreamExecutionEnvironment): Unit = {

    // --------------------------------------------------------------------
    // Kafka source (no watermarks → lowest latency)
    // --------------------------------------------------------------------
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(ConfigLoader.kafkaBootstrap)
      .setTopics(ConfigLoader.kafkaRawTelemetryTopic)
      .setGroupId("cityrover-latency")
      .setValueOnlyDeserializer(SimpleStringSchema())
      .build()

    val rawStream: DataStream[String] =
      env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),   // no event-time → no delay
        "raw-telemetry-source"
      )

    // --------------------------------------------------------------------
    // Parse JSON → GeoEvent
    // --------------------------------------------------------------------
    val parsed: DataStream[GeoEvent] =
      rawStream.map(new MapFunction[String, GeoEvent] {
        override def map(value: String): GeoEvent =
          EventParser.parse(value)
      })

    // --------------------------------------------------------------------
    // Pure processing-time pipeline (no event-time watermarks)
    // --------------------------------------------------------------------
    val processingTimeStream: DataStream[GeoEvent] = parsed

    // --------------------------------------------------------------------
    // Annotate events + register latency metrics
    // --------------------------------------------------------------------
    val indexed: DataStream[(GeoEvent, Long)] =
      processingTimeStream.map(new RichMapFunction[GeoEvent, (GeoEvent, Long)] {

        private var counter     = 0L
        private var initialized = false

        override def map(event: GeoEvent): (GeoEvent, Long) = {
          if (!initialized) {
            LatencyMetrics.register(getRuntimeContext.getMetricGroup)
            initialized = true
          }
          counter += 1
          (event, counter)
        }
      })

    val profiled: DataStream[GeoEvent] =
      indexed.map(new MapFunction[(GeoEvent, Long), GeoEvent] {
        override def map(value: (GeoEvent, Long)): GeoEvent = {
          val event = LatencyProfiler.annotate(value._1, value._2)
          LatencyMetrics.update(event)
          event
        }
      })

    // --------------------------------------------------------------------
    // Processing-time tumbling window (lowest latency)
    // --------------------------------------------------------------------
    val windowSizeMs = ConfigLoader.windowSizeMs

    val windowedCounts: SingleOutputStreamOperator[(String, Long)] =
      profiled
        .keyBy(_.roverId)
        .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(windowSizeMs)))
        .apply(new WindowFunction[GeoEvent, (String, Long), String, TimeWindow] {

          override def apply(
            key: String,
            window: TimeWindow,
            input: java.lang.Iterable[GeoEvent],
            out: Collector[(String, Long)]
          ): Unit = {
            val count = input.spliterator().getExactSizeIfKnown
            out.collect((key, count))
          }
        })

    // --------------------------------------------------------------------
    // WindowMetrics: ensure this uses processing-time windows internally
    // --------------------------------------------------------------------
    val latencyWindow = WindowMetrics.build(profiled, windowSizeMs)

    // --------------------------------------------------------------------
    // Sinks
    // --------------------------------------------------------------------
    windowedCounts.print("windowed-counts")
    latencyWindow.print("latency-window")
  }
}
