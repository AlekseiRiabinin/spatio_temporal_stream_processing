package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.util.Collector

import java.time.Duration

import cityrover.telemetry.Telemetry
import cityrover.util.ConfigLoader
import cityrover.metrics.{LatencyMetrics, WindowMetrics}
import cityrover.pipeline.LatencyProfiler
import cityrover.serialization.ByteArraySchema


object ProcessingPipeline {

  def build(env: StreamExecutionEnvironment): Unit = {

    // --------------------------------------------------------------------
    // Kafka source (Protobuf bytes, no watermarks → lowest latency)
    // --------------------------------------------------------------------
    val kafkaSource =
      KafkaSource.builder[Array[Byte]]()
        .setBootstrapServers(ConfigLoader.kafkaBootstrap)
        .setTopics(ConfigLoader.kafkaRawTelemetryTopic)
        .setGroupId("cityrover-latency")
        .setValueOnlyDeserializer(new ByteArraySchema())
        .build()

    val rawStream: DataStream[Array[Byte]] =
      env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "protobuf-telemetry-source"
      )

    // --------------------------------------------------------------------
    // Protobuf bytes → Telemetry (ScalaPB)
    // --------------------------------------------------------------------
    val parsed: DataStream[Telemetry] =
      rawStream.map(new MapFunction[Array[Byte], Telemetry] {
        override def map(value: Array[Byte]): Telemetry =
          Telemetry.parseFrom(value)
      })

    // --------------------------------------------------------------------
    // Pure processing-time pipeline (no event-time watermarks)
    // --------------------------------------------------------------------
    val processingTimeStream: DataStream[Telemetry] = parsed

    // --------------------------------------------------------------------
    // Annotate events + register latency metrics
    // --------------------------------------------------------------------
    val indexed: DataStream[(Telemetry, Long)] =
      processingTimeStream.map(new RichMapFunction[Telemetry, (Telemetry, Long)] {

        private var counter     = 0L
        private var initialized = false

        override def map(event: Telemetry): (Telemetry, Long) = {
          if (!initialized) {
            LatencyMetrics.register(getRuntimeContext.getMetricGroup)
            initialized = true
          }
          counter += 1
          (event, counter)
        }
      })

    val profiled: DataStream[(Telemetry, Option[Long])] =
      indexed.map(new MapFunction[(Telemetry, Long), (Telemetry, Option[Long])] {
        override def map(value: (Telemetry, Long)): (Telemetry, Option[Long]) = {
          val (event, index) = value
          val tsOpt = LatencyProfiler.annotate(index)
          LatencyMetrics.update(tsOpt)
          (event, tsOpt)
        }
      })

    // --------------------------------------------------------------------
    // Processing-time tumbling window (lowest latency)
    // --------------------------------------------------------------------
    val windowSizeMs = ConfigLoader.windowSizeMs

    val windowedCounts: SingleOutputStreamOperator[(String, Long)] =
      profiled
        .keyBy { case (event, _) => event.roverId }
        .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(windowSizeMs)))
        .apply(new WindowFunction[(Telemetry, Option[Long]), (String, Long), String, TimeWindow] {

          override def apply(
            key: String,
            window: TimeWindow,
            input: java.lang.Iterable[(Telemetry, Option[Long])],
            out: Collector[(String, Long)]
          ): Unit = {
            val count = input.spliterator().getExactSizeIfKnown
            out.collect((key, count))
          }
        })

    // --------------------------------------------------------------------
    // WindowMetrics: processing-time windows
    // --------------------------------------------------------------------
    val latencyWindow: SingleOutputStreamOperator[(String, Long, Long, Long, Double)] =
      WindowMetrics.build(profiled, windowSizeMs)

    // --------------------------------------------------------------------
    // Sinks
    // --------------------------------------------------------------------
    windowedCounts.print("windowed-counts")
    latencyWindow.print("latency-window")
  }
}
