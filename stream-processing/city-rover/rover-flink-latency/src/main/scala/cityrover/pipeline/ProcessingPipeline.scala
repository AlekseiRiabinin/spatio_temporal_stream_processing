package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.{
  DataStream,
  SingleOutputStreamOperator
}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{
  MapFunction,
  RichMapFunction
}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.util.Collector

import java.time.Duration

import cityrover.telemetry.{Telemetry, TelemetryEvent}
import cityrover.util.ConfigLoader
import cityrover.metrics.{LatencyMetrics, WindowMetrics}
import cityrover.serialization.ByteArraySchema


object ProcessingPipeline {

  def build(env: StreamExecutionEnvironment): Unit = {

    // --------------------------------------------------------------------
    // Kafka source
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
    // Protobuf bytes -> Telemetry
    // --------------------------------------------------------------------

    val parsedProto: DataStream[Telemetry] =
      rawStream.map(
        new MapFunction[Array[Byte], Telemetry] {
          override def map(value: Array[Byte]): Telemetry =
            Telemetry.parseFrom(value)
        }
      )

    // --------------------------------------------------------------------
    // Telemetry -> TelemetryEvent
    // --------------------------------------------------------------------

    val parsed: DataStream[TelemetryEvent] =
      parsedProto.map(
        new MapFunction[Telemetry, TelemetryEvent] {
          override def map(proto: Telemetry): TelemetryEvent =
            TelemetryEvent(
              roverId = proto.roverId,
              lat     = proto.lat.getOrElse(0.0),
              lon     = proto.lon.getOrElse(0.0),
              ts      = proto.ts,
              speed   = proto.speed.getOrElse(0.0),
              heading = proto.heading.getOrElse(0.0),
              edgeId  = proto.edgeId.getOrElse(""),
              routeId = proto.routeId.getOrElse("")
            )
        }
      )


    // --------------------------------------------------------------------
    // Processing-time pipeline
    // --------------------------------------------------------------------

    val processingTimeStream: DataStream[TelemetryEvent] = parsed

    // --------------------------------------------------------------------
    // Index events + register latency metric
    // --------------------------------------------------------------------

    val indexed: DataStream[(TelemetryEvent, Long)] =

      processingTimeStream.map(
        new RichMapFunction[TelemetryEvent, (TelemetryEvent, Long)] {

          private var counter = 0L
          private var latencyUpdater: LatencyMetrics.Updater = null

          override def map(event: TelemetryEvent): (TelemetryEvent, Long) = {

            if (latencyUpdater == null) {

              latencyUpdater =
                LatencyMetrics.register(getRuntimeContext.getMetricGroup)
            }

            counter += 1

            (event, counter)
          }
        }
      )

    // --------------------------------------------------------------------
    // Update Prometheus operator-chain latency.
    //
    // LatencyMetrics is registered inside the previous operator, so the
    // updater belongs to that operator's runtime context.
    //
    // Therefore this metric should be updated in the same operator where
    // it was registered.
    // --------------------------------------------------------------------

    // The current pipeline structure needs the profiler timestamp and
    // metric update to live in the same RichMapFunction.
    //
    // This is addressed below by using a dedicated profiling operator.

    val profiledWithMetrics: DataStream[(TelemetryEvent, Option[Long])] =

      indexed.map(
        new RichMapFunction[(TelemetryEvent, Long), (TelemetryEvent, Option[Long])] {

          private var latencyUpdater: LatencyMetrics.Updater = null

          override def map(
            value: (TelemetryEvent, Long)
          ): (TelemetryEvent, Option[Long]) = {

            if (latencyUpdater == null) {
              latencyUpdater =
                LatencyMetrics.register(getRuntimeContext.getMetricGroup)
            }

            val (event, index) = value

            val processingStartNs =
              LatencyProfiler.annotate(index)

            latencyUpdater.update(processingStartNs)

            (event, processingStartNs)
          }
        }
      )

    // --------------------------------------------------------------------
    // Processing-time tumbling windows
    // --------------------------------------------------------------------

    val windowSizeMs =
      ConfigLoader.windowSizeMs

    val windowedCounts: SingleOutputStreamOperator[(String, Long)] =
      profiledWithMetrics
        .keyBy { case (event, _) => event.roverId }
        .window(
          TumblingProcessingTimeWindows.of(
            Duration.ofMillis(windowSizeMs)
          )
        )
        .apply(
          new WindowFunction[
            (TelemetryEvent, Option[Long]),
            (String, Long),
            String,
            TimeWindow
          ] {

            override def apply(
              key: String,
              window: TimeWindow,
              input: java.lang.Iterable[(TelemetryEvent, Option[Long])],
              out: Collector[(String, Long)]
            ): Unit = {

              var count = 0L

              val it = input.iterator()

              while (it.hasNext) {
                it.next()
                count += 1
              }

              out.collect(
                (key, count)
              )
            }
          }
        )

    // --------------------------------------------------------------------
    // Window latency metrics
    //
    // Output:
    //
    //   roverId
    //   count
    //   min latency [ms]
    //   max latency [ms]
    //   avg latency [ms]
    // --------------------------------------------------------------------

    val latencyWindow: SingleOutputStreamOperator[(String, Long, Double, Double, Double)] =
      WindowMetrics.build(profiledWithMetrics, windowSizeMs)

    // --------------------------------------------------------------------
    // Sinks
    // --------------------------------------------------------------------

    windowedCounts.print("windowed-counts")
    latencyWindow.print("latency-window-ms")
  }
}
