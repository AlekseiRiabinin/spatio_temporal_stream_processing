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

import cityrover.telemetry.{Telemetry, TelemetryEvent, EnrichedTelemetryEvent}
import cityrover.util.ConfigLoader
import cityrover.metrics.{LatencyMetrics, WindowMetrics}
import cityrover.serialization.ByteArraySchema
import cityrover.cassandra.{CassandraSink, CassandraConnectorConfig}


object ProcessingPipeline:

  def build(
    env: StreamExecutionEnvironment,
    cassandraCfg: CassandraConnectorConfig
  ): Unit =

    // --------------------------------------------------------------------
    // Kafka source
    // --------------------------------------------------------------------
    val kafkaSource =
      KafkaSource.builder[Array[Byte]]()
        .setBootstrapServers(ConfigLoader.kafkaBootstrap)
        .setTopics(ConfigLoader.kafkaRawTelemetryTopic)
        .setGroupId("cityrover-latency")
        .setValueOnlyDeserializer(ByteArraySchema())
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
      rawStream.map { bytes => Telemetry.parseFrom(bytes) }

    // --------------------------------------------------------------------
    // Telemetry -> TelemetryEvent + processingStartNs
    // --------------------------------------------------------------------
    val parsed: DataStream[(TelemetryEvent, Long)] =
      parsedProto.map { proto =>
        val event = TelemetryEvent(
          roverId = proto.roverId,
          lat     = proto.lat.getOrElse(0.0),
          lon     = proto.lon.getOrElse(0.0),
          ts      = proto.ts,
          speed   = proto.speed.getOrElse(0.0),
          heading = proto.heading.getOrElse(0.0),
          edgeId  = proto.edgeId.getOrElse(""),
          routeId = proto.routeId.getOrElse("")
        )
        (event, System.nanoTime())
      }

    // --------------------------------------------------------------------
    // Register metrics + compute latency using original timestamp
    // --------------------------------------------------------------------
    val profiledWithMetrics: DataStream[(TelemetryEvent, Option[Long])] =
      parsed.map(new RichMapFunction[(TelemetryEvent, Long), (TelemetryEvent, Option[Long])]:

        private var latencyUpdater: LatencyMetrics.Updater = null

        override def map(value: (TelemetryEvent, Long)): (TelemetryEvent, Option[Long]) =
          if latencyUpdater == null then
            latencyUpdater = LatencyMetrics.register(getRuntimeContext.getMetricGroup)

          val (event, startNs) = value
          val latencyNs = Some(System.nanoTime() - startNs)

          latencyUpdater.update(latencyNs)
          (event, latencyNs)
      )

    // --------------------------------------------------------------------
    // Convert to EnrichedTelemetryEvent for Cassandra sink
    // --------------------------------------------------------------------
    val enriched: DataStream[EnrichedTelemetryEvent] =
      profiledWithMetrics.map { case (event, latencyOpt) =>
        EnrichedTelemetryEvent(
          roverId = event.roverId,
          ts      = event.ts,
          lat     = event.lat,
          lon     = event.lon,
          speed   = event.speed,
          heading = event.heading,
          latencyNs = latencyOpt.getOrElse(0L)
        )
      }

    // --------------------------------------------------------------------
    // Processing-time tumbling windows (still needed for metrics)
    // --------------------------------------------------------------------
    val windowSizeMs = ConfigLoader.windowSizeMs

    val windowedCounts: SingleOutputStreamOperator[(String, Long)] =
      profiledWithMetrics
        .keyBy { case (event, _) => event.roverId }
        .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(windowSizeMs)))
        .apply(
          new WindowFunction[
            (TelemetryEvent, Option[Long]),
            (String, Long),
            String,
            TimeWindow
          ]:
            override def apply(
              key: String,
              window: TimeWindow,
              input: java.lang.Iterable[(TelemetryEvent, Option[Long])],
              out: Collector[(String, Long)]
            ): Unit =
              var count = 0L
              val it = input.iterator()
              while it.hasNext do
                it.next()
                count += 1
              out.collect((key, count))
        )

    // --------------------------------------------------------------------
    // Window latency metrics
    // --------------------------------------------------------------------
    val latencyWindow: SingleOutputStreamOperator[(String, Long, Double, Double, Double)] =
      WindowMetrics.build(profiledWithMetrics, windowSizeMs)

    // --------------------------------------------------------------------
    // Final sink: Cassandra
    // --------------------------------------------------------------------
    enriched.sinkTo(new CassandraSink(cassandraCfg))

end ProcessingPipeline
