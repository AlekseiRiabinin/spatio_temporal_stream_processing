package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import java.time.Duration
import scala.jdk.CollectionConverters.*

import cityrover.model.{TelemetryEvent, EnrichedEvent}
import cityrover.model.toProtobuf

import cityrover.kafka.{KafkaSources, KafkaSinks}
import cityrover.telemetry.{Telemetry, EnrichedTelemetryEvent}
import cityrover.util.ConfigLoader

import cityrover.windows.{
  Window5sFunction,
  Window30sFunction,
  Window1mFunction,
  Window5mFunction
}


object FeatureProcessingPipeline:

  def build(env: StreamExecutionEnvironment): Unit =

    // 1. Kafka source (raw telemetry)
    val rawBytes: DataStream[Array[Byte]] =
      env.fromSource(
        KafkaSources.rawTelemetrySource(),
        WatermarkStrategy.noWatermarks(),
        "raw-telemetry-source"
      )

    // 2. Watermark strategy based on the event timestamp (`ts`).
    val watermarkStrategy: WatermarkStrategy[TelemetryEvent] =
      WatermarkStrategy
        .forBoundedOutOfOrderness[TelemetryEvent](Duration.ofSeconds(5))
        .withTimestampAssigner((event, _) => event.ts)
        .withIdleness(Duration.ofSeconds(30))

    // 3. Parse protobuf → TelemetryEvent, then assign timestamps + watermarks
    val telemetry: DataStream[TelemetryEvent] =
      rawBytes
        .map(bytes =>
          val proto = Telemetry.parseFrom(bytes)
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
        )
        .assignTimestampsAndWatermarks(watermarkStrategy)

    // 4. Compute rolling window features
    val enriched5s =
      telemetry
        .keyBy(_.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window5s)))
        .process(new Window5sFunction)

    val enriched30s =
      telemetry
        .keyBy(_.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window30s)))
        .process(new Window30sFunction)

    val enriched1m =
      telemetry
        .keyBy(_.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window1m)))
        .process(new Window1mFunction)

    val enriched5m =
      telemetry
        .keyBy(_.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window5m)))
        .process(new Window5mFunction)

    // union returns a plain DataStream; no cast needed
    val enrichedAll: DataStream[EnrichedEvent] =
      enriched5s.union(enriched30s, enriched1m, enriched5m)

    // 5. Kafka sink (protobuf enriched telemetry)
    val protobufStream: DataStream[EnrichedTelemetryEvent] =
      enrichedAll.map(_.toProtobuf)

    protobufStream
      .sinkTo(KafkaSinks.enrichedTelemetrySink())
