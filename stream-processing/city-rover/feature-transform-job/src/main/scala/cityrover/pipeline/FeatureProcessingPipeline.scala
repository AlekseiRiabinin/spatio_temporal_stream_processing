package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import java.time.Duration
import scala.jdk.CollectionConverters.*

import cityrover.model.{TelemetryEvent, EnrichedEvent}
import cityrover.model.toProtobuf

import cityrover.kafka.{KafkaSources, KafkaSinks}
import cityrover.telemetry.{Telemetry, EnrichedTelemetryEvent}
import cityrover.util.ConfigLoader

import cityrover.windows.{Window5sFunction, Window30sFunction, Window1mFunction, Window5mFunction}


object FeatureProcessingPipeline:

  def build(env: StreamExecutionEnvironment): Unit =

    // 1. Kafka source (raw telemetry)
    val rawBytes: DataStream[Array[Byte]] =
      env.fromSource(
        KafkaSources.rawTelemetrySource(),
        WatermarkStrategy.noWatermarks(),
        "raw-telemetry-source"
      )

    // 2. Parse protobuf → TelemetryEvent
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
        .name("parse-protobuf")

    // 3. Compute rolling window features
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

    val enrichedAll: SingleOutputStreamOperator[EnrichedEvent] =
      enriched5s
        .union(enriched30s)
        .union(enriched1m)
        .union(enriched5m)
        .asInstanceOf[SingleOutputStreamOperator[EnrichedEvent]]

    enrichedAll.name("compute-window-features")


    // 4. Kafka sink (protobuf enriched telemetry)
    val protobufStream: DataStream[EnrichedTelemetryEvent] =
      enrichedAll.map(_.toProtobuf)

    protobufStream
      .sinkTo(KafkaSinks.enrichedTelemetrySink())
      .name("enriched-kafka-sink")
