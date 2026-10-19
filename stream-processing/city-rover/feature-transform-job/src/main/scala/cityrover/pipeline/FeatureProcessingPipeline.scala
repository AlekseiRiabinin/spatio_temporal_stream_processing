package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import java.time.Duration

import cityrover.model.{TelemetryEvent, EnrichedEvent}
import cityrover.model.toProtobuf

import cityrover.kafka.{KafkaSources, KafkaSinks}
import cityrover.telemetry.{Telemetry, EnrichedTelemetryEvent}
import cityrover.util.{ConfigLoader, MetricsConfig}

import cityrover.metrics.{LatencyInstrumentedMap, StampedRecord}

import cityrover.windows.{
  Window5sFunction,
  Window30sFunction,
  Window1mFunction,
  Window5mFunction
}


object FeatureProcessingPipeline:

  def build(
    env: StreamExecutionEnvironment,
    metricsCfg: MetricsConfig
  ): Unit =

    // 1. Kafka source (raw telemetry bytes)
    val rawBytes: DataStream[Array[Byte]] =
      env.fromSource(
        KafkaSources.rawTelemetrySource(),
        WatermarkStrategy.noWatermarks(),
        "raw-telemetry-source"
      )

    // 2. Source-boundary instrumentation.
    val stampedBytes: DataStream[StampedRecord[Array[Byte]]] =
      rawBytes.map(
        LatencyInstrumentedMap[Array[Byte]](
          metricsCfg,
          subgroup = "transform"
        )
      )

    // 3. Parse protobuf -> TelemetryEvent, keeping the timing stamps.
    val stampedTelemetry: DataStream[StampedRecord[TelemetryEvent]] =
      stampedBytes.map { stamped =>
        val proto = Telemetry.parseFrom(stamped.payload)
        StampedRecord(
          payload = TelemetryEvent(
            roverId = proto.roverId,
            lat     = proto.lat.getOrElse(0.0),
            lon     = proto.lon.getOrElse(0.0),
            ts      = proto.ts,
            speed   = proto.speed.getOrElse(0.0),
            heading = proto.heading.getOrElse(0.0),
            edgeId  = proto.edgeId.getOrElse(""),
            routeId = proto.routeId.getOrElse("")
          ),
          t1Ms   = stamped.t1Ms,
          tSrcMs = stamped.tSrcMs
        )
      }

    // 4. Watermark strategy based on the event timestamp (`ts`).
    val watermarkStrategy: WatermarkStrategy[StampedRecord[TelemetryEvent]] =
      WatermarkStrategy
        .forBoundedOutOfOrderness[StampedRecord[TelemetryEvent]](Duration.ofSeconds(5))
        .withTimestampAssigner((stamped, _) => stamped.payload.ts)
        .withIdleness(Duration.ofSeconds(30))

    val telemetry: DataStream[StampedRecord[TelemetryEvent]] =
      stampedTelemetry.assignTimestampsAndWatermarks(watermarkStrategy)

    // 5. Rolling window features.
    //
    //    Each branch emits a DIFFERENT concrete subtype of EnrichedEvent:
    //      Window5sFunction  -> EnrichedEvent5s (Window5sPayload only)
    //      Window30sFunction -> EnrichedEvent30s (Window30sPayload only)
    //      Window1mFunction  -> EnrichedEvent1m (Window1mPayload only)
    //      Window5mFunction  -> EnrichedEvent5m (Window5mPayload only)
    //
    //    `windowEndTs` is set to `ctx.window.getEnd` (event-time, ms)
    //    inside each function and carried through the envelope. The
    //    writer uses it for Cassandra `USING TIMESTAMP ? * 1000`.
    //
    //    The explicit `DataStream[EnrichedEvent]` widenings make the
    //    union's common type obvious and catch a future 5th window that
    //    forgets to extend the sealed trait at compile time here.
    val enriched5s: DataStream[EnrichedEvent] =
      telemetry
        .keyBy(_.payload.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window5s)))
        .process(new Window5sFunction(metricsCfg))

    val enriched30s: DataStream[EnrichedEvent] =
      telemetry
        .keyBy(_.payload.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window30s)))
        .process(new Window30sFunction(metricsCfg))

    val enriched1m: DataStream[EnrichedEvent] =
      telemetry
        .keyBy(_.payload.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window1m)))
        .process(new Window1mFunction(metricsCfg))

    val enriched5m: DataStream[EnrichedEvent] =
      telemetry
        .keyBy(_.payload.roverId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(ConfigLoader.window5m)))
        .process(new Window5mFunction(metricsCfg))

    // 6. Union — interleaves the four partial-record streams.
    //    No join, no buffering; each branch fires independently.
    val enrichedAll: DataStream[EnrichedEvent] =
      enriched5s.union(enriched30s, enriched1m, enriched5m)

    // 7. Serialize to protobuf.
    //
    //    `toProtobuf` sets the `oneof features` variant based on the
    //    concrete EnrichedEvent subtype, and copies `windowEndTs` from
    //    the envelope (event-time, ms). Do NOT substitute a wall-clock
    //    timestamp here — replays would stop being idempotent and the
    //    writer's `USING TIMESTAMP * 1000` would lose meaning.
    val protobufStream: DataStream[EnrichedTelemetryEvent] =
      enrichedAll.map(_.toProtobuf)

    // 8. Kafka sink.
    //
    //    The Kafka message key is set to `roverId` inside the record
    //    serializer (`RoverIdKeySchema`), not by a Flink `keyBy` here.
    //    Do not add `keyBy(...)` before `sinkTo` — it would introduce
    //    a network shuffle without changing partitioning, since the
    //    Kafka key is what determines the destination partition.
    protobufStream
      .sinkTo(KafkaSinks.enrichedTelemetrySink())
