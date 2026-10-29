package cityrover.kafka

import zio._
import zio.kafka.consumer.{
  CommittableRecord,
  Consumer,
  ConsumerSettings,
  Offset,
  Subscription
}
import zio.kafka.serde.Serde
import zio.stream.ZStream

import cityrover.model.TelemetryEvent
import cityrover.telemetry.Telemetry
import cityrover.util.AppConfig


/** A raw telemetry event paired with the timing context needed for
  * latency metrics.
  *
  * ZIO counterpart of the Flink `StampedRecord[TelemetryEvent]`:
  *   - `t1Ms`   : Kafka broker timestamp (ms since epoch). May be -1 if
  *                the broker doesn't set one; metrics that need it are
  *                skipped when t1Ms < 0.
  *   - `tSrcMs` : wall clock at the source boundary (when the record
  *                left the consumer), in ms since epoch.
  *
  * `offset` stays alongside so `Main` can commit after produce succeeds.
  */
final case class StampedEvent(
  event: TelemetryEvent,
  t1Ms: Long,
  tSrcMs: Long
)


object Consumer:

  /** Build `ConsumerSettings` from the full `AppConfig`. */
  def settings(config: AppConfig): ConsumerSettings =
    ConsumerSettings(config.kafka.bootstrapServers.split(",").toList)
      .withGroupId(config.kafka.groupId)
      .withProperty("auto.offset.reset", config.kafka.consumer.offsetReset)
      .withProperty("fetch.min.bytes", config.kafka.consumer.fetchMinBytes.toString)
      .withProperty("fetch.max.wait.ms", config.kafka.consumer.fetchMaxWaitMs.toString)
      .withProperty("enable.auto.commit", "false")
      .withClientId("feature-transform-zio")

  /** Raw telemetry stream paired with its Kafka offset and timing context.
    *
    * `Main` uses:
    *   - `stamped.t1Ms`   → kafka_to_flink_ms, latency_ms
    *   - `stamped.tSrcMs` → queue_ms
    *   - `offset`         → committed after produce succeeds
    *
    * No metrics are recorded here; the consumer's only job is
    * "bytes → TelemetryEvent + timing + offset". Recorded once at the
    * effect site (`Main`), which avoids duplicating metric registration
    * across layers.
    */
  def streamWithOffsets(
    config: AppConfig
  ): ZStream[Consumer, Throwable, (StampedEvent, Offset)] =
    ZStream.serviceWithStream[Consumer] { consumer =>
      consumer
        .plainStream(
          Subscription.topics(config.kafka.rawTopic),
          Serde.byteArray,
          Serde.byteArray
        )
        .mapZIO { (record: CommittableRecord[Array[Byte], Array[Byte]]) =>
          ZIO.attempt {
            // Capture timings before the parse: t1 is the broker timestamp,
            // tSrc is "now" at the source boundary. The parse cost itself
            // is included in t_src → transform_start (queue_ms).
            val t1Ms = record.record.timestamp()
            val tSrcMs = java.lang.System.currentTimeMillis()

            val proto = Telemetry.parseFrom(record.record.value())
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

            (StampedEvent(event, t1Ms, tSrcMs), record.offset)
          }
        }
    }
