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


object Consumer:

  /** Build `ConsumerSettings` from the full `AppConfig`. The `kafka`
    * slice carries `bootstrapServers`, `groupId`, topic names, and the
    * nested consumer tuning block. `bootstrapServers` is a comma-separated
    * string in HOCON; `ConsumerSettings` requires a `List[String]`, so
    * it is split here.
    */
  def settings(config: AppConfig): ConsumerSettings =
    ConsumerSettings(config.kafka.bootstrapServers.split(",").toList)
      .withGroupId(config.kafka.groupId)
      .withProperty("auto.offset.reset", config.kafka.consumer.offsetReset)
      .withProperty("fetch.min.bytes", config.kafka.consumer.fetchMinBytes.toString)
      .withProperty("fetch.max.wait.ms", config.kafka.consumer.fetchMaxWaitMs.toString)
      .withProperty("enable.auto.commit", "false")
      .withClientId("feature-transform-zio")

  /** Raw telemetry stream paired with its Kafka offset.
    *
    * The `Consumer` requirement is intentionally left in the environment
    * (`ZStream[Consumer, ...]`), so `Main` can compose the consumer layer
    * once and reuse it. The `Consumer` instance is what `Main` constructs
    * via `Consumer.make(Consumer.settings(cfg))`.
    *
    * The offset is what `Main` commits after producing the enriched
    * records for that raw event. Keeping it paired with the event (rather
    * than committing here) preserves the at-least-once contract.
    */
  def streamWithOffsets(
    config: AppConfig
  ): ZStream[Consumer, Throwable, (TelemetryEvent, Offset)] =
    ZStream.serviceWithStream[Consumer] { consumer =>
      consumer
        .plainStream(
          Subscription.topics(config.kafka.rawTopic),
          Serde.byteArray,
          Serde.byteArray
        )
        .mapZIO { (record: CommittableRecord[Array[Byte], Array[Byte]]) =>
          ZIO.attempt {
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
            (event, record.offset)
          }
        }
    }
