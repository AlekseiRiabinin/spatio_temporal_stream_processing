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

import cityrover.telemetry.{EnrichedTelemetryEvent, WindowType}
import cityrover.util.AppConfig


/** Consumes enriched telemetry from the Kafka enriched topic and parses
  * it into `EnrichedTelemetryEvent`.
  *
  * Offset commits are intentionally NOT performed here. `Main` commits
  * the enriched-topic offset only after the Cassandra write succeeds —
  * that's the at-least-once contract. Committing here would lose writes
  * on crash.
  *
  * Validation happens inline at parse time: a message where `windowType`
  * and the populated `oneof features` variant disagree (or where the
  * oneof is empty, or `roverId` is empty) is rejected with a clear
  * exception. The failure propagates through the stream and fails the
  * pipeline — which is the right behavior for a malformed message. If
  * we'd rather skip-and-count, replace the `throw` with a `None` and
  * `collectSome`.
  *
  * Timing context:
  *   - `t1Ms`   : Kafka broker timestamp (ms). May be -1 if the broker
  *                doesn't set one; metrics needing it are skipped.
  *   - `tSrcMs` : wall clock at the source boundary (ms), captured
  *                before the protobuf parse so it reflects when the
  *                record left Kafka, not when parsing finished.
  */
object Consumer:

  /** Build `ConsumerSettings` from `AppConfig`. */
  def settings(config: AppConfig): ConsumerSettings =
    ConsumerSettings(config.kafka.bootstrapServers.split(",").toList)
      .withGroupId(config.kafka.groupId)
      .withProperty("auto.offset.reset", config.kafka.consumer.offsetReset)
      .withProperty("fetch.min.bytes", config.kafka.consumer.fetchMinBytes.toString)
      .withProperty("fetch.max.wait.ms", config.kafka.consumer.fetchMaxWaitMs.toString)
      .withProperty("isolation.level", config.kafka.consumer.isolationLevel)
      .withProperty("enable.auto.commit", "false")
      .withClientId("feature-writer-zio")

  /** Enriched telemetry stream paired with its Kafka offset and timing.
    *
    * The `Consumer` requirement is left in the environment so `Main`
    * can compose the consumer layer once. Offset is committed downstream
    * (after the Cassandra write) — see `CassandraSink.write`.
    *
    * `t1Ms` and `tSrcMs` are captured here so `Main` can record
    * `kafka_to_zio_ms` (broker → parse) and `latency_ms` (broker →
    * Cassandra completion). This mirrors the transform job's
    * `StampedEvent` pattern — a small wrapper in an untyped tuple
    * because the writer's pipeline is a single `mapZIO` and doesn't
    * need a full case class.
    */
  def streamWithOffsets(
    config: AppConfig
  ): ZStream[Consumer, Throwable, (EnrichedTelemetryEvent, Offset, Long, Long)] =
    ZStream.serviceWithStream[Consumer] { consumer =>
      consumer
        .plainStream(
          Subscription.topics(config.kafka.enrichedTopic),
          Serde.byteArray,
          Serde.byteArray
        )
        .mapZIO { (record: CommittableRecord[Array[Byte], Array[Byte]]) =>
          ZIO.attempt {
            // Capture timings before the parse: t1 is the broker
            // timestamp, tSrc is "now" at the source boundary. The
            // parse cost itself is included in t_src → write_start
            // (zio_ms).
            val t1Ms   = record.record.timestamp()
            val tSrcMs = java.lang.System.currentTimeMillis()
            val proto  = parseAndValidate(record.record.value())
            (proto, record.offset, t1Ms, tSrcMs)
          }
        }
    }

  // ---------------------------------------------------------------------------
  // Parse + validate.
  //
  // Rejections are thrown because a malformed message is a bug, not a
  // transient failure. Flink's DeserializationSchema behaves the same
  // way by default (fail the subtask, restart). If we later want
  // skip-and-count during a cutover, switch this to Option and filter.
  // ---------------------------------------------------------------------------

  private def parseAndValidate(bytes: Array[Byte]): EnrichedTelemetryEvent =
    val proto =
      try EnrichedTelemetryEvent.parseFrom(bytes)
      catch
        case e: Throwable =>
          throw new IllegalArgumentException(
            s"Failed to parse EnrichedTelemetryEvent (${bytes.length} bytes)", e
          )

    if proto.roverId.isEmpty then
      throw malformed(proto, "roverId is empty")

    val matches: Boolean =
      proto.features match

        case EnrichedTelemetryEvent.Features.W5S(_)  =>
          proto.windowType == WindowType.WINDOW_5S

        case EnrichedTelemetryEvent.Features.W30S(_) =>
          proto.windowType == WindowType.WINDOW_30S

        case EnrichedTelemetryEvent.Features.W1M(_)  =>
          proto.windowType == WindowType.WINDOW_1M

        case EnrichedTelemetryEvent.Features.W5M(_)  =>
          proto.windowType == WindowType.WINDOW_5M

        case EnrichedTelemetryEvent.Features.Empty   =>
          false

    if !matches then
      throw malformed(
        proto,
        s"windowType=${proto.windowType} " +
          s"disagrees with features=${proto.features.getClass.getSimpleName}"
      )

    proto

  private def malformed(
    proto: EnrichedTelemetryEvent,
    reason: String
  ): IllegalArgumentException =
    new IllegalArgumentException(
      s"Malformed EnrichedTelemetryEvent: $reason " +
      s"(roverId='${proto.roverId}', windowEndTs=${proto.windowEndTs})"
    )
