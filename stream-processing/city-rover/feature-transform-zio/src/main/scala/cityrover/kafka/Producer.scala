package cityrover.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import zio._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde

import cityrover.model.{EnrichedEvent, toProtobuf}
import cityrover.util.AppConfig


object Producer:

  /** Serde for `EnrichedEvent`.
    *
    * Direction is `EnrichedEvent <-> Array[Byte]`:
    *   - first lambda  (serialize)   : EnrichedEvent => Array[Byte], via `toProtobuf`
    *   - second lambda (deserialize) : Array[Byte] => EnrichedEvent, unused by the producer
    *
    * The producer never deserializes; `Producer.make` requires a symmetric
    * `Serde`, so the deserializer is a fail-fast stub. The writer job is the
    * only consumer of `EnrichedTelemetryEvent` and handles the real parse.
    */
  val enrichedSerde: Serde[Any, EnrichedEvent] =
    Serde.byteArray.inmap[EnrichedEvent](
      // decode: Array[Byte] => EnrichedEvent   (unused by the producer, fail-fast stub)
      _ => throw new UnsupportedOperationException(
        "EnrichedEvent deserialization is not used by the producer; the writer job handles it"
      )
    )(
      // encode: EnrichedEvent => Array[Byte]   (used by produceChunk)
      event => event.toProtobuf.toByteArray
    )

  /** Key serde: `roverId` as UTF-8 bytes. Same as the Flink job's
    * `RoverIdKeySchema` — same rover always hashes to the same partition.
    */
  val roverIdSerde: Serde[Any, String] = Serde.string

  /** Build `ProducerSettings` from the full `AppConfig`.
    *
    * Producer tuning knobs from `KafkaProducerConfig` are applied here.
    * `bootstrapServers` is a comma-separated HOCON string; zio-kafka
    * requires a `List[String]`.
    */
  def settings(config: AppConfig): ProducerSettings =
    ProducerSettings(config.kafka.bootstrapServers.split(",").toList)
      .withProperty("acks", config.kafka.producer.acks)
      .withProperty("linger.ms", config.kafka.producer.lingerMs.toString)
      .withProperty("compression.type", config.kafka.producer.compressionType)
      .withProperty("retries", config.kafka.producer.retries.toString)
      .withProperty(
        "max.in.flight.requests.per.connection",
        config.kafka.producer.maxInFlightRequestsPerConnection.toString
      )
      .withClientId("feature-transform-zio-producer")

  /** Produce a chunk of enriched events to the enriched topic.
    *
    * Key = `roverId`, value = protobuf bytes. Uses `produceChunk` (batch)
    * rather than `produce` per record — at the 5-minute boundary, up to
    * four events per rover fire simultaneously, and batching avoids
    * per-record round-trips.
    *
    * Returns the `RecordMetadata` for each produced record, so the caller
    * (`Main`) can log or count delivery results.
    */
  def produceChunk(
    config: AppConfig,
    events: Chunk[EnrichedEvent]
  ): ZIO[Producer, Throwable, Chunk[RecordMetadata]] =
    if events.isEmpty then ZIO.succeed(Chunk.empty)
    else
      val records: Chunk[ProducerRecord[String, EnrichedEvent]] =
        events.map { event =>
          new ProducerRecord[String, EnrichedEvent](
            config.kafka.enrichedTopic,
            event.envelope.roverId,   // key = roverId
            event
          )
        }

      ZIO.serviceWithZIO[Producer] { producer =>
        producer.produceChunk(records, roverIdSerde, enrichedSerde)
      }
