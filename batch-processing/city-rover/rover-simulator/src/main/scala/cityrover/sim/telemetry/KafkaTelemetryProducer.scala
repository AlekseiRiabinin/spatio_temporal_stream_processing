package cityrover.sim.telemetry

import cityrover.sim.model.TelemetryEvent
import io.circe.syntax._
import io.circe.generic.auto._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.Callback

import java.util.Properties


/**
  * KafkaTelemetryProducer sends TelemetryEvent JSON messages
  * to the Kafka topic defined in application.conf.
  *
  * Uses Kafka 3.8.0 client API.
  * Optimized for high‑frequency rover telemetry streams.
  */
class KafkaTelemetryProducer(bootstrapServers: String, topic: String) {

  private val props = new Properties()

  props.put("bootstrap.servers", bootstrapServers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // Recommended for telemetry streams
  props.put("enable.idempotence", "true")
  props.put("acks", "all")
  props.put("compression.type", "lz4")
  props.put("linger.ms", "5")
  props.put("batch.size", "32768")
  props.put("max.in.flight.requests.per.connection", "1") // strict ordering

  private val producer = new KafkaProducer[String, String](props)

  /** Serialize TelemetryEvent → JSON and send to Kafka asynchronously. */
  def send(event: TelemetryEvent): Unit = {
    val json = event.asJson.noSpaces
    val record = new ProducerRecord[String, String](topic, event.roverId, json)

    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          System.err.println(s"[KafkaTelemetryProducer] Failed to send event: ${exception.getMessage}")
        }
      }
    })

    // Low-latency telemetry: flush after each tick
    producer.flush()
  }

  /** Close producer gracefully. */
  def close(): Unit = {
    producer.flush()
    producer.close()
  }
}
