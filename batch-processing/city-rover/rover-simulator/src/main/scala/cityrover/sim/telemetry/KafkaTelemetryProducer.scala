package cityrover.sim.telemetry

import cityrover.sim.model.TelemetryEvent
import io.circe.syntax._
import io.circe.generic.auto._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties


/**
  * KafkaTelemetryProducer sends TelemetryEvent JSON messages
  * to the Kafka topic defined in application.conf.
  *
  * Uses Kafka 3.8.0 client API.
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

  private val producer = new KafkaProducer[String, String](props)

  /** Serialize TelemetryEvent → JSON and send to Kafka. */
  def send(event: TelemetryEvent): Unit = {
    val json = event.asJson.noSpaces
    val record = new ProducerRecord[String, String](topic, event.roverId, json)
    producer.send(record)
  }

  /** Close producer gracefully. */
  def close(): Unit = {
    producer.flush()
    producer.close()
  }
}
