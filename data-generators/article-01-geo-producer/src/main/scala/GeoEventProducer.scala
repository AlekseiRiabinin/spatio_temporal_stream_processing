import java.util.Properties
import java.util.UUID
import scala.util.Random
import java.time.Instant

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object GeoEventProducer {

  case class GeoEvent(
    id: String,
    wkt: String,
    timestamp: Long,
    attributes: Map[String, String]
  ) {
    def toJson: String =
      s"""{
         |  "id": "$id",
         |  "wkt": "$wkt",
         |  "timestamp": $timestamp,
         |  "attributes": {
         |    "speed": "${attributes("speed")}"
         |  }
         |}""".stripMargin
  }

  private def randomPoint(): String = {
    val lon = 30 + Random.nextDouble() * 20
    val lat = 50 + Random.nextDouble() * 10
    f"POINT($lon%.6f $lat%.6f)"
  }

  private def randomSpeed(): String =
    (20 + Random.nextInt(80)).toString

  def main(args: Array[String]): Unit = {

    val topic = sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")
    val bootstrap = sys.env.getOrElse(
      "KAFKA_BOOTSTRAP_SERVERS",
      "kafka-1:19092,kafka-2:19094"
    )
    val rate = sys.env.getOrElse("EVENT_RATE", "1000").toInt.max(1)

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("Shutting down GeoEventProducer...")
      producer.flush()
      producer.close()
    }))

    println(s"GeoEventProducer started → topic=$topic, rate=$rate events/sec")

    val intervalMs = 1000.0 / rate

    // --- Throughput counters ---
    var counter = 0L
    var lastReport = System.currentTimeMillis()

    while (true) {
      val event = GeoEvent(
        id = UUID.randomUUID().toString,
        wkt = randomPoint(),
        timestamp = Instant.now().toEpochMilli,
        attributes = Map("speed" -> randomSpeed())
      )

      val record = new ProducerRecord[String, String](topic, event.id, event.toJson)
      producer.send(record)

      counter += 1
      val now = System.currentTimeMillis()

      // Print throughput every second
      if (now - lastReport >= 1000) {
        println(s"[producer-throughput] eps=$counter")
        counter = 0
        lastReport = now
      }

      Thread.sleep(intervalMs.toLong)
    }
  }
}
