package phd.streammodels.producer

import java.util.{Properties, UUID}
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object GeoEventProducer {

  def main(args: Array[String]): Unit = {

    val topic = sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")

    val keys = sys.env.getOrElse("KEYS", "10").toInt.max(1)

    val ratePattern = RatePattern.fromEnv()
    val geomPattern = GeometryPattern.fromEnv()
    val tsPattern = TimestampPattern.fromEnv()

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)
    val rand = new Random()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("Shutting down article-02 GeoEventProducer...")
      producer.flush()
      producer.close()
    }))

    println(
      s"""article-02 GeoEventProducer started:
         |  topic              = $topic
         |  bootstrap          = $bootstrap
         |  KEYS               = $keys
         |  EVENT_RATE_PATTERN = ${sys.env.getOrElse("EVENT_RATE_PATTERN", "constant")}
         |  EVENT_RATE         = ${sys.env.getOrElse("EVENT_RATE", "50")}
         |  BURST_RATE         = ${sys.env.getOrElse("BURST_RATE", "100")}
         |  PAUSE_MS           = ${sys.env.getOrElse("PAUSE_MS", "5000")}
         |  WAVE_MIN           = ${sys.env.getOrElse("WAVE_MIN", "5")}
         |  WAVE_MAX           = ${sys.env.getOrElse("WAVE_MAX", "100")}
         |  WAVE_PERIOD_MS     = ${sys.env.getOrElse("WAVE_PERIOD_MS", "10000")}
         |  GEOMETRY_PATTERN   = ${sys.env.getOrElse("GEOMETRY_PATTERN", "random")}
         |  TIMESTAMP_PATTERN  = ${sys.env.getOrElse("TIMESTAMP_PATTERN", "realtime")}
         |""".stripMargin
    )

    var counter = 0L
    var lastReport = System.currentTimeMillis()

    while (true) {
      val nowFirst = System.currentTimeMillis()

      // Kafka partition key
      val key = s"veh-${rand.nextInt(keys)}"

      val wkt = geomPattern.nextWkt(rand)
      val ts = tsPattern.nextTimestamp(nowFirst, rand)

      val event = GeoEvent(
        id = UUID.randomUUID().toString,
        wkt = wkt,
        timestamp = ts,
        attributes = Map(
          "speed" -> (20 + rand.nextInt(80)).toString
        )
      )

      val record = new ProducerRecord[String, String](topic, key, event.toJson)
      producer.send(record)

      counter += 1
      val nowSecond = System.currentTimeMillis()
      if (nowSecond - lastReport >= 1000) {
        println(s"[producer-throughput] eps=$counter")
        counter = 0
        lastReport = nowSecond
      }

      val sleepMs = ratePattern.nextIntervalMs(nowSecond)
      Thread.sleep(math.max(1L, sleepMs))
    }
  }
}
