package phd.spatialmethods.producer

import java.util.{Properties, UUID}
import java.time.Instant
import scala.util.Random
import scala.collection.mutable

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object GeoEventProducer {

  private sealed trait MotionMode
  private case object Straight extends MotionMode
  private case object RandomWalk extends MotionMode
  private case object Swarm extends MotionMode
  private case object Collision extends MotionMode

  private case class ObjectState(
    lon: Double,
    lat: Double,
    speed: Double,   // m/s
    heading: Double  // degrees
  )

  private val EarthRadiusMeters = 6371000.0

  private def deg2rad(d: Double): Double = d * math.Pi / 180.0
  private def rad2deg(r: Double): Double = r * 180.0 / math.Pi

  private def move(
    state: ObjectState,
    dtSeconds: Double,
    rand: Random,
    mode: MotionMode
): ObjectState = {
    val speed = mode match {
      case Straight =>
        state.speed
      case RandomWalk =>
        (state.speed + rand.nextGaussian() * 1.0).max(1.0).min(40.0)
      case Swarm =>
        (state.speed + rand.nextGaussian() * 0.5).max(2.0).min(15.0)
      case Collision =>
        state.speed
    }

    val heading = mode match {
      case Straight =>
        state.heading
      case RandomWalk =>
        (state.heading + rand.nextGaussian() * 10.0 + 360.0) % 360.0
      case Swarm =>
        (state.heading + rand.nextGaussian() * 5.0 + 360.0) % 360.0
      case Collision =>
        state.heading
    }

    val distance = speed * dtSeconds
    val headingRad = deg2rad(heading)

    val dLat = (distance * math.cos(headingRad)) / EarthRadiusMeters
    val dLon = (distance * math.sin(headingRad)) / (EarthRadiusMeters * math.cos(deg2rad(state.lat)))

    val newLat = state.lat + rad2deg(dLat)
    val newLon = state.lon + rad2deg(dLon)

    state.copy(lon = newLon, lat = newLat, speed = speed, heading = heading)
  }

  private def parseMotionMode(): MotionMode = {
    sys.env.getOrElse("MOTION_MODE", "straight").toLowerCase match {
      case "straight"      => Straight
      case "random_walk"   => RandomWalk
      case "swarm"         => Swarm
      case "collision"     => Collision
      case other =>
        println(s"[MotionMode] Unknown '$other', using straight")
        Straight
    }
  }

  def main(args: Array[String]): Unit = {

    val topic = sys.env.getOrElse("KAFKA_TOPIC", "geo-events-topic")
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val keys = sys.env.getOrElse("KEYS", "50").toInt.max(1)

    val ratePattern = RatePattern.fromEnv()
    val geomPattern = GeometryPattern.fromEnv()
    val tsPattern = TimestampPattern.fromEnv()
    val motionMode = parseMotionMode()

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)
    val rand = new Random()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("Shutting down article-03 GeoEventProducer...")
      producer.flush()
      producer.close()
    }))

    println(
      s"""article-03 GeoEventProducer started:
         |  topic        = $topic
         |  bootstrap    = $bootstrap
         |  KEYS         = $keys
         |  MOTION_MODE  = ${sys.env.getOrElse("MOTION_MODE", "straight")}
         |""".stripMargin
    )

    val stateByObject = mutable.Map.empty[String, ObjectState]
    var lastTsByObject = mutable.Map.empty[String, Long]

    // For swarm / collision, define some structure
    val swarmCenter = (55.27, 25.20) // Dubai-ish
    val collisionPair = ("obj-0", "obj-1")

    var counter = 0L
    var lastReport = System.currentTimeMillis()

    while (true) {
      val now = System.currentTimeMillis()

      val objectId = s"obj-${rand.nextInt(keys)}"

      val prevState = stateByObject.getOrElseUpdate(
        objectId, {
          val (lon0, lat0) = motionMode match {
            case Swarm =>
              // start near swarm center
              val baseLon = swarmCenter._1
              val baseLat = swarmCenter._2
              (baseLon + rand.nextGaussian() * 0.01, baseLat + rand.nextGaussian() * 0.01)
            case Collision =>
              // two objects on collision course
              if (objectId == collisionPair._1)
                (55.26, 25.20)
              else if (objectId == collisionPair._2)
                (55.28, 25.20)
              else
                geomPattern.initialPoint(rand)
            case _ =>
              geomPattern.initialPoint(rand)
          }

          val baseSpeed = motionMode match {
            case Straight   => 15.0 + rand.nextDouble() * 10.0
            case RandomWalk => 10.0 + rand.nextDouble() * 15.0
            case Swarm      => 8.0 + rand.nextDouble() * 5.0
            case Collision  =>
              if (objectId == collisionPair._1) 20.0
              else if (objectId == collisionPair._2) 20.0
              else 10.0 + rand.nextDouble() * 10.0
          }

          val baseHeading = motionMode match {
            case Straight   => rand.nextDouble() * 360.0
            case RandomWalk => rand.nextDouble() * 360.0
            case Swarm      => 90.0 + rand.nextGaussian() * 20.0
            case Collision  =>
              if (objectId == collisionPair._1) 90.0   // east
              else if (objectId == collisionPair._2) 270.0 // west
              else rand.nextDouble() * 360.0
          }

          ObjectState(lon0, lat0, baseSpeed, baseHeading)
        }
      )

      val lastTs = lastTsByObject.getOrElse(objectId, now - 1000)
      val dtSeconds = (now - lastTs).max(1L) / 1000.0

      val newState = move(prevState, dtSeconds, rand, motionMode)
      stateByObject.update(objectId, newState)
      lastTsByObject.update(objectId, now)

      val tsInstant: Instant = tsPattern.nextInstant(now, rand)
      val wkt = f"POINT(${newState.lon}%.6f ${newState.lat}%.6f)"

      val event = GeoEvent(
        id = UUID.randomUUID().toString,
        objectId = objectId,
        timestamp = tsInstant,
        lon = newState.lon,
        lat = newState.lat,
        wkt = wkt,
        speed = newState.speed,
        heading = newState.heading,
        attributes = Map(
          "mode" -> motionMode.toString.toLowerCase,
          "source" -> "article-03-producer"
        )
      )

      val record = new ProducerRecord[String, String](topic, objectId, event.toJson)
      producer.send(record)

      counter += 1
      val now2 = System.currentTimeMillis()
      if (now2 - lastReport >= 1000) {
        println(s"[producer-throughput] eps=$counter")
        counter = 0
        lastReport = now2
      }

      val sleepMs = ratePattern.nextIntervalMs(now2)
      Thread.sleep(math.max(1L, sleepMs))
    }
  }
}
