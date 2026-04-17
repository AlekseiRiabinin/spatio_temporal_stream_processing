package phd.spatialmethods.producer

import java.util.{Properties, UUID}
import scala.util.Random
import scala.collection.mutable

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object GeoEventProducer {

  // ============================================================
  //  Service Area Configuration (1.2 km radius around a hub)
  // ============================================================

  private val hubLat = 59.9391     // Example: Nevsky Prospekt (store location)
  private val hubLon = 30.3158
  private val serviceRadiusMeters = 1200.0  // 1.2 km realistic delivery radius

  private def metersToLat(m: Double): Double =
    m / 111320.0

  private def metersToLon(lat: Double, m: Double): Double =
    m / (111320.0 * math.cos(math.toRadians(lat)))

  // ============================================================
  //  Geometry Patterns (Realistic Service Area)
  // ============================================================

  private trait GeometryPattern {
    def initialPoint(rand: Random): (Double, Double)
  }

  private case class RandomDiskRegion(
    hubLat: Double,
    hubLon: Double,
    radiusMeters: Double
  ) extends GeometryPattern {

    override def initialPoint(rand: Random): (Double, Double) = {
      val u = rand.nextDouble()
      val v = rand.nextDouble()

      val r = radiusMeters * math.sqrt(u)
      val theta = 2 * math.Pi * v

      val dx = r * math.cos(theta)
      val dy = r * math.sin(theta)

      val lat = hubLat + metersToLat(dy)
      val lon = hubLon + metersToLon(hubLat, dx)

      (lon, lat)
    }
  }

  private case class ClusteredAroundHub(
    hubLat: Double,
    hubLon: Double,
    stdDevMeters: Double
  ) extends GeometryPattern {

    override def initialPoint(rand: Random): (Double, Double) = {
      val dx = rand.nextGaussian() * stdDevMeters
      val dy = rand.nextGaussian() * stdDevMeters

      val lat = hubLat + metersToLat(dy)
      val lon = hubLon + metersToLon(hubLat, dx)

      (lon, lat)
    }
  }

  private object GeometryPattern {
    def fromEnv(): GeometryPattern = {
      sys.env.getOrElse("GEOMETRY_PATTERN", "random").toLowerCase match {

        case "random" =>
          RandomDiskRegion(hubLat, hubLon, serviceRadiusMeters)

        case "clustered" =>
          ClusteredAroundHub(hubLat, hubLon, stdDevMeters = serviceRadiusMeters / 2.0)

        case other =>
          println(s"[GeometryPattern] Unknown '$other', using random")
          RandomDiskRegion(hubLat, hubLon, serviceRadiusMeters)
      }
    }
  }

  // ============================================================
  //  Motion Modes
  // ============================================================

  private sealed trait MotionMode
  private case object Straight extends MotionMode
  private case object RandomWalk extends MotionMode
  private case object Swarm extends MotionMode
  private case object Collision extends MotionMode

  private case class ObjectState(
    lon: Double,
    lat: Double,
    speed: Double,
    heading: Double
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
        (state.speed + rand.nextGaussian() * 0.3).max(1.0).min(2.0)
      case Swarm =>
        (state.speed + rand.nextGaussian() * 0.2).max(0.7).min(1.5)
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
    val dLon = (distance * math.sin(headingRad)) /
      (EarthRadiusMeters * math.cos(deg2rad(state.lat)))

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

  // ============================================================
  //  Main Producer Loop
  // ============================================================

  def main(args: Array[String]): Unit = {

    val topic = sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")
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

    // Collision mode: define multiple head-on pairs
    val collisionPairs: Seq[(String, String)] =
      (0 until math.min(20, keys - (keys % 2)) by 2).map(i => (s"obj-$i", s"obj-${i + 1}"))

    def isFirstOfPair(id: String): Boolean =
      collisionPairs.exists(_._1 == id)

    def isSecondOfPair(id: String): Boolean =
      collisionPairs.exists(_._2 == id)

    // Collision cluster center (kept inside service area)
    val collisionCenterLon = hubLon
    val collisionCenterLat = hubLat
    val collisionOffsetDeg = 0.00003  // ~2–3m separation

    var counter = 0L
    var lastReport = System.currentTimeMillis()

    while (true) {
      val now = System.currentTimeMillis()
      val objectId = s"obj-${rand.nextInt(keys)}"

      val prevState = stateByObject.getOrElseUpdate(
        objectId, {
          val (lon0, lat0) = motionMode match {

            case Swarm =>
              geomPattern.initialPoint(rand)

            case Collision =>
              if (isFirstOfPair(objectId))
                (collisionCenterLon, collisionCenterLat)
              else if (isSecondOfPair(objectId))
                (collisionCenterLon + collisionOffsetDeg, collisionCenterLat)
              else
                geomPattern.initialPoint(rand)

            case _ =>
              geomPattern.initialPoint(rand)
          }

          val baseSpeed = motionMode match {
            case Straight =>
              1.5 + rand.nextDouble() * 1.0
            case RandomWalk =>
              1.0 + rand.nextDouble() * 1.0
            case Swarm =>
              0.7 + rand.nextDouble() * 0.8
            case Collision =>
              if (isFirstOfPair(objectId) || isSecondOfPair(objectId))
                1.5 + rand.nextDouble() * 0.7
              else
                1.0 + rand.nextDouble() * 1.0
          }

          val baseHeading = motionMode match {
            case Straight =>
              rand.nextDouble() * 360
            case RandomWalk =>
              rand.nextDouble() * 360
            case Swarm =>
              90 + rand.nextGaussian() * 20
            case Collision =>
              if (isFirstOfPair(objectId)) 0.0
              else if (isSecondOfPair(objectId)) 180.0
              else rand.nextDouble() * 360
          }

          ObjectState(lon0, lat0, baseSpeed, baseHeading)
        }
      )

      val lastTs = lastTsByObject.getOrElse(objectId, now - 1000)
      val dtSeconds = (now - lastTs).max(1L) / 1000.0

      val newState = move(prevState, dtSeconds, rand, motionMode)
      stateByObject.update(objectId, newState)
      lastTsByObject.update(objectId, now)

      val tsMillis: Long = tsPattern.nextInstant(now, rand).toEpochMilli
      val wkt = f"POINT(${newState.lon}%.6f ${newState.lat}%.6f)"

      val event = GeoEvent(
        id = UUID.randomUUID().toString,
        objectId = objectId,
        timestamp = tsMillis,
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
