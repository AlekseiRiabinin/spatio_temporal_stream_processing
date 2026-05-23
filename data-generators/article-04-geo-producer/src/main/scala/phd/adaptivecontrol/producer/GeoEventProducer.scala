package phd.adaptivecontrol.producer

import java.util.Properties
import java.util.UUID

import scala.collection.mutable
import scala.util.Random

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import phd.adaptivecontrol.producer.geometry.GeometryPattern
import phd.adaptivecontrol.producer.model.GeoEvent
import phd.adaptivecontrol.producer.model.ObjectState
import phd.adaptivecontrol.producer.motion._
import phd.adaptivecontrol.producer.rate.RatePattern
import phd.adaptivecontrol.producer.timestamp._
import phd.adaptivecontrol.producer.util.GeoUtils


object GeoEventProducer {

  // ============================================================
  //  Service Area Configuration
  // ============================================================
  private val hubLat = 59.9391
  private val hubLon = 30.3158
  private val serviceRadiusMeters = 1200.0


  // ============================================================
  //  Collision Configuration
  // ============================================================
  private val collisionOffsetMeters = 3.0


  // ============================================================
  //  Main Producer
  // ============================================================
  def main(args: Array[String]): Unit = {

    val topic =
      sys.env.getOrElse(
        "KAFKA_TOPIC",
        "spatio-temporal-events"
      )

    val bootstrapServers =
      sys.env.getOrElse(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-1:19092"
      )

    val numberOfObjects =
      sys.env
        .getOrElse("KEYS", "50")
        .toInt
        .max(1)

    val geometryPattern =
      GeometryPattern.fromEnv()

    val ratePattern =
      RatePattern.fromEnv()

    val timestampPattern =
      TimestampPattern.fromEnv()

    val motionMode =
      MotionMode.fromEnv()

    val rand = new Random()

    // ============================================================
    //  Kafka Producer
    // ============================================================
    val kafkaProps = new Properties()

    kafkaProps.put("bootstrap.servers", bootstrapServers)
    kafkaProps.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProps.put("value.serializer", classOf[StringSerializer].getName)
    kafkaProps.put("acks", "all")

    val producer =
      new KafkaProducer[String, String](kafkaProps)

    Runtime.getRuntime.addShutdownHook(
      new Thread(() => {

        println("[GeoEventProducer] shutting down...")

        producer.flush()
        producer.close()
      })
    )

    // ============================================================
    //  Runtime State
    // ============================================================
    val stateByObject =
      mutable.Map.empty[String, ObjectState]

    val collisionPairs =
      buildCollisionPairs(numberOfObjects)

    // ============================================================
    //  Startup Log
    // ============================================================

    println(
      s"""
         |==================================================
         | article-04 GeoEventProducer started
         |==================================================
         | topic              = $topic
         | bootstrapServers   = $bootstrapServers
         | objects            = $numberOfObjects
         | motionMode         = ${motionMode.name}
         | geometryPattern    = ${sys.env.getOrElse("GEOMETRY_PATTERN", "random")}
         | timestampPattern   = ${sys.env.getOrElse("TIMESTAMP_PATTERN", "realtime")}
         |==================================================
         |""".stripMargin
    )

    // ============================================================
    //  Throughput Monitoring
    // ============================================================
    var eventCounter = 0L
    var lastReportTimestamp = System.currentTimeMillis()

    // ============================================================
    //  Main Loop
    // ============================================================
    while (true) {

      val now =
        System.currentTimeMillis()

      val objectId =
        s"obj-${rand.nextInt(numberOfObjects)}"

      val previousState =
        stateByObject.getOrElseUpdate(
          objectId,
          initializeState(
            objectId,
            rand,
            geometryPattern,
            motionMode,
            collisionPairs
          )
        )

      // ============================================================
      //  Motion Simulation
      // ============================================================
      val updatedState =
        MotionSimulator.move(
          state = previousState,
          currentTimestamp = now,
          rand = rand,
          mode = motionMode
        )

      stateByObject.update(objectId, updatedState)

      // ============================================================
      //  Event-Time Generation
      // ============================================================
      val eventTimestamp =
        timestampPattern
          .nextTimestamp(now, rand)
          .toEpochMilli

      // ============================================================
      //  GeoEvent Creation
      // ============================================================
      val event =
        GeoEvent(
          id = UUID.randomUUID().toString,
          objectId = objectId,
          timestamp = eventTimestamp,
          lon = updatedState.lon,
          lat = updatedState.lat,
          wkt = f"POINT(${updatedState.lon}%.6f ${updatedState.lat}%.6f)",
          speed = updatedState.speed,
          heading = updatedState.heading,
          attributes = Map(
            "motionMode" -> motionMode.name,
            "source" -> "article-04-producer"
          )
        )

      // ============================================================
      //  Temporal Disorder Injection
      // ============================================================
      val disorderedEventOpt =
        DisorderSimulator.applyCompositeDisorder(event, rand)

      disorderedEventOpt.foreach { disorderedEvent =>

        val finalEvent =
          TimestampSkewInjector.applySkew(disorderedEvent, rand)

        // ============================================================
        //  Kafka Publish
        // ============================================================
        val record =
          new ProducerRecord[String, String](topic, objectId, finalEvent.toJson)

        producer.send(record)

        eventCounter += 1
      }

      // ============================================================
      //  Throughput Logging
      // ============================================================
      val currentTime =
        System.currentTimeMillis()

      if (currentTime - lastReportTimestamp >= 1000) {

        println(s"[producer-throughput] eps=$eventCounter")

        eventCounter = 0
        lastReportTimestamp = currentTime
      }

      // ============================================================
      //  Dynamic Rate Control
      // ============================================================
      val sleepMs =
        ratePattern.nextIntervalMs(currentTime)

      Thread.sleep(math.max(1L, sleepMs))
    }
  }


  // ============================================================
  //  Initial Object State
  // ============================================================
  private def initializeState(
    objectId: String,
    rand: Random,
    geometryPattern: GeometryPattern,
    motionMode: MotionMode,
    collisionPairs: Seq[(String, String)]
  ): ObjectState = {

    val (lon0, lat0) = {

      motionMode match {

        case CollisionMotion =>

          if (collisionPairs.exists(_._1 == objectId)) {

            (
              hubLon,
              hubLat
            )

          } else if (collisionPairs.exists(_._2 == objectId)) {

            (
              hubLon + GeoUtils.metersToLongitudeDegrees(hubLat, collisionOffsetMeters),
              hubLat
            )

          } else {

            geometryPattern.initialPoint(rand)
          }

        case _ =>
          geometryPattern.initialPoint(rand)
      }
    }

    val speed =
      motionMode match {

        case StraightMotion =>
          1.5 + rand.nextDouble()

        case RandomWalkMotion =>
          1.0 + rand.nextDouble()

        case SwarmMotion =>
          0.7 + rand.nextDouble() * 0.8

        case CollisionMotion =>
          1.5 + rand.nextDouble() * 0.5
      }

    val heading =
      motionMode match {

        case SwarmMotion =>
          90 + rand.nextGaussian() * 15

        case CollisionMotion =>

          if (collisionPairs.exists(_._1 == objectId))
            0.0
          else if (collisionPairs.exists(_._2 == objectId))
            180.0
          else
            rand.nextDouble() * 360

        case _ =>
          rand.nextDouble() * 360
      }

    ObjectState(
      lon = lon0,
      lat = lat0,
      speed = speed,
      heading = heading,
      lastTimestamp = System.currentTimeMillis()
    )
  }


  // ============================================================
  //  Collision Pair Builder
  // ============================================================
  private def buildCollisionPairs(numberOfObjects: Int): Seq[(String, String)] = {

    (0 until math.min(20, numberOfObjects - (numberOfObjects % 2)) by 2)
      .map(i => (s"obj-$i", s"obj-${i + 1}"))
  }
}
