package phd.adaptivecontrol

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

import java.time.Duration
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.pipeline.AdaptiveProcessingPipeline


/**
  * Article04AdaptiveControlJob
  *
  * Main Flink entry point for:
  *   - adaptive window control
  *   - adaptive watermark control
  *   - spatio-temporal stream processing
  *
  * Article 04:
  * Adaptive Window and Watermark Control
  * for Real-Time Spatio-Temporal Stream Processing
  */
object Article04AdaptiveControlJob {

  // ============================================================
  // JSON Mapper
  // ============================================================
  private val mapper = new ObjectMapper()

  mapper.registerModule(DefaultScalaModule)

  // ============================================================
  // Main
  // ============================================================
  def main(args: Array[String]): Unit = {

    println(
      "[MAIN] action=start job=Article04AdaptiveControlJob"
    )

    // ============================================================
    // 1. Flink Environment
    // ============================================================
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    println(
      s"[MAIN] action=env parallelism=${env.getParallelism}"
    )

    // ============================================================
    // 2. Kafka Configuration
    // ============================================================
    val bootstrap =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")

    val topic =
      sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")

    println(
      s"[MAIN] action=kafkaConfig bootstrap=$bootstrap topic=$topic"
    )

    val kafkaProps = new Properties()

    kafkaProps.setProperty("bootstrap.servers", bootstrap)
    kafkaProps.setProperty("group.id", "article04-adaptive-control")

    val kafkaConsumer =
      new FlinkKafkaConsumer[String](
        topic,
        new SimpleStringSchema(),
        kafkaProps
      )

    // ============================================================
    // 3. Deserialize GeoEvents
    // ============================================================
    println(
      "[MAIN] action=deserialization status=starting"
    )

    implicit val geoEventTypeInfo:
      TypeInformation[GeoEvent] =
        createTypeInformation[GeoEvent]

    val geoEventStream: DataStream[GeoEvent] =
      env
        .addSource(kafkaConsumer)
        .map(json => mapper.readValue(json, classOf[GeoEvent]))
        .filter(_.isValid)

    println(
      "[MAIN] action=deserialization status=ready"
    )

    // ============================================================
    // 4. Watermark Strategy
    // ============================================================
    val watermarkDelayMs =
      sys.env
        .getOrElse("WATERMARK_DELAY_MS", "3000")
        .toLong

    println(
      s"[MAIN] action=watermarkStrategy delayMs=$watermarkDelayMs"
    )

    val watermarkStrategy =
      WatermarkStrategy
        .forBoundedOutOfOrderness[GeoEvent](Duration.ofMillis(watermarkDelayMs))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[GeoEvent] {

            override def extractTimestamp(
              event: GeoEvent,
              recordTimestamp: Long
            ): Long = {

              event.timestamp
            }
          }
        )

    val timedGeoEventStream =
      geoEventStream.assignTimestampsAndWatermarks(watermarkStrategy)

    // ============================================================
    // 5. Adaptive Processing Pipeline
    // ============================================================
    println(
      "[MAIN] action=pipelineInit status=starting"
    )

    val processedStream =
      AdaptiveProcessingPipeline.buildPipeline(env, timedGeoEventStream)

    println(
      "[MAIN] action=pipelineInit status=ready"
    )

    // ============================================================
    // 6. Output
    // ============================================================
    println(
      "[MAIN] action=output status=printing"
    )

    processedStream
      .map(result => mapper.writeValueAsString(result))
      .print()

    // ============================================================
    // 7. Execute
    // ============================================================
    println(
      "[MAIN] action=execute job=Article04AdaptiveControlJob"
    )

    env.execute(
      "Article 04: Adaptive Window and Watermark Control"
    )
  }
}
