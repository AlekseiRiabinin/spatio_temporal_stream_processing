package phd.adaptivecontrol

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

import java.time.Duration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.pipeline.AdaptivePipeline


/**
  * Article04AdaptiveControlJob
  *
  * Main Flink entry point for:
  *   - adaptive window control
  *   - adaptive watermark control
  *   - spatio-temporal stream processing
  */
object Article04AdaptiveControlJob {

  // ============================================================
  // JSON Mapper
  // ============================================================
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {

    println("[MAIN] action=start job=Article04AdaptiveControlJob")

    // ============================================================
    // 1. Flink Environment
    // ============================================================
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    println(s"[MAIN] action=env parallelism=${env.getParallelism}")

    // ============================================================
    // 2. CLI-based Experiment Configuration
    // ============================================================
    if (args.length < 5) {
      throw new IllegalArgumentException(
        "Usage: <profile> <rate> <motion> <windowSizeMs> <watermarkDelayMs>"
      )
    }

    val profile = args(0)
    val ratePattern = args(1)
    val motionMode = args(2)

    val windowSizeMs = args(3).toLong
    val watermarkDelayMs = args(4).toLong

    val bootstrap =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")

    val topic =
      sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")

    val adaptiveConfig =
      AdaptiveConfig(
        windowSizeMs = windowSizeMs,
        watermarkDelayMs = watermarkDelayMs
      )

    println(
      s"""
         |[MAIN] action=config
         | profile=$profile
         | ratePattern=$ratePattern
         | motionMode=$motionMode
         | bootstrap=$bootstrap
         | topic=$topic
         | windowSizeMs=$windowSizeMs
         | watermarkDelayMs=$watermarkDelayMs
         |""".stripMargin
    )

    // ============================================================
    // 3. Kafka Source
    // ============================================================
    val kafkaSource =
      KafkaSource.builder[String]()
        .setBootstrapServers(bootstrap)
        .setTopics(topic)
        .setGroupId("article04-adaptive-control")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

    println("[MAIN] action=deserialization status=starting")

    implicit val geoEventTypeInfo: TypeInformation[GeoEvent] =
      createTypeInformation[GeoEvent]

    val rawStream: DataStream[String] =
      env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "KafkaSource"
      )

    val geoEventStream: DataStream[GeoEvent] =
      rawStream
        .map(json => mapper.readValue(json, classOf[GeoEvent]))
        .filter(_.isValid)

    println("[MAIN] action=deserialization status=ready")

    // ============================================================
    // 4. Watermark Strategy (uses CLI config)
    // ============================================================
    object GeoEventTimestampAssigner
      extends SerializableTimestampAssigner[GeoEvent] {

      override def extractTimestamp(
        event: GeoEvent,
        recordTimestamp: Long
      ): Long = event.timestamp
    }

    val watermarkStrategy =
      WatermarkStrategy
        .forBoundedOutOfOrderness[GeoEvent](
          Duration.ofMillis(adaptiveConfig.watermarkDelayMs)
        )
        .withTimestampAssigner(GeoEventTimestampAssigner)

    val timedGeoEventStream =
      geoEventStream.assignTimestampsAndWatermarks(watermarkStrategy)

    println("[MAIN] action=watermarkStrategy status=initialized")

    // ============================================================
    // 5. Adaptive Pipeline (config injected)
    // ============================================================
    println("[MAIN] action=pipelineInit status=starting")

    val processedStream =
      AdaptivePipeline.build(
        env,
        timedGeoEventStream,
        adaptiveConfig
      )

    println("[MAIN] action=pipelineInit status=ready")

    // ============================================================
    // 6. Output
    // ============================================================
    println("[MAIN] action=output status=printing")

    processedStream
      .map(result => mapper.writeValueAsString(result))
      .print()

    // ============================================================
    // 7. Execute
    // ============================================================
    println("[MAIN] action=execute job=Article04AdaptiveControlJob")

    env.execute("Article 04: Adaptive Window and Watermark Control")
  }
}
