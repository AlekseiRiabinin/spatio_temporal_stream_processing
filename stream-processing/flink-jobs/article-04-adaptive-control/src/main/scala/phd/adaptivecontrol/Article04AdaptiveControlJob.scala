package phd.adaptivecontrol

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.pipeline.{AdaptivePipeline, WatermarkManager}


object Article04AdaptiveControlJob {

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
    // 2. Experiment Configuration
    // ============================================================
    val bootstrap =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")

    val topic =
      sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")

    val windowSizeMs =
      sys.env.getOrElse("FIXED_WINDOW_MS", "5000").toLong

    val watermarkDelayMs =
      sys.env.getOrElse("FIXED_WATERMARK_MS", "3000").toLong

    // Strategies (optional env overrides)
    val windowStrategyMode =
      sys.env.getOrElse("WINDOW_STRATEGY", "fixed")

    val watermarkStrategyMode =
      sys.env.getOrElse("WATERMARK_STRATEGY", "fixed")

    val mlInferenceFlag =
      sys.env.getOrElse("ML_INFERENCE", "false").toBoolean

    val adaptiveConfig =
      AdaptiveConfig(
        windowSizeMs = windowSizeMs,
        watermarkDelayMs = watermarkDelayMs,
        adaptiveWindowSizeMs = windowSizeMs,
        adaptiveWatermarkDelayMs = watermarkDelayMs,
        windowStrategy = windowStrategyMode,
        watermarkStrategy = watermarkStrategyMode,
        mlInference = mlInferenceFlag
      )

    println(
      s"[MAIN] action=config " +
      s"bootstrap=$bootstrap " +
      s"topic=$topic " +
      s"windowSizeMs=${adaptiveConfig.windowSizeMs} " +
      s"watermarkDelayMs=${adaptiveConfig.watermarkDelayMs} " +
      s"windowStrategy=${adaptiveConfig.windowStrategy} " +
      s"watermarkStrategy=${adaptiveConfig.watermarkStrategy} " +
      s"mlInference=${adaptiveConfig.mlInference}"
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
    // 4. Watermark Strategy (adaptive-capable)
    // ============================================================
    val wmStrategy =
      WatermarkManager.build(adaptiveConfig)

    val timedGeoEventStream =
      geoEventStream.assignTimestampsAndWatermarks(wmStrategy)

    println("[MAIN] action=watermarkStrategy status=initialized")

    // ============================================================
    // 5. Adaptive Pipeline
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
