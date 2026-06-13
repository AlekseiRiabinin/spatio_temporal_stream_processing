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
import phd.adaptivecontrol.adaptive.{ONNXInference, StreamProfiler}


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

    // ============================================================
    // 2. Config (env-driven)
    // ============================================================
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")

    val windowSizeMs = sys.env.getOrElse("FIXED_WINDOW_MS", "5000").toLong
    val watermarkDelayMs = sys.env.getOrElse("FIXED_WATERMARK_MS", "3000").toLong

    val windowStrategyMode = sys.env.getOrElse("WINDOW_STRATEGY", "fixed")
    val watermarkStrategyMode = sys.env.getOrElse("WATERMARK_STRATEGY", "fixed")
    val mlInferenceFlag = sys.env.getOrElse("ML_INFERENCE", "false").toBoolean

    // ============================================================
    // 3. MODEL PATHS (mounted from Docker volumes)
    // ============================================================
    val windowModelPath =
      sys.env.getOrElse("WINDOW_MODEL_PATH", "/opt/models/model_a_window.onnx")

    val watermarkModelPath =
      sys.env.getOrElse("WATERMARK_MODEL_PATH", "/opt/models/model_b_watermark.onnx")

    println(s"[MAIN] action=models window=$windowModelPath watermark=$watermarkModelPath")

    // ============================================================
    // 4. Adaptive Config
    // ============================================================
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
        "[MAIN] action=config " +
          s"windowStrategy=${adaptiveConfig.windowStrategy} " +
          s"watermarkStrategy=${adaptiveConfig.watermarkStrategy} " +
          s"mlInference=${adaptiveConfig.mlInference}"
      )

    // ============================================================
    // 5. INITIALIZE ML RUNTIME
    // ============================================================
    if (mlInferenceFlag) {

      println("[MAIN] action=onnx init status=starting")

      ONNXInference.initialize(adaptiveConfig)

      println(
        s"[MAIN] action=onnx init status=ready mode=${ONNXInference.status}"
      )

    } else {

      println(
        "[MAIN] action=onnx init status=disabled"
      )
    }

    // ============================================================
    // 6. Kafka Source
    // ============================================================
    val kafkaSource =
      KafkaSource.builder[String]()
        .setBootstrapServers(bootstrap)
        .setTopics(topic)
        .setGroupId("article04-adaptive-control")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

    implicit val geoEventTypeInfo: TypeInformation[GeoEvent] =
      createTypeInformation[GeoEvent]

    val rawStream =
      env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "KafkaSource"
      )

    val geoEventStream =
      rawStream
        .map(json => mapper.readValue(json, classOf[GeoEvent]))
        .filter(_.isValid)

    // ============================================================
    // 7. Watermarks
    // ============================================================
    val wmStrategy = WatermarkManager.build(adaptiveConfig)

    val timedGeoEventStream =
      geoEventStream.assignTimestampsAndWatermarks(wmStrategy)

    // ============================================================
    // 8. PIPELINE
    // ============================================================
    val processedStream =
      AdaptivePipeline.build(env, timedGeoEventStream, adaptiveConfig)

    // ============================================================
    // 9. OUTPUT
    // ============================================================
    processedStream
      .map(result => mapper.writeValueAsString(result))
      .print()

    // ============================================================
    // 10. EXECUTE
    // ============================================================
    env.execute("Article 04: Adaptive Window and Watermark Control")
  }
}
