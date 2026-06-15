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

import phd.adaptivecontrol.pipeline.{
  AdaptivePipeline,
  WatermarkManager
}

import phd.adaptivecontrol.adaptive.{
  ONNXInference,
  StreamProfiler,
  AdaptiveRuntimeState
}


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
    val bootstrap =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")

    val topic =
      sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")

    val windowSizeMs =
      sys.env.getOrElse("FIXED_WINDOW_MS", "5000").toLong

    val watermarkDelayMs =
      sys.env.getOrElse("FIXED_WATERMARK_MS", "3000").toLong

    val mlInferenceFlag =
      sys.env.getOrElse("ML_INFERENCE", "false").toBoolean

    val mode =
      sys.env.getOrElse("ADAPTIVE_MODE", "fixed") // fixed | adaptive

    // ============================================================
    // 3. Model paths
    // ============================================================
    val windowModelPath =
      sys.env.getOrElse("WINDOW_MODEL_PATH", "/opt/models/model_a_window.onnx")

    val watermarkModelPath =
      sys.env.getOrElse("WATERMARK_MODEL_PATH", "/opt/models/model_b_watermark.onnx")

    val scalerParamsPath =
      sys.env.getOrElse("SCALER_PARAMS_PATH", "/opt/models/scaler_params.json")

    println(
      "[MAIN] action=models " +
      s"window=$windowModelPath " +
      s"watermark=$watermarkModelPath " +
      s"scaler=$scalerParamsPath"
    )

    // ============================================================
    // 4. Adaptive Config (NO STRINGS FOR MODE)
    // ============================================================
    val adaptiveConfig =
      AdaptiveConfig(
        windowSizeMs = windowSizeMs,
        watermarkDelayMs = watermarkDelayMs,
        mlInference = mlInferenceFlag,
        windowModelPath = windowModelPath,
        watermarkModelPath = watermarkModelPath,
        scalerParamsPath = scalerParamsPath
      )

    println(
      "[MAIN] action=config " +
      s"mlInference=$mlInferenceFlag"
    )

    // ============================================================
    // 5. Runtime mode (SOURCE OF TRUTH NOW)
    // ============================================================
    if (mode == "adaptive") {
      AdaptiveRuntimeState.setMode(AdaptiveRuntimeState.Adaptive)
      AdaptiveRuntimeState.initialize(windowSizeMs, watermarkDelayMs)
    } else {
      AdaptiveRuntimeState.setMode(AdaptiveRuntimeState.Fixed)
    }

    println(
      "[MAIN] action=runtime_state " +
      s"windowMs=${AdaptiveRuntimeState.windowSizeMs} " +
      s"watermarkMs=${AdaptiveRuntimeState.watermarkDelayMs}"
    )

    // ============================================================
    // 6. ML runtime
    // ============================================================
    if (mlInferenceFlag && AdaptiveRuntimeState.isAdaptive) {

      println("[MAIN] action=onnx init status=starting")

      ONNXInference.initialize(adaptiveConfig)

      println(
        "[MAIN] action=onnx init status=ready " +
        s"mode=${ONNXInference.status}"
      )

    } else {

      println("[MAIN] action=onnx init status=disabled")
    }

    // ============================================================
    // 7. Kafka Source
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
    // 8. Watermarks
    // ============================================================
    val wmStrategy =
      WatermarkManager.build(adaptiveConfig)

    val timedGeoEventStream =
      geoEventStream.assignTimestampsAndWatermarks(wmStrategy)

    // ============================================================
    // 9. Pipeline
    // ============================================================
    val processedStream =
      AdaptivePipeline.build(env, timedGeoEventStream, adaptiveConfig)

    // ============================================================
    // 10. Output
    // ============================================================
    processedStream
      .map(result => mapper.writeValueAsString(result))
      .print()

    // ============================================================
    // 11. Execute
    // ============================================================
    env.execute("Article 04: Adaptive Window and Watermark Control")
  }
}
