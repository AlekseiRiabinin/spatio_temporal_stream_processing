package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.interaction.InteractionEngine
import phd.adaptivecontrol.adaptive.{StreamProfiler, AdaptiveController}

object AdaptivePipeline {

  // ============================================================
  // Interaction processing + profiling + adaptive control
  // ============================================================
  class InteractionFlatMap(config: AdaptiveConfig)
      extends FlatMapFunction[List[GeoEvent], Interaction]
      with Serializable {

    @transient private var engine: InteractionEngine = _
    @transient private var initialized: Boolean = false

    @transient private var lastAdaptationTs: Long = 0L

    // ----------------------------------------------------------
    // Initialization
    // ----------------------------------------------------------
    private def initIfNeeded(): Unit = {
      if (!initialized) {
        StreamProfiler.setConfig(config)
        initialized = true
      }
    }

    private def getEngine: InteractionEngine = {
      if (engine == null)
        engine = new InteractionEngine(config)

      engine
    }

    private def shouldAdapt(): Boolean = {
      val now = System.currentTimeMillis()

      if (
        now - lastAdaptationTs >=
        config.adaptationIntervalMs
      ) {
        lastAdaptationTs = now
        true
      } else {
        false
      }
    }

    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      // ----------------------------------------------------------
      // 0. Initialization
      // ----------------------------------------------------------
      initIfNeeded()

      // ----------------------------------------------------------
      // 1. Event profiling
      // ----------------------------------------------------------
      StreamProfiler.updateEvents(batch)

      // ----------------------------------------------------------
      // 2. Interaction analysis
      // ----------------------------------------------------------
      val results =
        getEngine.process(batch)

      // ----------------------------------------------------------
      // 3. Interaction profiling
      // ----------------------------------------------------------
      StreamProfiler.updateInteractions(results)

      // ----------------------------------------------------------
      // 4. Window profiling
      // ----------------------------------------------------------
      StreamProfiler.updateWindow(batch.size)

      // ----------------------------------------------------------
      // 5. Build ML feature vector
      // ----------------------------------------------------------
      val features =
        StreamProfiler.snapshot()

      // ----------------------------------------------------------
      // 6. Adaptive control
      // ----------------------------------------------------------
      if (
        config.windowStrategy == "adaptive" ||
        config.watermarkStrategy == "adaptive" ||
        config.mlInference
      ) {

        if (shouldAdapt()) {

          val inferenceStart =
            System.nanoTime()

          val decision =
            AdaptiveController.decide(features)

          val inferenceLatencyMs =
            (System.nanoTime() - inferenceStart) /
              1000000.0

          // ----------------------------------------
          // Update runtime configuration
          // ----------------------------------------
          config.adaptiveWindowSizeMs =
            decision.windowSizeMs

          config.adaptiveWatermarkDelayMs =
            decision.watermarkDelayMs

          // ----------------------------------------
          // Store adaptive metrics
          // ----------------------------------------
          StreamProfiler.updateAdaptiveDecision(
            decision,
            inferenceLatencyMs
          )

          println(
            "[ADAPTIVE CONTROL] " +
              s"window=${decision.windowSizeMs} " +
              s"watermark=${decision.watermarkDelayMs} " +
              s"inference_ms=${inferenceLatencyMs.formatted("%.3f")} " +
              s"confidence=${decision.confidence} " +
              s"strategy=${decision.strategy}"
          )
        }
      }

      // ----------------------------------------------------------
      // 7. Log metrics
      // ----------------------------------------------------------
      StreamProfiler.logSnapshot()

      // ----------------------------------------------------------
      // 8. Emit interactions
      // ----------------------------------------------------------
      results.foreach(out.collect)
    }
  }

  // ============================================================
  // Pipeline builder
  // ============================================================
  def build(
      env: StreamExecutionEnvironment,
      inputStream: DataStream[GeoEvent],
      config: AdaptiveConfig
  ): DataStream[Interaction] = {

    println(
      "[ADAPTIVE PIPELINE] action=start " +
        s"windowSizeMs=${config.windowSizeMs} " +
        s"watermarkDelayMs=${config.watermarkDelayMs} " +
        s"windowStrategy=${config.windowStrategy} " +
        s"watermarkStrategy=${config.watermarkStrategy} " +
        s"mlInference=${config.mlInference} " +
        s"adaptationIntervalMs=${config.adaptationIntervalMs}"
    )

    // ------------------------------------------------------------
    // Window processing
    // ------------------------------------------------------------
    val windowedStream: DataStream[List[GeoEvent]] =
      WindowProcessor.applyWindow(
        inputStream,
        config
      )

    println(
      "[ADAPTIVE PIPELINE] action=windowing status=initialized"
    )

    // ------------------------------------------------------------
    // Interaction processing
    // ------------------------------------------------------------
    implicit val interactionTypeInfo
        : TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    val interactions: DataStream[Interaction] =
      windowedStream.flatMap(
        new InteractionFlatMap(config)
      )

    println(
      "[ADAPTIVE PIPELINE] action=interactionAnalysis status=ready"
    )

    println(
      "[ADAPTIVE PIPELINE] action=adaptiveControl status=enabled"
    )

    interactions
  }
}
