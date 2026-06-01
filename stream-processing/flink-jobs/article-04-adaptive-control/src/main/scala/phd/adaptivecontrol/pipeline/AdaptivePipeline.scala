package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.interaction.InteractionEngine
import phd.adaptivecontrol.adaptive.StreamProfiler


object AdaptivePipeline {

  // ============================================================
  // Interaction processing + profiling (Flink-safe)
  // ============================================================
  class InteractionFlatMap
    extends FlatMapFunction[List[GeoEvent], Interaction]
    with Serializable {

    @transient private var engine: InteractionEngine = _

    private def getEngine: InteractionEngine = {
      if (engine == null) {
        engine = new InteractionEngine()
      }
      engine
    }

    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      StreamProfiler.updateEvents(batch)
      StreamProfiler.updateWindow(batch.size)

      val results =
        getEngine.process(batch)

      StreamProfiler.updateInteractions(results)
      StreamProfiler.logSnapshot()

      results.foreach(out.collect)
    }
  }

  // ============================================================
  // Pipeline
  // ============================================================
  def build(
    env: StreamExecutionEnvironment,
    inputStream: DataStream[GeoEvent],
    config: AdaptiveConfig
  ): DataStream[Interaction] = {

    println(
      "[ADAPTIVE PIPELINE] action=start " +
      s"windowSizeMs=${config.windowSizeMs} " +
      s"watermarkDelayMs=${config.watermarkDelayMs}"
    )

    // ------------------------------------------------------------
    // 1. Window processing
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
    // 2. Interaction analysis + profiling
    // ------------------------------------------------------------
    implicit val interactionTypeInfo: TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    val interactions: DataStream[Interaction] =
      windowedStream.flatMap(new InteractionFlatMap)

    println(
      "[ADAPTIVE PIPELINE] action=interactionAnalysis status=ready"
    )

    // ------------------------------------------------------------
    // 3. Future adaptive-control hooks
    // ------------------------------------------------------------
    println(
      "[ADAPTIVE PIPELINE] action=adaptiveControl status=disabled"
    )

    interactions
  }
}
