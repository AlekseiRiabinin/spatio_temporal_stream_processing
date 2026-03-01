package phd.streammodels.stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.algorithms.StreamModel
import phd.streammodels.windows.WindowStrategy
import phd.streammodels.model.TypeInfos._


object StreamTopology {

  /**
    * Build the full pipeline:
    *   1. Read events from Kafka
    *   2. Apply the selected stream model (Actor, Log, Dataflow, MicroBatch)
    *   3. Apply the selected window strategy (Session, Dynamic, Adaptive, MultiTrigger)
    */
  def build[K](
    env: StreamExecutionEnvironment,
    model: StreamModel[K],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    val source: DataStream[Event] =
      KafkaSourceFactory.createEventSource(env)

    println(
      s"🔵 [TOPOLOGY] Source initialized, " +
      s"model=${model.getClass.getSimpleName}, " +
      s"strategy=${windowStrategy.getClass.getSimpleName}"
    )

    val pipeline = model.buildPipeline(source, windowStrategy)

    // Log pipeline creation
    println(
      s"🔵 [TOPOLOGY] Pipeline built: " +
      s"model=${model.getClass.getSimpleName}, " +
      s"strategy=${windowStrategy.getClass.getSimpleName}, " +
      s"pipelineType=${pipeline.getClass.getSimpleName}"
    )

    pipeline
  }

  /**
    * Simple sink for debugging Article 2 experiments.
    */
  def sinkResults[K: TypeInformation](
    results: DataStream[WindowResult[K]]
  ): DataStream[WindowResult[K]] = {

    val debugged = results.map { r: WindowResult[K] =>
      // per-window logging, not per record
      println(
        s"""
          |🔵 [TOPOLOGY] Window Result →
          |  key           = ${r.partition}
          |  windowStart   = ${r.windowStart}
          |  windowEnd     = ${r.windowEnd}
          |  value         = ${r.value}
          |  processingTime= ${r.processingTime.getOrElse(-1L)}
          |  timestamp     = ${System.currentTimeMillis()}
          |""".stripMargin
      )
      r
    }.name("DebugSink")

    debugged.print().name("PrintSink") // still useful for terminal output

    debugged
  }
}
