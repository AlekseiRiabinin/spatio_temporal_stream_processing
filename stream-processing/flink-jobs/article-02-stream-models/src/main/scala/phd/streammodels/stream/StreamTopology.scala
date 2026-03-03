package phd.streammodels.stream

import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.algorithms.StreamModel
import phd.streammodels.windows.WindowStrategy
import phd.streammodels.model.TypeInfos._


object StreamTopology {

  private val LOG = LoggerFactory.getLogger("TOPOLOGY")

  def build[K](
    env: StreamExecutionEnvironment,
    model: StreamModel[K],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    LOG.info(
      s"""
        |Building pipeline →
        |  model  = ${model.getClass.getSimpleName}
        |  window = ${windowStrategy.getClass.getSimpleName}
        |""".stripMargin
    )

    val source: DataStream[Event] =
      KafkaSourceFactory.createEventSource(env)

    LOG.info("Source initialized")

    val pipeline = model.buildPipeline(source, windowStrategy)

    LOG.info(
      s"Pipeline created → type=${pipeline.getClass.getSimpleName}"
    )

    pipeline
  }

  def sinkResults[K: TypeInformation](
    results: DataStream[WindowResult[K]]
  ): DataStream[WindowResult[K]] = {

    LOG.info("Sink attached")

    val debugged = results.map { r: WindowResult[K] =>

      LOG.info(
        s"""
           |Window Result:
           |  key         = ${r.partition}
           |  windowStart = ${r.windowStart}
           |  windowEnd   = ${r.windowEnd}
           |  value       = ${r.value}
           |  timestamp   = ${System.currentTimeMillis()}
           |""".stripMargin
      )

      r
    }.name("TopologyDebugSink")

    debugged.print().name("PrintSink")

    debugged
  }
}
