package phd.architecture.operators

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import phd.architecture.model.WindowResult
import phd.architecture.model.TypeInfos._


final class AttachProcessingLatencyFunction
  extends ProcessFunction[WindowResult, WindowResult] {

  override def processElement(
    value: WindowResult,
    ctx: ProcessFunction[WindowResult, WindowResult]#Context,
    out: Collector[WindowResult]
  ): Unit = {

    val now = ctx.timerService().currentProcessingTime()
    val updated = value.copy(processingTime = Some(now))
    out.collect(updated)
  }
}

object LatencyMetrics {

    /**
   * Attaches processing-time to each WindowResult
   * measuredResults = L(measuredResults)
   */
  def attachProcessingLatency(
    stream: DataStream[WindowResult]
  ): DataStream[WindowResult] = {

    val fn = new AttachProcessingLatencyFunction
    stream.process(fn)
  }
}
