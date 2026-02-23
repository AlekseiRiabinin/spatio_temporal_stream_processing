package phd.streammodels.operators

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.metrics.{Gauge, Histogram, HistogramStatistics}
import scala.collection.mutable
import phd.streammodels.model.{Event, WindowResult}


/**
 * Generic window aggregation function for Article 2.
 * Counts events per key (K), where K may be String, Int, SpatialPartition, etc.
 */
final class CountEventsWindowFunction[K]
  extends ProcessWindowFunction[Event, WindowResult[K], K, TimeWindow] {

  @transient private var windowLatency: Histogram = _
  @transient private var lastProcessingLatency: Long = 0L

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    windowLatency = group.histogram("windowLatency", new CustomLatencyHistogram)

    group.gauge[Long, Gauge[Long]](
      "processingLatency",
      new Gauge[Long] {
        override def getValue: Long = lastProcessingLatency
      }
    )
  }

  override def process(
    key: K,
    context: Context,
    elements: Iterable[Event],
    out: Collector[WindowResult[K]]
  ): Unit = {

    println(
      s"🔥 WINDOW FIRING: key=$key, elements=${elements.size}, " +
      s"windowStart=${context.window.getStart}, windowEnd=${context.window.getEnd}, " +
      s"currentWatermark=${context.currentWatermark}"
    )

    val count = elements.size.toLong
    val start = context.window.getStart
    val end = context.window.getEnd
    val now = context.currentProcessingTime

    val latencySeconds = (now - end) / 1000.0
    windowLatency.update(latencySeconds.toLong)

    lastProcessingLatency = now - end

    out.collect(
      WindowResult[K](
        partition = key,
        windowStart = start,
        windowEnd = end,
        value = count,
        processingTime = Some(now)
      )
    )
  }
}

/**
 * Simple in-memory histogram for Flink metrics.
 */
private class CustomLatencyHistogram extends Histogram with Serializable {

  @transient private var values: mutable.ArrayBuffer[Long] = _

  private def ensureInit(): Unit = {
    if (values == null) {
      values = mutable.ArrayBuffer.empty[Long]
    }
  }

  override def update(value: Long): Unit = {
    ensureInit()
    values += value
  }

  override def getCount: Long =
    if (values == null) 0L else values.size.toLong

  override def getStatistics: HistogramStatistics = {
    ensureInit()
    new CustomHistogramStatistics(values.toArray)
  }
}

private class CustomHistogramStatistics(data: Array[Long]) extends HistogramStatistics {

  private val sorted: Array[Long] = data.sorted

  override def getMin: Long = if (sorted.isEmpty) 0L else sorted.head
  override def getMax: Long = if (sorted.isEmpty) 0L else sorted.last

  override def getMean: Double =
    if (sorted.isEmpty) 0.0 else sorted.map(_.toDouble).sum / sorted.length.toDouble

  override def getStdDev: Double = {
    if (sorted.length <= 1) 0.0
    else {
      val mean = getMean
      val variance =
        sorted.map(v => math.pow(v.toDouble - mean, 2)).sum / (sorted.length.toDouble - 1.0)
      math.sqrt(variance)
    }
  }

  override def getQuantile(quantile: Double): Double = {
    if (sorted.isEmpty) 0.0
    else {
      val q = math.min(1.0, math.max(0.0, quantile))
      val idx = math.round(q * (sorted.length - 1)).toInt
      sorted(idx).toDouble
    }
  }

  override def getValues: Array[Long] = data
  override def size: Int = data.length
}

object WindowAggregation {
  def countEvents[K]: CountEventsWindowFunction[K] =
    new CountEventsWindowFunction[K]
}
