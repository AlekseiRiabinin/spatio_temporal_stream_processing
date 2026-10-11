package cityrover.metrics

import org.apache.flink.api.common.functions.{OpenContext, RichMapFunction}
import org.apache.flink.metrics.Counter
import org.slf4j.LoggerFactory

import cityrover.util.MetricsConfig


/**
 * Combined source-boundary instrumentation.
 *
 * Sits immediately after the KafkaSource. Captures `t_src_ms` and attaches
 * it to the record as a `StampedRecord`. Also records `kafka_to_flink_ms`
 * IF the Kafka broker timestamp (`t1_ms`) is available on the payload via
 * `t1Extractor` — otherwise the downstream ProcessFunction does it.
 *
 * The LatencyMetrics registry is built inside `open()`, so each subtask
 * gets its own Flink histograms. That's the correct Flink idiom.
 */
final class LatencyInstrumentedMap[T](
  metricsCfg: MetricsConfig,
  subgroup: String,
  t1Extractor: T => Long = (_: T) => -1L
) extends RichMapFunction[T, StampedRecord[T]]:

  @transient private val log = LoggerFactory.getLogger(getClass)

  @transient private var lat: LatencyMetrics  = _
  @transient private var taggedCounter: Counter = _
  @transient private var recordedHere: Counter  = _

  override def open(openContext: OpenContext): Unit =
    super.open(openContext)

    if metricsCfg.prometheusEnabled then
      val groupName: String = metricsCfg.metricGroup

      val group = getRuntimeContext.getMetricGroup
        .addGroup(groupName)
        .addGroup(subgroup)

      taggedCounter = group.counter("tagged_records_total")
      recordedHere  = group.counter("kafka_to_flink_recorded_here_total")

      log.info(
        s"LatencyInstrumentedMap opened: metricGroup=$groupName, " +
        s"subgroup=$subgroup, metricsEnabled=${metricsCfg.prometheusEnabled}"
      )
    else
      log.info("LatencyInstrumentedMap opened with metrics disabled")

  override def map(value: T): StampedRecord[T] =
    val tSrcMs = System.currentTimeMillis().toDouble
    val t1Ms   = t1Extractor(value)

    if taggedCounter != null then taggedCounter.inc()

    if t1Ms >= 0L then
      lat.recordKafkaToFlink(t1Ms.toDouble, tSrcMs)
      if recordedHere != null then recordedHere.inc()

    StampedRecord(
      payload = value,
      t1Ms    = t1Ms,
      tSrcMs  = tSrcMs
    )


object LatencyInstrumentedMap:

  def apply[T](
    metricsCfg: MetricsConfig,
    subgroup: String
  ): LatencyInstrumentedMap[T] =
    new LatencyInstrumentedMap[T](metricsCfg, subgroup)

  def withEmbeddedT1[T](
    metricsCfg: MetricsConfig,
    subgroup: String,
    extractT1: T => Long
  ): LatencyInstrumentedMap[T] =
    new LatencyInstrumentedMap[T](metricsCfg, subgroup, extractT1)
