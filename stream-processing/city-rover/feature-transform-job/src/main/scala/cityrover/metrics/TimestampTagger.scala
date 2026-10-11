package cityrover.metrics

import org.apache.flink.api.common.functions.{OpenContext, RichMapFunction}
import org.apache.flink.metrics.Counter
import org.slf4j.LoggerFactory

import cityrover.util.MetricsConfig


/**
 * Captures t_src_ms and the Kafka broker timestamp, and attaches both to
 * the record so downstream operators can compute `kafka_to_flink_ms`.
 *
 * Placement: immediately after the KafkaSource, before any transformation.
 *
 *   source → [TimestampTagger] → transform → sink
 *
 * Note on t1:
 *   Flink's DataStream API does not expose the Kafka record timestamp via
 *   a plain map function. The idiomatic way to obtain it is from a
 *   ProcessFunction, where `ctx.timestamp()` carries the broker timestamp.
 *
 *   This tagger therefore does NOT know t1 at map time. It only captures
 *   t_src_ms. The downstream transformation operator reads t1 from
 *   `ctx.timestamp()` and computes kafka_to_flink_ms using the t_src_ms
 *   attached here.
 *
 * Relationship to LatencyInstrumentedMap:
 *   TimestampTagger is a minimal operator: it only stamps the record.
 *   LatencyInstrumentedMap is a superset: it also records the
 *   `kafka_to_flink_ms` histogram when t1 is available.
 *
 *   In the current pipeline, we use LatencyInstrumentedMap. This class is
 *   retained as a lighter alternative for cases where the histogram
 *   recording is handled elsewhere (e.g. a downstream ProcessFunction).
 *
 * @param metricsCfg   parsed metrics configuration
 * @param subgroup     per-operator subgroup (e.g. "transform", "writer")
 * @param t1Extractor  function to extract t1 from the deserialized payload
 *                     (defaults to returning -1 → downstream fills it in)
 */
final class TimestampTagger[T](
  metricsCfg: MetricsConfig,
  subgroup: String,
  t1Extractor: T => Long = (_: T) => -1L
) extends RichMapFunction[T, StampedRecord[T]]:

  @transient private val log = LoggerFactory.getLogger(getClass)

  @transient private var taggedCounter: Counter = _

  override def open(openContext: OpenContext): Unit =
    super.open(openContext)

    if metricsCfg.prometheusEnabled then
      val groupName: String = metricsCfg.metricGroup

      val group = getRuntimeContext.getMetricGroup
        .addGroup(groupName)
        .addGroup(subgroup)

      taggedCounter = group.counter("tagged_records_total")

      log.info(
        s"TimestampTagger opened: metricGroup=$groupName, " +
        s"subgroup=$subgroup, metricsEnabled=${metricsCfg.prometheusEnabled}"
      )
    else
      log.info("TimestampTagger opened with metrics disabled")

  override def map(value: T): StampedRecord[T] =
    // Capture t_src_ms as the very first thing — earliest wall-clock
    // we can obtain inside the operator chain.
    val tSrcMs = System.currentTimeMillis().toDouble

    // If the deserializer preserved the Kafka broker timestamp, extract it.
    // Otherwise the downstream ProcessFunction reads it from ctx.timestamp().
    val t1Ms = t1Extractor(value)

    if taggedCounter != null then taggedCounter.inc()

    StampedRecord(
      payload = value,
      t1Ms    = t1Ms,
      tSrcMs  = tSrcMs
    )


object TimestampTagger:

  def apply[T](
    metricsCfg: MetricsConfig,
    subgroup: String
  ): TimestampTagger[T] =
    new TimestampTagger[T](metricsCfg, subgroup)

  def withEmbeddedT1[T](
    metricsCfg: MetricsConfig,
    subgroup: String,
    extractT1: T => Long
  ): TimestampTagger[T] =
    new TimestampTagger[T](metricsCfg, subgroup, extractT1)
