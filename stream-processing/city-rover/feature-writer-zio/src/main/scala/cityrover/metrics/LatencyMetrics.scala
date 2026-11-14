package cityrover.metrics

import zio._
import zio.metrics.{Metric, MetricKeyType, MetricLabel}


import cityrover.util.MetricsConfig

/** Latency histograms for feature-writer-zio.
  *
  * Mirrors the metric surface of the Flink writer job, with the
  * runtime-specific stage renamed:
  *   - kafka_to_flink_ms → kafka_to_zio_ms : t_src_ms - t1_ms   (broker → parse)
  *   - flink_ms          → zio_ms          : t_write_start - t_src (parse → sink start)
  *   - write_ms          : t2 - t_write_start   (Cassandra write duration)
  *   - latency_ms        : t2 - t1              (end-to-end)
  *
  * `kafka_to_zio_ms` replaces `kafka_to_flink_ms` because the writer
  * runs on ZIO, not Flink. The metric name is consistent with the
  * runtime: same formula, different suffix.
  *
  * ZIO metrics are global and name-keyed; registration happens implicitly
  * on first use. The Prometheus connector in `Main` reads them at scrape
  * time. There is no per-subtask registry like Flink's RuntimeContext.
  *
  * `namespace` and `subgroup` are attached as MetricLabels so Prometheus
  * can distinguish this job from the transform job and filter by stage.
  */
object LatencyMetrics:

  /** Build the histogram boundaries from the config.
    *
    * `MetricKeyType.Histogram.Boundaries` lives directly under
    * `zio.metrics`, not nested inside `Metric`.
    *
    * `fromChunk` appends `Double.MaxValue` automatically, so the
    * resulting boundaries always satisfy the invariant that the last
    * bucket captures everything.
    */
  def boundaries(config: MetricsConfig): MetricKeyType.Histogram.Boundaries =
    val buckets = config.buckets
    if buckets.isEmpty then
      MetricKeyType.Histogram.Boundaries.exponential(0.5, 2.0, 12)
    else
      MetricKeyType.Histogram.Boundaries.fromChunk(buckets)

  /** Attach namespace + subgroup labels to a histogram. */
  private def tagHistogram(
    m: Metric.Histogram[Double],
    config: MetricsConfig,
    subgroup: String
  ): Metric.Histogram[Double] =
    m.tagged(
      MetricLabel("namespace", config.namespace),
      MetricLabel("subgroup", subgroup)
    )

  // ---------------------------------------------------------------------------
  // Histograms — one per writer stage.
  //
  // `Metric.Histogram[Double]` is the type alias for
  // `Metric[MetricKeyType.Histogram, Double, MetricState.Histogram]`.
  // Defs, not vals: boundaries depend on config, so callers build them
  // once in Main and reuse the resulting values.
  // ---------------------------------------------------------------------------

  /** Broker timestamp → consumer parse. */
  def kafkaToZioMs(
    config: MetricsConfig,
    subgroup: String
  ): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("kafka_to_zio_ms", boundaries(config)),
      config, subgroup
    )

  /** Consumer parse → sink start (queue time). */
  def zioMs(
    config: MetricsConfig,
    subgroup: String
  ): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("zio_ms", boundaries(config)),
      config, subgroup
    )

  /** Sink start → Cassandra completion. */
  def writeMs(
    config: MetricsConfig,
    subgroup: String
  ): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("write_ms", boundaries(config)),
      config, subgroup
    )

  /** Broker timestamp → Cassandra completion (end-to-end). */
  def latencyMs(
    config: MetricsConfig,
    subgroup: String
  ): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("latency_ms", boundaries(config)),
      config, subgroup
    )

  // ---------------------------------------------------------------------------
  // Counters — observability of the recording paths themselves.
  // ---------------------------------------------------------------------------

  /** Records successfully parsed from the enriched topic. */
  def consumedRecords(config: MetricsConfig): Metric.Counter[Long] =
    Metric.counter("consumed_records_total")
      .tagged(MetricLabel("namespace", config.namespace))

  /** Records successfully written to Cassandra. */
  def writtenRecords(config: MetricsConfig): Metric.Counter[Long] =
    Metric.counter("written_records_total")
      .tagged(MetricLabel("namespace", config.namespace))

  /** Cassandra write failures. */
  def writeErrors(config: MetricsConfig): Metric.Counter[Long] =
    Metric.counter("write_errors_total")
      .tagged(MetricLabel("namespace", config.namespace))
