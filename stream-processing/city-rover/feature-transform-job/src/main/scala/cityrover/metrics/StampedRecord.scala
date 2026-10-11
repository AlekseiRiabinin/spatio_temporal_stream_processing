package cityrover.metrics


/**
 * Record wrapper carrying the timing stamps captured between the source
 * and the transformation operator.
 *
 * This mirrors the "inline map after the source" trick used in the PyFlink
 * probes (kafka_latency_probe.py, cassandra_latency_probe.py), where
 * `stamp_arrival` attaches `t_src_ms` to the payload before the process
 * function runs.
 *
 * Fields:
 *   payload — the actual domain object (Array[Byte], TelemetryEvent, …)
 *   t1Ms    — Kafka broker timestamp of the record, if available.
 *             Set to -1L when the broker timestamp is not available at
 *             map time (the common case with SimpleStringSchema-style
 *             deserializers). A downstream ProcessFunction can then read
 *             ctx.timestamp() to obtain the real value.
 *   tSrcMs  — wall clock (Unix-epoch ms, as a Double) at the moment the
 *             record left the source operator. This is the earliest
 *             timestamp we can obtain in the operator chain without a
 *             custom Java deserializer.
 *
 * The `Double` type for tSrcMs is deliberate: it preserves sub-millisecond
 * precision, matching the PyFlink probes' use of time.time() * 1000.0.
 */
final case class StampedRecord[T](
  payload: T,
  t1Ms: Long,
  tSrcMs: Double
)
