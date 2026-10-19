package cityrover.windows

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.ArrayList

import cityrover.model.{TelemetryEvent, EnrichedEvent, EnrichedEvent5s, Window5sPayload}
import cityrover.metrics.{LatencyMetrics, StampedRecord}
import cityrover.windows.FeatureEnvelopeBuilder
import cityrover.util.MetricsConfig
import cityrover.util.GeoUtils.haversine


/**
 * 5-second window feature computation.
 *
 * Input type is `StampedRecord[TelemetryEvent]` — the wrapper carrying
 * `t1Ms` (Kafka broker timestamp) and `tSrcMs` (wall clock at the source
 * boundary). The window function unwraps it for the computation and uses
 * the stamps to record latency metrics.
 *
 * Recorded metrics (mirroring the PyFlink probes):
 *   transform_ms      — per window
 *   kafka_to_flink_ms — per element (only if t1 is available)
 *   queue_ms          — per element
 *   latency_ms        — per element (only if t1 is available)
 */
class Window5sFunction(metricsCfg: MetricsConfig)
  extends ProcessWindowFunction[
    StampedRecord[TelemetryEvent],
    EnrichedEvent,
    String,
    TimeWindow
  ]:

  @transient private val log = LoggerFactory.getLogger(getClass)
  @transient private var lat: LatencyMetrics = _

  override def open(openContext: OpenContext): Unit =
    super.open(openContext)
    lat = LatencyMetrics.register(getRuntimeContext, metricsCfg, "window_5s")
    log.info(s"Window5sFunction opened (metricsEnabled=${metricsCfg.prometheusEnabled})")

  override def process(
    key: String,
    ctx: ProcessWindowFunction[
      StampedRecord[TelemetryEvent], EnrichedEvent, String, TimeWindow
    ]#Context,
    stampedEvents: java.lang.Iterable[StampedRecord[TelemetryEvent]],
    out: Collector[EnrichedEvent]
  ): Unit =


    // ----------------------------------------------------------------
    // 0. Capture transform start time.
    // ----------------------------------------------------------------
    val tTransformStartMs = System.currentTimeMillis().toDouble

    // ----------------------------------------------------------------
    // 1. Materialize the input iterable and unwrap StampedRecord.
    //    Two parallel lists: domain events + their timing stamps.
    // ----------------------------------------------------------------
    val list   = new ArrayList[TelemetryEvent]()
    val stamps = new ArrayList[StampedRecord[TelemetryEvent]]()

    val iter   = stampedEvents.iterator()
    while iter.hasNext do
      val s = iter.next()
      list.add(s.payload)
      stamps.add(s)

    if list.isEmpty then return

    // --- speedAvg ---
    var speedSum = 0.0
    var speedMax = Double.MinValue
    var speedMin = Double.MaxValue
    var i = 0
    while i < list.size() do
      val s = list.get(i).speed
      speedSum += s
      if s > speedMax then speedMax = s
      if s < speedMin then speedMin = s
      i += 1
    val speedAvg = speedSum / list.size()

    // --- headingChange ---
    val headingChange =
      if list.size() >= 2 then
        math.abs(list.get(list.size() - 1).heading - list.get(0).heading)
      else 0.0

    // --- distanceTraveled ---
    var distanceTraveled = 0.0
    var j = 0
    while j < list.size() - 1 do
      val a = list.get(j)
      val b = list.get(j + 1)
      distanceTraveled += haversine(a.lat, a.lon, b.lat, b.lon)
      j += 1

    // ----------------------------------------------------------------
    // 2. Capture transform done time.
    // ----------------------------------------------------------------
    val tTransformDoneMs = System.currentTimeMillis().toDouble

    // ----------------------------------------------------------------
    // 3. Record latency metrics.
    // ----------------------------------------------------------------
    lat.recordTransform(tTransformStartMs, tTransformDoneMs)

    i = 0
    while i < stamps.size() do
      val s = stamps.get(i)
      if s.t1Ms >= 0L then
        lat.recordKafkaToFlink(s.t1Ms.toDouble, s.tSrcMs)
        lat.recordEndToEnd(s.t1Ms.toDouble, tTransformDoneMs)
      lat.recordQueue(s.tSrcMs, tTransformStartMs)
      i += 1

    // ----------------------------------------------------------------
    // 4. Emit partial record: envelope + 1m payload only.
    // ----------------------------------------------------------------
    val windowEndTs = ctx.window.getEnd   // event-time ms

    val envelope = FeatureEnvelopeBuilder.build(
      list, windowEndTs, tTransformStartMs.toLong
    )

    out.collect(
      EnrichedEvent5s(
        envelope = envelope,
        features = Window5sPayload(
          speedAvg5s         = speedAvg,
          speedMax5s         = speedMax,
          speedMin5s         = speedMin,
          accelerationAvg5s  = 0.0,
          headingChange5s    = headingChange,
          distanceTraveled5s = distanceTraveled
        )
      )
    )
