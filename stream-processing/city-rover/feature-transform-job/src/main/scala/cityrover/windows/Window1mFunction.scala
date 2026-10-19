package cityrover.windows

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.ArrayList

import cityrover.model.{TelemetryEvent, EnrichedEvent, EnrichedEvent1m, Window1mPayload}
import cityrover.metrics.{LatencyMetrics, StampedRecord}
import cityrover.windows.FeatureEnvelopeBuilder
import cityrover.util.MetricsConfig
import cityrover.util.GeoUtils.haversine


/**
 * 1-minute window feature computation.
 *
 * Input type is `StampedRecord[TelemetryEvent]` — the wrapper that carries
 * `t1Ms` (Kafka broker timestamp) and `tSrcMs` (wall clock at the source
 * boundary). The window function unwraps it for the actual computation and
 * uses the stamps to record latency metrics.
 *
 * Recorded metrics (mirroring the PyFlink probes):
 *   transform_ms      — per window (start → done of the computation)
 *   kafka_to_flink_ms — per element (only if t1 is available)
 *   queue_ms          — per element (t_src → transform start)
 *   latency_ms        — per element (only if t1 is available)
 */
class Window1mFunction(metricsCfg: MetricsConfig)
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
    lat = LatencyMetrics.register(getRuntimeContext, metricsCfg, "window_1m")
    log.info(s"Window1mFunction opened (metricsEnabled=${metricsCfg.prometheusEnabled})")

  override def process(
    key: String,
    ctx: ProcessWindowFunction[
      StampedRecord[TelemetryEvent],
      EnrichedEvent,
      String,
      TimeWindow
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

    val iter = stampedEvents.iterator()
    while iter.hasNext do
      val stamped = iter.next()
      list.add(stamped.payload)
      stamps.add(stamped)

    if list.isEmpty then return

    val size = list.size()
    val last = list.get(size - 1)

    // --- speedAvg, speedStd ---
    var speedSum = 0.0
    var i = 0
    while i < size do
      speedSum += list.get(i).speed
      i += 1
    val speedAvg = speedSum / size

    var sqDiffSum = 0.0
    i = 0
    while i < size do
      val d = list.get(i).speed - speedAvg
      sqDiffSum += d * d
      i += 1
    val speedStd = math.sqrt(sqDiffSum / size)

    // --- accelerationAvg ---
    var accelerationAvg = 0.0
    if size >= 2 then
      var diffSum = 0.0
      i = 0
      while i < size - 1 do
        diffSum += list.get(i + 1).speed - list.get(i).speed
        i += 1
      accelerationAvg = diffSum / (size - 1)

    // --- jerkAvg ---
    var jerkAvg = 0.0
    if size >= 3 then
      var jerkSum = 0.0
      i = 0
      while i < size - 2 do
        val a = list.get(i).speed
        val b = list.get(i + 1).speed
        val c = list.get(i + 2).speed
        jerkSum += (c - b) - (b - a)
        i += 1
      jerkAvg = jerkSum / (size - 2)

    // --- turnRateAvg ---
    var turnRateAvg = 0.0
    if size >= 2 then
      var turnSum = 0.0
      i = 0
      while i < size - 1 do
        turnSum += math.abs(list.get(i + 1).heading - list.get(i).heading)
        i += 1
      turnRateAvg = turnSum / (size - 1)

    // --- idleRatio ---
    var idleCount = 0
    i = 0
    while i < size do
      if list.get(i).speed < 0.5 then idleCount += 1
      i += 1
    val idleRatio = idleCount.toDouble / size.toDouble

    // --- distanceTraveled ---
    var distanceTraveled = 0.0
    i = 0
    while i < size - 1 do
      val a = list.get(i)
      val b = list.get(i + 1)
      distanceTraveled += haversine(a.lat, a.lon, b.lat, b.lon)
      i += 1

    // --- congestionLevel ---
    val congestionLevel = if speedAvg < 5.0 then 1.0 else 0.0

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
      EnrichedEvent1m(
        envelope = envelope,
        features = Window1mPayload(
          speedAvg1m         = speedAvg,
          speedStd1m         = speedStd,
          accelerationAvg1m  = accelerationAvg,
          jerkAvg1m          = jerkAvg,
          turnRateAvg1m      = turnRateAvg,
          idleRatio1m        = idleRatio,
          distanceTraveled1m = distanceTraveled,
          congestionLevel1m  = congestionLevel
        )
      )
    )
