package cityrover.windows

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.ArrayList

import cityrover.model.{TelemetryEvent, EnrichedEvent}
import cityrover.metrics.{LatencyMetrics, StampedRecord}
import cityrover.util.MetricsConfig
import cityrover.util.GeoUtils.{haversine, computeGridCell, computeRegion}


/**
 * 30-second window feature computation.
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
class Window30sFunction(metricsCfg: MetricsConfig)
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
    lat = LatencyMetrics.register(getRuntimeContext, metricsCfg, "window_30s")
    log.info(s"Window30sFunction opened (metricsEnabled=${metricsCfg.prometheusEnabled})")

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

    // --- speedAvg ---
    var speedSum = 0.0
    var i = 0
    while i < size do
      speedSum += list.get(i).speed
      i += 1
    val speedAvg = speedSum / size

    // --- speedStd ---
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

    // --- stopsCount ---
    var stopsCount = 0
    i = 0
    while i < size do
      if list.get(i).speed < 0.5 then stopsCount += 1
      i += 1

    // --- distanceTraveled ---
    var distanceTraveled = 0.0
    i = 0
    while i < size - 1 do
      val a = list.get(i)
      val b = list.get(i + 1)
      distanceTraveled += haversine(a.lat, a.lon, b.lat, b.lon)
      i += 1

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

    out.collect(
      EnrichedEvent(
        roverId = last.roverId,
        edgeId = last.edgeId,
        routeId = last.routeId,
        lat = last.lat,
        lon = last.lon,
        ts = last.ts,
        speed = last.speed,
        heading = last.heading,

        // 5s window placeholders
        speedAvg5s = 0.0,
        speedMax5s = 0.0,
        speedMin5s = 0.0,
        accelerationAvg5s = 0.0,
        headingChange5s = 0.0,
        distanceTraveled5s = 0.0,

        // 30s window features
        speedAvg30s = speedAvg,
        speedStd30s = speedStd,
        accelerationAvg30s = accelerationAvg,
        jerkAvg30s = jerkAvg,
        turnRateAvg30s = turnRateAvg,
        stopsCount30s = stopsCount,
        distanceTraveled30s = distanceTraveled,

        // 1m placeholders
        speedAvg1m = 0.0,
        speedStd1m = 0.0,
        accelerationAvg1m = 0.0,
        jerkAvg1m = 0.0,
        turnRateAvg1m = 0.0,
        idleRatio1m = 0.0,
        distanceTraveled1m = 0.0,
        congestionLevel1m = 0.0,

        // 5m placeholders
        speedAvg5m = 0.0,
        speedStd5m = 0.0,
        accelerationAvg5m = 0.0,
        jerkAvg5m = 0.0,
        turnRateAvg5m = 0.0,
        idleRatio5m = 0.0,
        distanceTraveled5m = 0.0,
        congestionLevel5m = 0.0,

        // geospatial
        gridCellId = computeGridCell(last.lat, last.lon),
        regionId = computeRegion(last.lat, last.lon),
        snappedEdgeId = last.edgeId,
        snappedLat = last.lat,
        snappedLon = last.lon,

        // route
        routeProgress = 0.0,
        routeDeviation = 0.0,
        expectedSpeed = 0.0,
        speedRatio = 0.0,

        // behavioral
        drivingStyleScore = 0.0,
        anomalyScore = 0.0,

        // metadata
        featureVersion = "v1",
        featureTimestamp = System.currentTimeMillis(),
        featureLatencyMs = System.currentTimeMillis() - last.ts,

        // quality
        isOutlier = false,
        isGpsJump = false,
        isSpeedAnomaly = false,
        isHeadingAnomaly = false,

        // debug
        rawEventHash = last.hashCode().toHexString,
        processingNode = java.net.InetAddress.getLocalHost.getHostName,
        processingTimeMs = System.currentTimeMillis()
      )
    )
