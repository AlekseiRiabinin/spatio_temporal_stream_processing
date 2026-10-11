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

    val last = list.get(list.size() - 1)

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

        // 5s window features
        speedAvg5s = speedAvg,
        speedMax5s = speedMax,
        speedMin5s = speedMin,
        accelerationAvg5s = 0.0,
        headingChange5s = headingChange,
        distanceTraveled5s = distanceTraveled,

        // 30s placeholders
        speedAvg30s = 0.0,
        speedStd30s = 0.0,
        accelerationAvg30s = 0.0,
        jerkAvg30s = 0.0,
        turnRateAvg30s = 0.0,
        stopsCount30s = 0,
        distanceTraveled30s = 0.0,

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
