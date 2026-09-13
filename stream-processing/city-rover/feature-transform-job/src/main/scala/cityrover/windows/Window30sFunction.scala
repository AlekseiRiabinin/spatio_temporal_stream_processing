package cityrover.windows

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.jdk.CollectionConverters.*

import cityrover.model.{TelemetryEvent, EnrichedEvent}
import cityrover.util.GeoUtils.{haversine, computeGridCell, computeRegion}


class Window30sFunction
  extends ProcessWindowFunction[
    TelemetryEvent,
    EnrichedEvent,
    String,
    TimeWindow
  ]:

  override def process(
    key: String,
    ctx: ProcessWindowFunction[TelemetryEvent, EnrichedEvent, String, TimeWindow]#Context,
    events: java.lang.Iterable[TelemetryEvent],
    out: Collector[EnrichedEvent]
  ): Unit =

    val list = events.iterator().asScala.toList
    if list.isEmpty then return

    val last = list.last

    val speeds = list.map(_.speed)
    val speedAvg = speeds.sum / speeds.size
    val speedStd =
      math.sqrt(speeds.map(s => math.pow(s - speedAvg, 2)).sum / speeds.size)

    val accelerationAvg =
      if speeds.size >= 2 then
        val diffs = speeds.sliding(2).collect { case Seq(a, b) => b - a }.toList
        diffs.sum / diffs.size
      else 0.0

    val jerkAvg =
      if speeds.size >= 3 then
        val jerks = speeds.sliding(3).collect {
          case Seq(a, b, c) => (c - b) - (b - a)
        }.toList
        jerks.sum / jerks.size
      else 0.0

    val turnRateAvg =
      if list.size >= 2 then
        val turns = list.sliding(2).collect {
          case Seq(a, b) => math.abs(b.heading - a.heading)
        }.toList
        turns.sum / turns.size
      else 0.0

    val stopsCount = speeds.count(_ < 0.5)

    val distanceTraveled =
      list.sliding(2).map {
        case Seq(a, b) => haversine(a.lat, a.lon, b.lat, b.lon)
        case _         => 0.0
      }.sum

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
