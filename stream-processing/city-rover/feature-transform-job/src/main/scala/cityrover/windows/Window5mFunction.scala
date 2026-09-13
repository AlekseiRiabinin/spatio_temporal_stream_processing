package cityrover.windows

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.jdk.CollectionConverters.*

import cityrover.model.{TelemetryEvent, EnrichedEvent}
import cityrover.util.GeoUtils.{haversine, computeGridCell, computeRegion}


class Window5mFunction
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

    val idleRatio =
      speeds.count(_ < 0.5).toDouble / speeds.size.toDouble

    val distanceTraveled =
      list.sliding(2).map {
        case Seq(a, b) => haversine(a.lat, a.lon, b.lat, b.lon)
        case _         => 0.0
      }.sum

    val congestionLevel =
      if speedAvg < 5.0 then 1.0 else 0.0

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

        // 5s placeholders
        speedAvg5s = 0.0,
        speedMax5s = 0.0,
        speedMin5s = 0.0,
        accelerationAvg5s = 0.0,
        headingChange5s = 0.0,
        distanceTraveled5s = 0.0,

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

        // 5m window features
        speedAvg5m = speedAvg,
        speedStd5m = speedStd,
        accelerationAvg5m = accelerationAvg,
        jerkAvg5m = jerkAvg,
        turnRateAvg5m = turnRateAvg,
        idleRatio5m = idleRatio,
        distanceTraveled5m = distanceTraveled,
        congestionLevel5m = congestionLevel,

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
