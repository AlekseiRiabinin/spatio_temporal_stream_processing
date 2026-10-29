package cityrover.window

import cityrover.model.{
  TelemetryEvent,
  Window5sPayload,
  Window30sPayload,
  Window1mPayload,
  Window5mPayload
}
import cityrover.util.GeoUtils.haversine


/** Pure feature math for the four tumbling event-time windows.
  *
  * Each `add` is a fold: `(acc, event) => acc'`. Each `result` converts
  * the final accumulator into the wire-facing payload.
  *
  * The formulas mirror the Flink `Window*Function` bodies field-for-field,
  * so a given set of events produces identical payloads in both jobs. When
  * the Flink job's formula changes, this file must change too.
  *
  * `add*` also records `lastEvent = Some(ev)` on every event, so
  * `EventTimeWindow.fireReadyWindows` can build a correct
  * `FeatureEnvelope` from the true "latest event in the window" —
  * not the triggering event.
  */
object WindowAggregator {

  // ============================================================
  // 5-second window
  // ============================================================

  def add5s(acc: Window5sAccumulator, ev: TelemetryEvent): Window5sAccumulator = {
    val newDistance =
      (acc.prevLat, acc.prevLon) match
        case (Some(plat), Some(plon)) => acc.distanceTraveled + haversine(plat, plon, ev.lat, ev.lon)
        case _                        => acc.distanceTraveled

    acc.copy(
      speedSum         = acc.speedSum + ev.speed,
      speedMax         = math.max(acc.speedMax, ev.speed),
      speedMin         = math.min(acc.speedMin, ev.speed),
      count            = acc.count + 1L,
      firstHeading     = acc.firstHeading.orElse(Some(ev.heading)),
      lastHeading      = Some(ev.heading),
      distanceTraveled = newDistance,
      prevLat          = Some(ev.lat),
      prevLon          = Some(ev.lon),
      lastSeenTs       = ev.ts,
      lastEvent        = Some(ev)   // ← new
    )
  }

  def result5s(acc: Window5sAccumulator, windowEndTs: Long): Window5sPayload = {
    val speedAvg = if acc.count > 0 then acc.speedSum / acc.count else 0.0
    val headingChange =
      (acc.firstHeading, acc.lastHeading) match
        case (Some(first), Some(last)) => math.abs(last - first)
        case _                          => 0.0

    Window5sPayload(
      speedAvg5s         = speedAvg,
      speedMax5s         = if acc.count > 0 then acc.speedMax else 0.0,
      speedMin5s         = if acc.count > 0 then acc.speedMin else 0.0,
      accelerationAvg5s  = 0.0,
      headingChange5s    = headingChange,
      distanceTraveled5s = acc.distanceTraveled
    )
  }

  // ============================================================
  // 30-second window
  // ============================================================

  def add30s(acc: Window30sAccumulator, ev: TelemetryEvent): Window30sAccumulator = {
    val newAccel: Option[Double] =
      acc.prevSpeed.map(prev => ev.speed - prev)

    val newAccelSum  = newAccel.fold(acc.accelSum)(a => acc.accelSum + a)
    val newJerkDelta =
      for
        curr <- newAccel
        prev <- acc.prevAccel
      yield curr - prev
    val newJerkSum   = newJerkDelta.fold(acc.jerkSum)(j => acc.jerkSum + j)
    val newJerkCount = newJerkDelta.fold(acc.jerkCount)(_ => acc.jerkCount + 1L)

    val newTurnDelta = acc.prevHeading.map(ph => math.abs(ev.heading - ph))
    val newTurnSum   = newTurnDelta.fold(acc.turnSum)(t => acc.turnSum + t)
    val newTurnCount = newTurnDelta.fold(acc.turnCount)(_ => acc.turnCount + 1L)

    val newDistance =
      (acc.prevLat, acc.prevLon) match
        case (Some(plat), Some(plon)) => acc.distanceTraveled + haversine(plat, plon, ev.lat, ev.lon)
        case _                        => acc.distanceTraveled

    acc.copy(
      speedSum         = acc.speedSum + ev.speed,
      speedSumSq       = acc.speedSumSq + ev.speed * ev.speed,
      count            = acc.count + 1L,
      prevSpeed        = Some(ev.speed),
      prevAccel        = newAccel.orElse(acc.prevAccel),
      accelSum         = newAccelSum,
      jerkSum          = newJerkSum,
      jerkCount        = newJerkCount,
      prevHeading      = Some(ev.heading),
      turnSum          = newTurnSum,
      turnCount        = newTurnCount,
      stopsCount       = if ev.speed < 0.5 then acc.stopsCount + 1L else acc.stopsCount,
      distanceTraveled = newDistance,
      prevLat          = Some(ev.lat),
      prevLon          = Some(ev.lon),
      lastSeenTs       = ev.ts,
      lastEvent        = Some(ev)   // ← new
    )
  }

  def result30s(acc: Window30sAccumulator, windowEndTs: Long): Window30sPayload = {
    val count    = acc.count
    val speedAvg = if count > 0 then acc.speedSum / count else 0.0
    val speedVar = if count > 0 then acc.speedSumSq / count - speedAvg * speedAvg else 0.0
    val speedStd = math.sqrt(math.max(speedVar, 0.0))

    val accelAvg  = if count > 1 then acc.accelSum / (count - 1) else 0.0
    val jerkAvg   = if acc.jerkCount > 0 then acc.jerkSum / acc.jerkCount else 0.0
    val turnAvg   = if acc.turnCount > 0 then acc.turnSum / acc.turnCount else 0.0

    Window30sPayload(
      speedAvg30s         = speedAvg,
      speedStd30s         = speedStd,
      accelerationAvg30s  = accelAvg,
      jerkAvg30s          = jerkAvg,
      turnRateAvg30s      = turnAvg,
      stopsCount30s       = acc.stopsCount.toInt,
      distanceTraveled30s = acc.distanceTraveled
    )
  }

  // ============================================================
  // 1-minute window
  // ============================================================

  def add1m(acc: Window1mAccumulator, ev: TelemetryEvent): Window1mAccumulator = {
    val newAccel: Option[Double] =
      acc.prevSpeed.map(prev => ev.speed - prev)

    val newAccelSum  = newAccel.fold(acc.accelSum)(a => acc.accelSum + a)
    val newJerkDelta =
      for
        curr <- newAccel
        prev <- acc.prevAccel
      yield curr - prev
    val newJerkSum   = newJerkDelta.fold(acc.jerkSum)(j => acc.jerkSum + j)
    val newJerkCount = newJerkDelta.fold(acc.jerkCount)(_ => acc.jerkCount + 1L)

    val newTurnDelta = acc.prevHeading.map(ph => math.abs(ev.heading - ph))
    val newTurnSum   = newTurnDelta.fold(acc.turnSum)(t => acc.turnSum + t)
    val newTurnCount = newTurnDelta.fold(acc.turnCount)(_ => acc.turnCount + 1L)

    val newDistance =
      (acc.prevLat, acc.prevLon) match
        case (Some(plat), Some(plon)) => acc.distanceTraveled + haversine(plat, plon, ev.lat, ev.lon)
        case _                        => acc.distanceTraveled

    acc.copy(
      speedSum         = acc.speedSum + ev.speed,
      speedSumSq       = acc.speedSumSq + ev.speed * ev.speed,
      count            = acc.count + 1L,
      prevSpeed        = Some(ev.speed),
      prevAccel        = newAccel.orElse(acc.prevAccel),
      accelSum         = newAccelSum,
      jerkSum          = newJerkSum,
      jerkCount        = newJerkCount,
      prevHeading      = Some(ev.heading),
      turnSum          = newTurnSum,
      turnCount        = newTurnCount,
      idleCount        = if ev.speed < 0.5 then acc.idleCount + 1L else acc.idleCount,
      distanceTraveled = newDistance,
      prevLat          = Some(ev.lat),
      prevLon          = Some(ev.lon),
      lastSeenTs       = ev.ts,
      lastEvent        = Some(ev)   // ← new
    )
  }

  def result1m(acc: Window1mAccumulator, windowEndTs: Long): Window1mPayload = {
    val count    = acc.count
    val speedAvg = if count > 0 then acc.speedSum / count else 0.0
    val speedVar = if count > 0 then acc.speedSumSq / count - speedAvg * speedAvg else 0.0
    val speedStd = math.sqrt(math.max(speedVar, 0.0))

    val accelAvg  = if count > 1 then acc.accelSum / (count - 1) else 0.0
    val jerkAvg   = if acc.jerkCount > 0 then acc.jerkSum / acc.jerkCount else 0.0
    val turnAvg   = if acc.turnCount > 0 then acc.turnSum / acc.turnCount else 0.0
    val idleRatio = if count > 0 then acc.idleCount.toDouble / count.toDouble else 0.0
    val congestionLevel = if speedAvg < 5.0 then 1.0 else 0.0

    Window1mPayload(
      speedAvg1m         = speedAvg,
      speedStd1m         = speedStd,
      accelerationAvg1m  = accelAvg,
      jerkAvg1m          = jerkAvg,
      turnRateAvg1m      = turnAvg,
      idleRatio1m        = idleRatio,
      distanceTraveled1m = acc.distanceTraveled,
      congestionLevel1m  = congestionLevel
    )
  }

  // ============================================================
  // 5-minute window
  // ============================================================

  def add5m(acc: Window5mAccumulator, ev: TelemetryEvent): Window5mAccumulator = {
    val newAccel: Option[Double] =
      acc.prevSpeed.map(prev => ev.speed - prev)

    val newAccelSum  = newAccel.fold(acc.accelSum)(a => acc.accelSum + a)
    val newJerkDelta =
      for
        curr <- newAccel
        prev <- acc.prevAccel
      yield curr - prev
    val newJerkSum   = newJerkDelta.fold(acc.jerkSum)(j => acc.jerkSum + j)
    val newJerkCount = newJerkDelta.fold(acc.jerkCount)(_ => acc.jerkCount + 1L)

    val newTurnDelta = acc.prevHeading.map(ph => math.abs(ev.heading - ph))
    val newTurnSum   = newTurnDelta.fold(acc.turnSum)(t => acc.turnSum + t)
    val newTurnCount = newTurnDelta.fold(acc.turnCount)(_ => acc.turnCount + 1L)

    val newDistance =
      (acc.prevLat, acc.prevLon) match
        case (Some(plat), Some(plon)) => acc.distanceTraveled + haversine(plat, plon, ev.lat, ev.lon)
        case _                        => acc.distanceTraveled

    acc.copy(
      speedSum         = acc.speedSum + ev.speed,
      speedSumSq       = acc.speedSumSq + ev.speed * ev.speed,
      count            = acc.count + 1L,
      prevSpeed        = Some(ev.speed),
      prevAccel        = newAccel.orElse(acc.prevAccel),
      accelSum         = newAccelSum,
      jerkSum          = newJerkSum,
      jerkCount        = newJerkCount,
      prevHeading      = Some(ev.heading),
      turnSum          = newTurnSum,
      turnCount        = newTurnCount,
      idleCount        = if ev.speed < 0.5 then acc.idleCount + 1L else acc.idleCount,
      distanceTraveled = newDistance,
      prevLat          = Some(ev.lat),
      prevLon          = Some(ev.lon),
      lastSeenTs       = ev.ts,
      lastEvent        = Some(ev)   // ← new
    )
  }

  def result5m(acc: Window5mAccumulator, windowEndTs: Long): Window5mPayload = {
    val count    = acc.count
    val speedAvg = if count > 0 then acc.speedSum / count else 0.0
    val speedVar = if count > 0 then acc.speedSumSq / count - speedAvg * speedAvg else 0.0
    val speedStd = math.sqrt(math.max(speedVar, 0.0))

    val accelAvg  = if count > 1 then acc.accelSum / (count - 1) else 0.0
    val jerkAvg   = if acc.jerkCount > 0 then acc.jerkSum / acc.jerkCount else 0.0
    val turnAvg   = if acc.turnCount > 0 then acc.turnSum / acc.turnCount else 0.0
    val idleRatio = if count > 0 then acc.idleCount.toDouble / count.toDouble else 0.0
    val congestionLevel = if speedAvg < 5.0 then 1.0 else 0.0

    Window5mPayload(
      speedAvg5m         = speedAvg,
      speedStd5m         = speedStd,
      accelerationAvg5m  = accelAvg,
      jerkAvg5m          = jerkAvg,
      turnRateAvg5m      = turnAvg,
      idleRatio5m        = idleRatio,
      distanceTraveled5m = acc.distanceTraveled,
      congestionLevel5m  = congestionLevel
    )
  }
}
