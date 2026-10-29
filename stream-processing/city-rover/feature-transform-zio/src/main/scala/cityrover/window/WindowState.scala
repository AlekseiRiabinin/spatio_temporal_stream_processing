package cityrover.window

import cityrover.model.TelemetryEvent


/** Incremental accumulator types for the four tumbling event-time windows.
  *
  * Each window keeps one accumulator per rover (keyed by `roverId` in the
  * window state map). The accumulators are constant-sized — they do NOT
  * hold raw events. That's what makes the 5-minute window cheap on
  * checkpoint/memory compared to a raw-event buffer.
  *
  * The fields are chosen so `WindowAggregator.add` is a local update and
  * `WindowAggregator.result` is a pure function of the accumulator. The
  * "prev*" fields exist because some features depend on the previous
  * element's value (jerk = Δaccel, turnRate = Δheading, distance via
  * haversine between consecutive events).
  *
  * `lastEvent` is the most recent event merged into the accumulator.
  * `EventTimeWindow` extracts it when a window fires, so
  * `FeatureEnvelopeBuilder` can build an envelope from the true
  * "latest event in the window" — not from the triggering event (which
  * may be much later if the watermark advanced on a subsequent chunk).
  *
  * All timestamps (`lastSeenTs`) are in MILLISECONDS, event-time.
  */

// ============================================================
// Per-window accumulators
// ============================================================

/** 5-second window accumulator (ultra-short term). */
final case class Window5sAccumulator(
  speedSum: Double,
  speedMax: Double,
  speedMin: Double,
  count: Long,
  firstHeading: Option[Double],
  lastHeading: Option[Double],
  distanceTraveled: Double,
  prevLat: Option[Double],
  prevLon: Option[Double],
  lastSeenTs: Long,
  lastEvent: Option[TelemetryEvent]
)

/** 30-second window accumulator (short term). */
final case class Window30sAccumulator(
  speedSum: Double,
  speedSumSq: Double,
  count: Long,
  prevSpeed: Option[Double],
  prevAccel: Option[Double],
  accelSum: Double,
  jerkSum: Double,
  jerkCount: Long,
  prevHeading: Option[Double],
  turnSum: Double,
  turnCount: Long,
  stopsCount: Long,
  distanceTraveled: Double,
  prevLat: Option[Double],
  prevLon: Option[Double],
  lastSeenTs: Long,
  lastEvent: Option[TelemetryEvent]
)

/** 1-minute window accumulator (medium term). */
final case class Window1mAccumulator(
  speedSum: Double,
  speedSumSq: Double,
  count: Long,
  prevSpeed: Option[Double],
  prevAccel: Option[Double],
  accelSum: Double,
  jerkSum: Double,
  jerkCount: Long,
  prevHeading: Option[Double],
  turnSum: Double,
  turnCount: Long,
  idleCount: Long,
  distanceTraveled: Double,
  prevLat: Option[Double],
  prevLon: Option[Double],
  lastSeenTs: Long,
  lastEvent: Option[TelemetryEvent]
)

/** 5-minute window accumulator (long term). */
final case class Window5mAccumulator(
  speedSum: Double,
  speedSumSq: Double,
  count: Long,
  prevSpeed: Option[Double],
  prevAccel: Option[Double],
  accelSum: Double,
  jerkSum: Double,
  jerkCount: Long,
  prevHeading: Option[Double],
  turnSum: Double,
  turnCount: Long,
  idleCount: Long,
  distanceTraveled: Double,
  prevLat: Option[Double],
  prevLon: Option[Double],
  lastSeenTs: Long,
  lastEvent: Option[TelemetryEvent]
)

// ============================================================
// Empty accumulators
// ============================================================

object Window5sAccumulator:
  val empty: Window5sAccumulator = Window5sAccumulator(
    speedSum = 0.0,
    speedMax = Double.MinValue,
    speedMin = Double.MaxValue,
    count = 0L,
    firstHeading = None,
    lastHeading = None,
    distanceTraveled = 0.0,
    prevLat = None,
    prevLon = None,
    lastSeenTs = Long.MinValue,
    lastEvent = None
  )

object Window30sAccumulator:
  val empty: Window30sAccumulator = Window30sAccumulator(
    speedSum = 0.0,
    speedSumSq = 0.0,
    count = 0L,
    prevSpeed = None,
    prevAccel = None,
    accelSum = 0.0,
    jerkSum = 0.0,
    jerkCount = 0L,
    prevHeading = None,
    turnSum = 0.0,
    turnCount = 0L,
    stopsCount = 0L,
    distanceTraveled = 0.0,
    prevLat = None,
    prevLon = None,
    lastSeenTs = Long.MinValue,
    lastEvent = None
  )

object Window1mAccumulator:
  val empty: Window1mAccumulator = Window1mAccumulator(
    speedSum = 0.0,
    speedSumSq = 0.0,
    count = 0L,
    prevSpeed = None,
    prevAccel = None,
    accelSum = 0.0,
    jerkSum = 0.0,
    jerkCount = 0L,
    prevHeading = None,
    turnSum = 0.0,
    turnCount = 0L,
    idleCount = 0L,
    distanceTraveled = 0.0,
    prevLat = None,
    prevLon = None,
    lastSeenTs = Long.MinValue,
    lastEvent = None
  )

object Window5mAccumulator:
  val empty: Window5mAccumulator = Window5mAccumulator(
    speedSum = 0.0,
    speedSumSq = 0.0,
    count = 0L,
    prevSpeed = None,
    prevAccel = None,
    accelSum = 0.0,
    jerkSum = 0.0,
    jerkCount = 0L,
    prevHeading = None,
    turnSum = 0.0,
    turnCount = 0L,
    idleCount = 0L,
    distanceTraveled = 0.0,
    prevLat = None,
    prevLon = None,
    lastSeenTs = Long.MinValue,
    lastEvent = None
  )

// ============================================================
// Union type used by the state map
// ============================================================

/** Discriminated union of the four window accumulators. */
sealed trait WindowAccumulator:
  /** When was the last event merged into this accumulator? Event-time ms. */
  def lastSeenTs: Long

  /** The most recent event merged into this accumulator. Used by
    * `EventTimeWindow.fireReadyWindows` to build a correct
    * `FeatureEnvelope` from the true latest event in the window. */
  def lastEvent: Option[TelemetryEvent]

final case class Acc5s(state: Window5sAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs
  def lastEvent: Option[TelemetryEvent] = state.lastEvent

final case class Acc30s(state: Window30sAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs
  def lastEvent: Option[TelemetryEvent] = state.lastEvent

final case class Acc1m(state: Window1mAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs
  def lastEvent: Option[TelemetryEvent] = state.lastEvent

final case class Acc5m(state: Window5mAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs
  def lastEvent: Option[TelemetryEvent] = state.lastEvent
