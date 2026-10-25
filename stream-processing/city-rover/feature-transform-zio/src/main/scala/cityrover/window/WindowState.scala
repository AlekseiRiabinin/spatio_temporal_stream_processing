package cityrover.window


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
  * All timestamps (`windowStartTs`, `lastSeenTs`) are in MILLISECONDS,
  * event-time. They mirror the corresponding Flink window boundaries so
  * the two jobs produce identical `windowEndTs` values.
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
  lastSeenTs: Long
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
  lastSeenTs: Long
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
  lastSeenTs: Long
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
  lastSeenTs: Long
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
    lastSeenTs = Long.MinValue
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
    lastSeenTs = Long.MinValue
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
    lastSeenTs = Long.MinValue
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
    lastSeenTs = Long.MinValue
  )

// ============================================================
// Union type used by the state map
// ============================================================

/** Discriminated union of the four window accumulators.
  *
  * The window state map is keyed by `(roverId, WindowType)`, but the map's
  * value type must accommodate any of the four accumulators. This sealed
  * trait is the type-erasable way to do that without `Any`, and it gives
  * `EventTimeWindow` exhaustive pattern matches when it walks the map.
  */
sealed trait WindowAccumulator:
  /** When was the last event merged into this accumulator? Event-time ms. */
  def lastSeenTs: Long

final case class Acc5s(state: Window5sAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs

final case class Acc30s(state: Window30sAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs

final case class Acc1m(state: Window1mAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs

final case class Acc5m(state: Window5mAccumulator) extends WindowAccumulator:
  def lastSeenTs: Long = state.lastSeenTs
