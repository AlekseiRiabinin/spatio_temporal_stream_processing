package cityrover.window


/** Per-rover watermark tracking, replacing Flink's `WatermarkStrategy`.
  *
  * In Flink, `forBoundedOutOfOrderness(5s)` advances a single job-wide
  * watermark as `max(ts) - allowedLateness`, and `withIdleness(30s)`
  * lets idle sources be excluded so the watermark can still advance.
  * Here we do the same thing, but per rover:
  *
  *   - `advance` on every event: `wm = max(currentWm, ts - allowedLatenessMs)`
  *     so out-of-order events don't regress the watermark.
  *   - `advance` on idle-tick: `wm = max(currentWm, now - allowedLatenessMs)`
  *     so a silent rover's pending windows can still fire.
  *
  * Why per-rover rather than global: each rover's events arrive on the
  * same Kafka partition (raw producer keys by `roverId`), so per-partition
  * state maps naturally to per-rover state, and a slow rover can't stall
  * a fast rover's window firing. Flink's single job-wide watermark is a
  * consequence of its operator model; ZIO's per-partition streams don't
  * impose that.
  *
  * All timestamps are event-time milliseconds since epoch.
  */

/** Immutable watermark state for a set of rovers.
  *
  * `watermarks`:    roverId -> current watermark (max seen ts - lateness)
  * `lastEventTs`:   roverId -> ts of the most recent event (for idleness)
  * `lastAdvanceTs`: roverId -> wall-clock at the last advance (for ticks)
  */
final case class WatermarkState(
  watermarks: Map[String, Long],
  lastEventTs: Map[String, Long],
  lastAdvanceTs: Map[String, Long]
):
  def watermarkFor(roverId: String): Option[Long] = watermarks.get(roverId)
  def lastEventFor(roverId: String): Option[Long]  = lastEventTs.get(roverId)

object WatermarkState:
  val empty: WatermarkState = WatermarkState(Map.empty, Map.empty, Map.empty)

object WatermarkTracker {

  /** Advance the watermark for one rover on an incoming event.
    *
    * Called once per `TelemetryEvent` as it enters the window state machine.
    *
    * @param state              current watermark state
    * @param roverId            which rover the event belongs to
    * @param eventTs            the event's event-time (`ev.ts`, ms)
    * @param allowedLatenessMs  same knob as Flink's `forBoundedOutOfOrderness`
    * @param nowWallClockMs     wall clock at the time of the event (only stored,
    *                           not used in the watermark formula itself)
    * @return the updated state; the previous state is not mutated
    */
  def advance(
    state: WatermarkState,
    roverId: String,
    eventTs: Long,
    allowedLatenessMs: Long,
    nowWallClockMs: Long
  ): WatermarkState =
    val candidate = eventTs - allowedLatenessMs
    val newWm =
      state.watermarks.get(roverId) match
        case Some(prev) => math.max(prev, candidate)
        case None       => candidate

    state.copy(
      watermarks = state.watermarks.updated(roverId, newWm),
      lastEventTs = state.lastEventTs.updated(roverId, eventTs),
      lastAdvanceTs = state.lastAdvanceTs.updated(roverId, nowWallClockMs)
    )

  /** Advance the watermark for one rover purely from wall clock, without
    * a new event. Used by the idleness tick: a rover that hasn't sent
    * anything for `idlenessMs` should have its watermark pushed to
    * `now - allowedLatenessMs - idlenessMs`, so its pending windows can
    * fire (or be evicted) rather than lingering forever.
    *
    * If the rover has never been seen, this is a no-op — there is nothing
    * to advance, and creating a phantom watermark entry would be misleading.
    */
  def advanceForIdleness(
    state: WatermarkState,
    roverId: String,
    allowedLatenessMs: Long,
    idlenessMs: Long,
    nowWallClockMs: Long
  ): WatermarkState =
    state.lastEventTs.get(roverId) match
      case None => state
      case Some(lastSeen) =>
        val idleSince = nowWallClockMs - lastSeen
        if idleSince < idlenessMs then state
        else
          val candidate = nowWallClockMs - allowedLatenessMs - idlenessMs
          val newWm =
            state.watermarks.get(roverId) match
              case Some(prev) => math.max(prev, candidate)
              case None       => candidate
          state.copy(
            watermarks = state.watermarks.updated(roverId, newWm),
            lastAdvanceTs = state.lastAdvanceTs.updated(roverId, nowWallClockMs)
          )

  /** Has a window ending at `windowEndTs` fired for this rover?
    *
    * Fire rule: `wm >= windowEnd`. This is the same predicate Flink uses
    * (`watermark >= window.maxTimestamp()` for tumbling event-time windows).
    *
    * If the rover has no watermark yet (never seen an event), returns
    * `false` — no window should fire for a rover that's never appeared.
    */
  def shouldFire(
    state: WatermarkState,
    roverId: String,
    windowEndTs: Long
  ): Boolean =
    state.watermarks.get(roverId) match
      case Some(wm) => wm >= windowEndTs
      case None     => false

  /** Should a rover be evicted from the watermark state?
    *
    * We keep watermark entries indefinitely by default (they're tiny —
    * two Longs per rover), but if the fleet is very large and some
    * rovers never come back, this gives `EventTimeWindow` a way to drop
    * them. The threshold is a multiple of idleness: `idleSince > 2 *
    * idlenessMs` means the rover has been silent for at least two idle
    * periods, so it's safe to forget.
    */
  def shouldEvict(
    state: WatermarkState,
    roverId: String,
    idlenessMs: Long,
    nowWallClockMs: Long
  ): Boolean =
    state.lastEventTs.get(roverId) match
      case None       => false
      case Some(last) => (nowWallClockMs - last) > 2 * idlenessMs

  /** Drop a rover's watermark and last-seen entries. */
  def evict(state: WatermarkState, roverId: String): WatermarkState =
    state.copy(
      watermarks = state.watermarks - roverId,
      lastEventTs = state.lastEventTs - roverId,
      lastAdvanceTs = state.lastAdvanceTs - roverId
    )
}
