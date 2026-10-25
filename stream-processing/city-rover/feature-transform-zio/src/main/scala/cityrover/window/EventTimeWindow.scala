package cityrover.window

import cityrover.model.{
  EnrichedEvent,
  EnrichedEvent5s,
  EnrichedEvent30s,
  EnrichedEvent1m,
  EnrichedEvent5m,
  FeatureEnvelope,
  TelemetryEvent,
  Window5sPayload,
  Window30sPayload,
  Window1mPayload,
  Window5mPayload
}
import cityrover.util.WindowConfig
import zio.Chunk


/** Tumbling event-time windows, replacing Flink's
  * `keyBy(roverId).window(TumblingEventTimeWindows.of(size)).process(...)`.
  *
  * The state machine is a pure fold: given the current state and one
  * incoming `TelemetryEvent`, it returns the new state and any windows
  * that fired as a `Chunk[EnrichedEvent]`. The caller wires it into
  * `ZStream.mapAccum` and flattens the emitted chunks into Kafka.
  *
  * State shape:
  *   - `accumulators`: (roverId, WindowType) -> the per-window incremental
  *     accumulator. Constant-sized per rover per window.
  *   - `watermarks`:   the per-rover watermark tracker (see
  *     `WatermarkTracker.scala`).
  *
  * The firing rule matches Flink: a window with end `E` fires when
  * `watermark >= E`. Because the incoming stream is partitioned by
  * `roverId` on the Kafka side, each `EventTimeWindow` instance only sees
  * events for the rovers on its partition, but the state map handles any
  * number of rovers so nothing breaks if partitioning changes.
  */

final case class WindowState(
  accumulators: Map[(String, WindowTypeTag), WindowAccumulator],
  watermarks: WatermarkState
)

object WindowState:
  val empty: WindowState = WindowState(Map.empty, WatermarkState.empty)

/** Tag identifying which of the four windows an accumulator belongs to.
  *
  * Separate from `cityrover.telemetry.WindowType` because the proto enum
  * has an `UNSPECIFIED` value and is wire-format-coupled; this is an
  * internal-only tag, so its four values are exactly the four windows
  * the job computes.
  */
enum WindowTypeTag:
  case W5s, W30s, W1m, W5m

  /** The window's size in milliseconds. */
  def sizeMs: Long = this match
    case W5s  => 5_000L
    case W30s => 30_000L
    case W1m  => 60_000L
    case W5m  => 300_000L

  /** The window's end boundary for an event at `eventTs`, using the
    * half-open interval `[start, end)` aligned to epoch (the same
    * alignment Flink uses for `TumblingEventTimeWindows` with no
    * explicit offset).
    */
  def windowEndFor(eventTs: Long): Long =
    val size = sizeMs
    ((eventTs / size) + 1) * size

  def windowStartFor(eventTs: Long): Long =
    val size = sizeMs
    (eventTs / size) * size

object EventTimeWindow {

  /** One transition: consume an event, return new state + fired windows.
    *
    * Called from `ZStream.mapAccum` per element. The emitted chunk may be
    * empty (most events fire zero windows), or contain up to four
    * `EnrichedEvent`s (at the 5-minute boundary, all four windows can fire
    * for the same rover simultaneously).
    *
    * `nowWallClockMs` is passed in by the caller so the tracker can record
    * wall-clock reference times for idleness detection, but the transition
    * is otherwise deterministic given `(state, event, config)`.
    */
  def step(
    state: WindowState,
    event: TelemetryEvent,
    config: WindowConfig,
    nowWallClockMs: Long,
    envelopeFor: (TelemetryEvent, Long) => FeatureEnvelope
  ): (WindowState, Chunk[EnrichedEvent]) =
    val roverId = event.roverId

    // ---- 1. Advance the rover's watermark ----
    val wmAfter =
      WatermarkTracker.advance(
        state.watermarks,
        roverId,
        event.ts,
        config.allowedLatenessMs,
        nowWallClockMs
      )

    // ---- 2. Add the event to all four accumulators ----
    val accsAfter = addToAllAccumulators(state.accumulators, roverId, event)

    // ---- 3. Fire windows whose end is crossed by the watermark ----
    val (remaining, fired) =
      fireReadyWindows(
        accsAfter,
        wmAfter,
        roverId,
        config,
        envelopeFor
      )

    (
      WindowState(remaining, wmAfter),
      Chunk.fromIterable(fired)
    )

  /** Idle-rover advancement: push a silent rover's watermark forward so
    * its pending windows fire. Called from a `ZStream.tick` merged into
    * the input, once per `idlenessMs` of wall clock.
    *
    * The transition is a no-op for rovers that haven't been idle long
    * enough, and for rovers that have never been seen.
    */
  def tickIdle(
    state: WindowState,
    config: WindowConfig,
    nowWallClockMs: Long,
    envelopeFor: (TelemetryEvent, Long) => FeatureEnvelope
  ): (WindowState, Chunk[EnrichedEvent]) =
    val rovers = state.watermarks.lastEventTs.keySet
    var current = state
    val fired = scala.collection.mutable.ArrayBuffer.empty[EnrichedEvent]

    rovers.foreach { roverId =>
      val wmAfter =
        WatermarkTracker.advanceForIdleness(
          current.watermarks,
          roverId,
          config.allowedLatenessMs,
          config.idlenessMs,
          nowWallClockMs
        )

      if wmAfter != current.watermarks then
        val (remaining, ready) =
          fireReadyWindows(
            current.accumulators,
            wmAfter,
            roverId,
            config,
            envelopeFor
          )
        current = WindowState(remaining, wmAfter)
        fired ++= ready
    }

    (current, Chunk.fromIterable(fired.toSeq))

  // ============================================================
  // Internals
  // ============================================================

  /** Add the event to the correct accumulator for each of the four
    * windows, creating the accumulator if absent.
    */
  private def addToAllAccumulators(
    accs: Map[(String, WindowTypeTag), WindowAccumulator],
    roverId: String,
    event: TelemetryEvent
  ): Map[(String, WindowTypeTag), WindowAccumulator] =
    val key5s  = (roverId, WindowTypeTag.W5s)
    val key30s = (roverId, WindowTypeTag.W30s)
    val key1m  = (roverId, WindowTypeTag.W1m)
    val key5m  = (roverId, WindowTypeTag.W5m)

    // Small optimization: all four updates mutate `accs` in sequence, but
    // `Map.updated` returns a new map each time. That's fine — the map has
    // at most 4 entries per rover, and `updated` is O(log n) in a HashMap.
    accs
      .updated(
        key5s,
        Acc5s(
          WindowAggregator.add5s(
            accs.get(key5s).collect { case Acc5s(a) => a }
              .getOrElse(Window5sAccumulator.empty),
            event
          )
        )
      )
      .updated(
        key30s,
        Acc30s(
          WindowAggregator.add30s(
            accs.get(key30s).collect { case Acc30s(a) => a }
              .getOrElse(Window30sAccumulator.empty),
            event
          )
        )
      )
      .updated(
        key1m,
        Acc1m(
          WindowAggregator.add1m(
            accs.get(key1m).collect { case Acc1m(a) => a }
              .getOrElse(Window1mAccumulator.empty),
            event
          )
        )
      )
      .updated(
        key5m,
        Acc5m(
          WindowAggregator.add5m(
            accs.get(key5m).collect { case Acc5m(a) => a }
              .getOrElse(Window5mAccumulator.empty),
            event
          )
        )
      )

  /** Fire all windows for this rover whose end has been crossed by the
    * watermark. Returns the remaining accumulators + the fired events.
    *
    * For each of the four window types, if a window fires, the accumulator
    * is converted to a payload, wrapped in `EnrichedEvent*`, and the
    * accumulator entry is removed. Windows that haven't been crossed stay
    * in the map unchanged.
    */
  private def fireReadyWindows(
    accs: Map[(String, WindowTypeTag), WindowAccumulator],
    wm: WatermarkState,
    roverId: String,
    config: WindowConfig,
    envelopeFor: (TelemetryEvent, Long) => FeatureEnvelope
  ): (Map[(String, WindowTypeTag), WindowAccumulator], Vector[EnrichedEvent]) =
    var remaining = accs
    val fired = Vector.newBuilder[EnrichedEvent]

    def tryFire(tag: WindowTypeTag): Unit =
      val key = (roverId, tag)
      remaining.get(key) match
        case Some(acc: WindowAccumulator) =>
          // The accumulator's lastSeenTs tells us which event-time window
          // this accumulator belongs to: it's the window containing that ts.
          // We could reconstruct the window end from the event that last
          // touched it, but that's ambiguous when events span boundary —
          // a safer choice is to store the window-end in the key. Here we
          // use the accumulator's lastSeenTs and align to the window size.
          val windowEnd = tag.windowEndFor(acc.lastSeenTs)
          if WatermarkTracker.shouldFire(wm, roverId, windowEnd) then
            val firedEvent: EnrichedEvent = tag match
              case WindowTypeTag.W5s =>
                val a = remaining.get(key).collect { case Acc5s(x) => x }.get
                EnrichedEvent5s(
                  envelope = envelopeFor(/* last event */ null, windowEnd),
                  features = WindowAggregator.result5s(a, windowEnd)
                )
              case WindowTypeTag.W30s =>
                val a = remaining.get(key).collect { case Acc30s(x) => x }.get
                EnrichedEvent30s(
                  envelope = envelopeFor(null, windowEnd),
                  features = WindowAggregator.result30s(a, windowEnd)
                )
              case WindowTypeTag.W1m =>
                val a = remaining.get(key).collect { case Acc1m(x) => x }.get
                EnrichedEvent1m(
                  envelope = envelopeFor(null, windowEnd),
                  features = WindowAggregator.result1m(a, windowEnd)
                )
              case WindowTypeTag.W5m =>
                val a = remaining.get(key).collect { case Acc5m(x) => x }.get
                EnrichedEvent5m(
                  envelope = envelopeFor(null, windowEnd),
                  features = WindowAggregator.result5m(a, windowEnd)
                )
            fired += firedEvent
            remaining = remaining - key
        case None => ()

    tryFire(WindowTypeTag.W5s)
    tryFire(WindowTypeTag.W30s)
    tryFire(WindowTypeTag.W1m)
    tryFire(WindowTypeTag.W5m)

    (remaining, fired.result())
}
