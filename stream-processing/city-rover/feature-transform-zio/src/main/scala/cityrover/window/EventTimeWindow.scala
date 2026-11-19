package cityrover.window

import scala.collection.mutable.ArrayBuffer
import zio.Chunk

import cityrover.model.{
  EnrichedEvent,
  EnrichedEvent5s,
  EnrichedEvent30s,
  EnrichedEvent1m,
  EnrichedEvent5m,
  FeatureEnvelope,
  TelemetryEvent
}
import cityrover.util.WindowConfig


/** Tumbling event-time windows, replacing Flink's
  * `keyBy(roverId).window(TumblingEventTimeWindows.of(size)).process(...)`.
  *
  * The state machine is a pure fold: given the current state and one
  * incoming `TelemetryEvent`, it returns the new state and any windows
  * that fired as a `Chunk[EnrichedEvent]`. The caller wires it into
  * `ZStream.mapAccum` and flattens the emitted chunks into Kafka.
  *
  * The firing rule matches Flink: a window with end `E` fires when
  * `watermark >= E`. Because the incoming stream is partitioned by
  * `roverId` on the Kafka side, each `EventTimeWindow` instance only sees
  * events for the rovers on its partition, but the state map handles any
  * number of rovers so nothing breaks if partitioning changes.
  *
  * Accumulator keying: `(roverId, WindowTypeTag, windowEnd)`.
  * One accumulator per *window instance*, not per rover per window type.
  * This is what lets the 5s window `[100000, 105000)` fire independently
  * of `[105000, 110000)` — events for the second window don't pollute
  * the first.
  */

final case class WindowState(
  accumulators: Map[(String, WindowTypeTag, Long), WindowAccumulator],
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
  *
  * Window sizes come from `WindowConfig` — no hardcoded millisecond
  * values, so `application.conf` is authoritative.
  */
enum WindowTypeTag:
  case W5s, W30s, W1m, W5m

  /** The window's size in milliseconds, from the config. */
  def sizeMs(config: WindowConfig): Long = this match
    case W5s  => config.sizeMs5s
    case W30s => config.sizeMs30s
    case W1m  => config.sizeMs1m
    case W5m  => config.sizeMs5m

  /** The window's end boundary for an event at `eventTs`, using the
    * half-open interval `[start, end)` aligned to epoch (the same
    * alignment Flink uses for `TumblingEventTimeWindows` with no
    * explicit offset).
    */
  def windowEndFor(eventTs: Long, config: WindowConfig): Long =
    val size = sizeMs(config)
    ((eventTs / size) + 1) * size

  def windowStartFor(eventTs: Long, config: WindowConfig): Long =
    val size = sizeMs(config)
    (eventTs / size) * size

object EventTimeWindow {

  /** One transition: consume an event, return new state + fired windows.
    *
    * Called from `ZStream.mapAccum` per element. The emitted chunk may be
    * empty (most events fire zero windows), or contain up to four
    * `EnrichedEvent`s (at the 5-minute boundary, all four windows can fire
    * for the same rover simultaneously).
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

    // ---- 2. Add the event to the four window accumulators it belongs to ----
    val accsAfter = addToAllAccumulators(state.accumulators, roverId, event, config)

    // ---- 3. Fire every window whose end the watermark has crossed ----
    val (remaining, fired) =
      fireReadyWindows(accsAfter, wmAfter, roverId, envelopeFor)

    (WindowState(remaining, wmAfter), Chunk.fromIterable(fired))

  /** Idle-rover advancement: push a silent rover's watermark forward so
    * its pending windows fire. Called from a `ZStream.tick` merged into
    * the input, once per `idlenessMs` of wall clock.
    */
  def tickIdle(
    state: WindowState,
    config: WindowConfig,
    nowWallClockMs: Long,
    envelopeFor: (TelemetryEvent, Long) => FeatureEnvelope
  ): (WindowState, Chunk[EnrichedEvent]) =
    val rovers = state.watermarks.lastEventTs.keySet
    var current = state
    val fired = ArrayBuffer.empty[EnrichedEvent]

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
          fireReadyWindows(current.accumulators, wmAfter, roverId, envelopeFor)
        current = WindowState(remaining, wmAfter)
        fired ++= ready
    }

    (current, Chunk.fromIterable(fired.toSeq))

  // ============================================================
  // Internals
  // ============================================================

  /** Add the event to the correct accumulator for each of the four
    * windows. The key includes the window's end boundary, so an event
    * at `ts=105000` goes into `(roverId, W5s, 110000)`, not the same
    * accumulator as an event at `ts=100000` (which goes into
    * `(roverId, W5s, 105000)`).
    */
  private def addToAllAccumulators(
    accs: Map[(String, WindowTypeTag, Long), WindowAccumulator],
    roverId: String,
    event: TelemetryEvent,
    config: WindowConfig
  ): Map[(String, WindowTypeTag, Long), WindowAccumulator] =
    val wEnd5s  = WindowTypeTag.W5s.windowEndFor(event.ts, config)
    val wEnd30s = WindowTypeTag.W30s.windowEndFor(event.ts, config)
    val wEnd1m  = WindowTypeTag.W1m.windowEndFor(event.ts, config)
    val wEnd5m  = WindowTypeTag.W5m.windowEndFor(event.ts, config)

    val key5s  = (roverId, WindowTypeTag.W5s,  wEnd5s)
    val key30s = (roverId, WindowTypeTag.W30s, wEnd30s)
    val key1m  = (roverId, WindowTypeTag.W1m,  wEnd1m)
    val key5m  = (roverId, WindowTypeTag.W5m,  wEnd5m)

    val acc5s  = accs.get(key5s).collect  { case Acc5s(a)  => a }.getOrElse(Window5sAccumulator.empty)
    val acc30s = accs.get(key30s).collect { case Acc30s(a) => a }.getOrElse(Window30sAccumulator.empty)
    val acc1m  = accs.get(key1m).collect  { case Acc1m(a)  => a }.getOrElse(Window1mAccumulator.empty)
    val acc5m  = accs.get(key5m).collect  { case Acc5m(a)  => a }.getOrElse(Window5mAccumulator.empty)

    accs
      .updated(key5s,  Acc5s(WindowAggregator.add5s(acc5s, event)))
      .updated(key30s, Acc30s(WindowAggregator.add30s(acc30s, event)))
      .updated(key1m,  Acc1m(WindowAggregator.add1m(acc1m, event)))
      .updated(key5m,  Acc5m(WindowAggregator.add5m(acc5m, event)))

  /** Fire every window for this rover whose end the watermark has
    * crossed. Returns the remaining accumulators + the fired events.
    *
    * The window end comes from the accumulator key, not from
    * `windowEndFor(lastEvent.ts)`: the key *is* the window end. So a
    * window `[100000, 105000)` fires when the watermark reaches
    * `105000`, regardless of where `lastEvent.ts` falls (it's always
    * within `[100000, 105000)` by construction).
    *
    * The `envelopeFor` callback receives the true "last event in the
    * window", so the `FeatureEnvelope` snapshot fields (`lat`, `lon`,
    * `ts`, `speed`, `heading`, `edgeId`, `routeId`) come from that event,
    * not from the event that happened to advance the watermark.
    */
  private def fireReadyWindows(
    accs: Map[(String, WindowTypeTag, Long), WindowAccumulator],
    wm: WatermarkState,
    roverId: String,
    envelopeFor: (TelemetryEvent, Long) => FeatureEnvelope
  ): (Map[(String, WindowTypeTag, Long), WindowAccumulator], Vector[EnrichedEvent]) =
    var remaining = accs
    val fired = Vector.newBuilder[EnrichedEvent]

    // Find every accumulator entry for this rover whose window end the
    // watermark has crossed. This includes the window the current event
    // belongs to (if the event itself advanced the watermark past its
    // own end — possible with a large out-of-orderness allowance) and
    // every earlier window whose end has already been crossed.
    val ready: List[((String, WindowTypeTag, Long), WindowAccumulator)] =
      accs.iterator
        .filter { case ((rid, _, wEnd), _) =>
          rid == roverId && WatermarkTracker.shouldFire(wm, roverId, wEnd)
        }
        .toList

    ready.foreach { case (key @ (_, tag, wEnd), acc) =>
      acc.lastEvent match
        case Some(lastEv) =>
          val envelope = envelopeFor(lastEv, wEnd)
          val firedEvent: EnrichedEvent = tag match
            case WindowTypeTag.W5s =>
              EnrichedEvent5s(
                envelope = envelope,
                features = WindowAggregator.result5s(acc.asInstanceOf[Acc5s].state, wEnd)
              )
            case WindowTypeTag.W30s =>
              EnrichedEvent30s(
                envelope = envelope,
                features = WindowAggregator.result30s(acc.asInstanceOf[Acc30s].state, wEnd)
              )
            case WindowTypeTag.W1m =>
              EnrichedEvent1m(
                envelope = envelope,
                features = WindowAggregator.result1m(acc.asInstanceOf[Acc1m].state, wEnd)
              )
            case WindowTypeTag.W5m =>
              EnrichedEvent5m(
                envelope = envelope,
                features = WindowAggregator.result5m(acc.asInstanceOf[Acc5m].state, wEnd)
              )
          fired += firedEvent
          remaining = remaining - key
        case None =>
          // Accumulator exists but no event was ever added — shouldn't
          // happen (accumulator is created in addToAllAccumulators on
          // the first add*), but defensive: skip rather than crash.
          ()
    }

    (remaining, fired.result())
}
