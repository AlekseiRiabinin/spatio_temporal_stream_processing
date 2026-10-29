package cityrover.window

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
import zio.Chunk


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

    // ---- 2. Add the event to all four accumulators ----
    val accsAfter = addToAllAccumulators(state.accumulators, roverId, event)

    // ---- 3. Fire windows whose end is crossed by the watermark ----
    val (remaining, fired) =
      fireReadyWindows(accsAfter, wmAfter, roverId, config, envelopeFor)

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
          fireReadyWindows(current.accumulators, wmAfter, roverId, config, envelopeFor)
        current = WindowState(remaining, wmAfter)
        fired ++= ready
    }

    (current, Chunk.fromIterable(fired.toSeq))

  // ============================================================
  // Internals
  // ============================================================

  private def addToAllAccumulators(
    accs: Map[(String, WindowTypeTag), WindowAccumulator],
    roverId: String,
    event: TelemetryEvent
  ): Map[(String, WindowTypeTag), WindowAccumulator] =
    val key5s  = (roverId, WindowTypeTag.W5s)
    val key30s = (roverId, WindowTypeTag.W30s)
    val key1m  = (roverId, WindowTypeTag.W1m)
    val key5m  = (roverId, WindowTypeTag.W5m)

    val acc5s  = accs.get(key5s).collect  { case Acc5s(a)  => a }.getOrElse(Window5sAccumulator.empty)
    val acc30s = accs.get(key30s).collect { case Acc30s(a) => a }.getOrElse(Window30sAccumulator.empty)
    val acc1m  = accs.get(key1m).collect  { case Acc1m(a)  => a }.getOrElse(Window1mAccumulator.empty)
    val acc5m  = accs.get(key5m).collect  { case Acc5m(a)  => a }.getOrElse(Window5mAccumulator.empty)

    accs
      .updated(key5s,  Acc5s(WindowAggregator.add5s(acc5s, event)))
      .updated(key30s, Acc30s(WindowAggregator.add30s(acc30s, event)))
      .updated(key1m,  Acc1m(WindowAggregator.add1m(acc1m, event)))
      .updated(key5m,  Acc5m(WindowAggregator.add5m(acc5m, event)))

  /** Fire all windows for this rover whose end has been crossed by the
    * watermark. Returns the remaining accumulators + the fired events.
    *
    * The `windowEndTs` is derived from the accumulator's `lastEvent.ts`
    * (the event-time of the latest event in the window), aligned to the
    * window's size. Using `lastEvent.ts` rather than `lastSeenTs` is
    * equivalent — `add*` sets both from the same event — but
    * `lastEvent.ts` is what the envelope builder also uses, so they stay
    * in lockstep.
    *
    * The `envelopeFor` callback receives the true "last event in the
    * window", so the `FeatureEnvelope` snapshot fields (`lat`, `lon`,
    * `ts`, `speed`, `heading`, `edgeId`, `routeId`) come from that event,
    * not from the event that happened to advance the watermark.
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
        case Some(acc) =>
          acc.lastEvent match
            case Some(lastEv) =>
              val windowEnd = tag.windowEndFor(lastEv.ts, config)
              if WatermarkTracker.shouldFire(wm, roverId, windowEnd) then
                val envelope = envelopeFor(lastEv, windowEnd)
                val firedEvent: EnrichedEvent = tag match
                  case WindowTypeTag.W5s =>
                    val a = acc.asInstanceOf[Acc5s].state
                    EnrichedEvent5s(
                      envelope = envelope,
                      features = WindowAggregator.result5s(a, windowEnd)
                    )
                  case WindowTypeTag.W30s =>
                    val a = acc.asInstanceOf[Acc30s].state
                    EnrichedEvent30s(
                      envelope = envelope,
                      features = WindowAggregator.result30s(a, windowEnd)
                    )
                  case WindowTypeTag.W1m =>
                    val a = acc.asInstanceOf[Acc1m].state
                    EnrichedEvent1m(
                      envelope = envelope,
                      features = WindowAggregator.result1m(a, windowEnd)
                    )
                  case WindowTypeTag.W5m =>
                    val a = acc.asInstanceOf[Acc5m].state
                    EnrichedEvent5m(
                      envelope = envelope,
                      features = WindowAggregator.result5m(a, windowEnd)
                    )
                fired += firedEvent
                remaining = remaining - key
            case None =>
              // Accumulator exists but no event was ever added — shouldn't
              // happen (accumulator is created in addToAllAccumulators on
              // the first add*), but defensive: skip rather than crash.
              ()
        case None => ()

    tryFire(WindowTypeTag.W5s)
    tryFire(WindowTypeTag.W30s)
    tryFire(WindowTypeTag.W1m)
    tryFire(WindowTypeTag.W5m)

    (remaining, fired.result())
}
