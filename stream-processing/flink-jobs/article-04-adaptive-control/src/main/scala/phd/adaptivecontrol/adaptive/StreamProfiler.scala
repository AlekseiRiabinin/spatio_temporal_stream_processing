package phd.adaptivecontrol.adaptive

import scala.collection.mutable
import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.model.InteractionType._
import phd.adaptivecontrol.model.StreamFeatures


/**
 * StreamProfiler
 *
 * Runtime telemetry + ML feature extraction layer
 * for adaptive watermark/window control.
 *
 * Now fully self-contained:
 * - snapshot() computes processing latency internally
 * - FeatureExtractor does NOT inject external timing
 */
object StreamProfiler extends Serializable {

  // ============================================================
  // Event state
  // ============================================================
  private var totalEvents: Long = 0L
  private var lateEvents: Long = 0L
  private var outOfOrderEvents: Long = 0L

  private var lastEventTimestamp: Long = Long.MinValue
  private var accumulatedLatencyMs: Double = 0.0

  private val recentEventTimes = mutable.Queue[Long]()
  private val EventRateWindowMs = 5000L

  // ============================================================
  // Interaction state
  // ============================================================
  private var totalInteractions: Long = 0L

  private var collisionCount: Long = 0L
  private var proximityCount: Long = 0L
  private var swarmCount: Long = 0L
  private var conflictCount: Long = 0L

  // ============================================================
  // Window state
  // ============================================================
  private var totalWindows: Long = 0L
  private var totalWindowEvents: Long = 0L

  // ============================================================
  // PUBLIC READ ACCESS (safe aggregation layer)
  // ============================================================
  def getTotalEvents: Long = totalEvents
  def getTotalInteractions: Long = totalInteractions

  def getCollisionCount: Long = collisionCount
  def getProximityCount: Long = proximityCount
  def getSwarmCount: Long = swarmCount
  def getConflictCount: Long = conflictCount

  def getTotalWindows: Long = totalWindows
  def getTotalWindowEvents: Long = totalWindowEvents

  // ============================================================
  // EVENTS
  // ============================================================
  def updateEvents(events: Seq[GeoEvent]): Unit =
    events.foreach(observeEvent)

  private def observeEvent(event: GeoEvent): Unit = {

    val now = System.currentTimeMillis()
    totalEvents += 1

    // event rate window tracking
    recentEventTimes.enqueue(now)

    while (
      recentEventTimes.nonEmpty &&
      now - recentEventTimes.front > EventRateWindowMs
    ) {
      recentEventTimes.dequeue()
    }

    // disorder detection
    if (event.timestamp < lastEventTimestamp)
      outOfOrderEvents += 1

    // lateness heuristic
    if (event.timestamp < now - 1000)
      lateEvents += 1

    lastEventTimestamp =
      math.max(lastEventTimestamp, event.timestamp)

    // latency accumulation
    val latency = now - event.timestamp
    if (latency > 0)
      accumulatedLatencyMs += latency
  }

  // ============================================================
  // INTERACTIONS
  // ============================================================
  def updateInteractions(interactions: Seq[Interaction]): Unit = {

    totalInteractions += interactions.size

    interactions.foreach { i =>
      i.interactionType match {
        case Collision => collisionCount += 1
        case Proximity => proximityCount += 1
        case Swarm     => swarmCount += 1
        case Conflict  => conflictCount += 1
      }
    }
  }

  // ============================================================
  // WINDOWS
  // ============================================================
  def updateWindow(batchSize: Int): Unit = {
    totalWindows += 1
    totalWindowEvents += batchSize
  }

  // ============================================================
  // FEATURE METRICS
  // ============================================================
  def eventRate: Double =
    recentEventTimes.size.toDouble / (EventRateWindowMs / 1000.0)

  def disorderRatio: Double =
    if (totalEvents == 0) 0.0
    else outOfOrderEvents.toDouble / totalEvents

  def lateEventRatio: Double =
    if (totalEvents == 0) 0.0
    else lateEvents.toDouble / totalEvents

  def averageLatencyMs: Double =
    if (totalEvents == 0) 0.0
    else accumulatedLatencyMs / totalEvents

  def averageWindowFill: Double =
    if (totalWindows == 0) 0.0
    else totalWindowEvents.toDouble / totalWindows

  def interactionRate: Double =
    if (totalEvents == 0) 0.0
    else totalInteractions.toDouble / totalEvents

  // ============================================================
  // SNAPSHOT (UPDATED CORE CHANGE)
  // ============================================================
  def snapshot(): StreamFeatures = {

    val safeInteractions = math.max(totalInteractions, 1L)

    val collisionRate =
      collisionCount.toDouble / safeInteractions

    val proximityRate =
      proximityCount.toDouble / safeInteractions

    val swarmRate =
      swarmCount.toDouble / safeInteractions

    val conflictRate =
      conflictCount.toDouble / safeInteractions

    val now = System.currentTimeMillis()

    StreamFeatures(
      eventRate = eventRate,
      disorderRatio = disorderRatio,
      lateEventRatio = lateEventRatio,
      averageLatencyMs = averageLatencyMs,

      windowFillRatio = averageWindowFill,

      interactionRate = interactionRate,
      collisionRate = collisionRate,
      proximityRate = proximityRate,
      swarmRate = swarmRate,
      conflictRate = conflictRate,

      // still placeholder (watermark module comes later)
      watermarkLagMs = 0L,

      // internal latency (no external injection anymore)
      processingLatencyMs = now - lastEventTimestamp,

      timestamp = now
    )
  }

  // ============================================================
  // DEBUG
  // ============================================================
  def logSnapshot(): Unit = {
    println(
      "[METRIC] " +
        s"event_rate=${eventRate.formatted("%.2f")} " +
        s"disorder_ratio=${disorderRatio.formatted("%.4f")} " +
        s"late_event_ratio=${lateEventRatio.formatted("%.4f")} " +
        s"avg_latency_ms=${averageLatencyMs.formatted("%.2f")} " +
        s"avg_window_fill=${averageWindowFill.formatted("%.2f")} " +
        s"interaction_rate=${interactionRate.formatted("%.4f")} " +
        s"collision_count=$collisionCount " +
        s"proximity_count=$proximityCount " +
        s"swarm_count=$swarmCount " +
        s"conflict_count=$conflictCount"
    )
  }
}
