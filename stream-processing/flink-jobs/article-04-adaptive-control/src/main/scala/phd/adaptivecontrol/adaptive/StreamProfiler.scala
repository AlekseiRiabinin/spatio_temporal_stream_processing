package phd.adaptivecontrol.adaptive

import scala.collection.mutable

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{
  GeoEvent,
  Interaction,
  StreamFeatures,
  AdaptiveDecision
}
import phd.adaptivecontrol.model.InteractionType._


object StreamProfiler extends Serializable {

  // ============================================================
  // Event state
  // ============================================================

  private var totalEvents: Long = 0L
  private var lateEvents: Long = 0L
  private var outOfOrderEvents: Long = 0L

  // event-time tracking (STRICT)
  private var lastEventTimestamp: Long = Long.MinValue
  private var maxEventTimestampSeen: Long = Long.MinValue

  private var accumulatedEventLatencyMs: Double = 0.0

  private val recentEventTimes = mutable.Queue[Long]()
  private val EventRateWindowMs = 5000L

  // ============================================================
  // Latency model
  // ============================================================

  private var latencyEMA: Double = 0.0
  private val alpha: Double = 0.1

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
  // Watermark state
  // ============================================================

  private var currentWatermark: Long = Long.MinValue
  private var lastEffectiveWatermark: Long = Long.MinValue

  // ============================================================
  // Adaptive control state
  // ============================================================

  private var adaptationCount: Long = 0L

  private var lastPredictedWindowMs: Long = 0L
  private var lastPredictedWatermarkMs: Long = 0L

  private var lastInferenceLatencyMs: Double = 0.0
  private var totalInferenceLatencyMs: Double = 0.0

  private var windowChangeCount: Long = 0L
  private var watermarkChangeCount: Long = 0L

  // oscillation tracking
  private var windowOscillation: Long = 0L
  private var watermarkOscillation: Long = 0L

  private var lastWindowDirection: Int = 0
  private var lastWatermarkDirection: Int = 0

  // bounds tracking
  private var minWindowMs: Long = Long.MaxValue
  private var maxWindowMs: Long = Long.MinValue

  private var minWatermarkMs: Long = Long.MaxValue
  private var maxWatermarkMs: Long = Long.MinValue

  // adaptation interval tracking
  private var lastAdaptationTs: Long = 0L
  private var adaptationIntervalSumMs: Double = 0.0

  // ============================================================
  // Adaptive configuration
  // ============================================================

  private var config: Option[AdaptiveConfig] = None

  private var adaptiveWindowSizeMs: Long = 0L
  private var adaptiveWatermarkDelayMs: Long = 0L

  // ============================================================
  // ML categorical features
  // ============================================================

  private var profile: String = "mixed"
  private var ratePattern: String = "constant"
  private var motionMode: String = "random_walk"

  // ============================================================
  // Controller metrics
  // ============================================================

  private var previousWindowSizeMs: Long = 0L
  private var previousWatermarkDelayMs: Long = 0L

  // ============================================================
  // Processing metrics
  // ============================================================

  private var processingOperations: Long = 0L
  private var accumulatedProcessingLatencyMs: Double = 0.0
  private var processingEMA: Double = 0.0

  // ============================================================
  // Configuration
  // ============================================================

  def setConfig(cfg: AdaptiveConfig): Unit = {

    config = Some(cfg)

    adaptiveWindowSizeMs =
      cfg.windowSizeMs

    adaptiveWatermarkDelayMs =
      cfg.watermarkDelayMs

    previousWindowSizeMs =
      adaptiveWindowSizeMs

    previousWatermarkDelayMs =
      adaptiveWatermarkDelayMs

    minWindowMs =
      adaptiveWindowSizeMs

    maxWindowMs =
      adaptiveWindowSizeMs

    minWatermarkMs =
      adaptiveWatermarkDelayMs

    maxWatermarkMs =
      adaptiveWatermarkDelayMs

    profile = "mixed"
    ratePattern = "constant"
    motionMode = "random_walk"
  }

  def updateWatermark(wm: Long): Unit =
    currentWatermark = wm


  // ============================================================
  // Events
  // ============================================================

  def updateEvents(events: Seq[GeoEvent]): Unit =
    events.foreach(observeEvent)

  private def observeEvent(event: GeoEvent): Unit = {

    val now = System.currentTimeMillis()

    // ------------------------------------------------------------
    // Event count
    // ------------------------------------------------------------
    totalEvents += 1
    recentEventTimes.enqueue(now)

    while (
      recentEventTimes.nonEmpty &&
      now - recentEventTimes.front > EventRateWindowMs
    ) {
      recentEventTimes.dequeue()
    }

    // ------------------------------------------------------------
    // Order tracking
    // ------------------------------------------------------------
    if (event.timestamp < lastEventTimestamp)
      outOfOrderEvents += 1

    lastEventTimestamp =
      math.max(lastEventTimestamp, event.timestamp)

    maxEventTimestampSeen =
      math.max(maxEventTimestampSeen, event.timestamp)

    // ------------------------------------------------------------
    // Latency metrics
    // ------------------------------------------------------------
    val latencyMs: Double =
      math.max(0L, now - event.timestamp).toDouble

    accumulatedEventLatencyMs += latencyMs

    latencyEMA =
      if (latencyEMA == 0.0) latencyMs
      else alpha * latencyMs + (1.0 - alpha) * latencyEMA
  }


  // ============================================================
  // Interactions
  // ============================================================

  def updateInteractions(interactions: Seq[Interaction]): Unit = {

    totalInteractions += interactions.size

    interactions.foreach { interaction =>
      interaction.interactionType match {
        case Collision => collisionCount += 1
        case Proximity => proximityCount += 1
        case Swarm     => swarmCount += 1
        case Conflict  => conflictCount += 1
      }
    }
  }


  // ============================================================
  // Window statistics
  // ============================================================

  def updateWindow(batchSize: Int): Unit = {
    totalWindows += 1
    totalWindowEvents += batchSize
  }


  // ============================================================
  // Controller statistics
  // ============================================================

  def updateAdaptiveDecision(
    decision: AdaptiveDecision,
    inferenceLatencyMs: Double
  ): Unit = {

    adaptationCount += 1

    if (
      lastPredictedWindowMs != 0 &&
      lastPredictedWindowMs != decision.windowSizeMs
    )
      windowChangeCount += 1

    if (
      lastPredictedWatermarkMs != 0 &&
      lastPredictedWatermarkMs != decision.watermarkDelayMs
    )
      watermarkChangeCount += 1

    val windowDir =
      math.signum(decision.windowSizeMs - lastPredictedWindowMs).toInt

    val watermarkDir =
      math.signum(decision.watermarkDelayMs - lastPredictedWatermarkMs).toInt

    // -------------------------------
    // Oscillation detection
    // -------------------------------

    if (
      windowDir != 0 &&
      lastWindowDirection != 0 &&
      windowDir != lastWindowDirection
    )
      windowOscillation += 1

    if (
      watermarkDir != 0 &&
      lastWatermarkDirection != 0 &&
      watermarkDir != lastWatermarkDirection
    )
      watermarkOscillation += 1

    lastWindowDirection = windowDir
    lastWatermarkDirection = watermarkDir

    // -------------------------------
    // State update
    // -------------------------------

    lastPredictedWindowMs = decision.windowSizeMs
    lastPredictedWatermarkMs = decision.watermarkDelayMs

    adaptiveWindowSizeMs = decision.windowSizeMs
    adaptiveWatermarkDelayMs = decision.watermarkDelayMs

    lastInferenceLatencyMs = inferenceLatencyMs
    totalInferenceLatencyMs += inferenceLatencyMs

    val now = System.currentTimeMillis()

    if (lastAdaptationTs != 0L)
      adaptationIntervalSumMs += (now - lastAdaptationTs)

    lastAdaptationTs = now

    minWindowMs = math.min(minWindowMs, decision.windowSizeMs)
    maxWindowMs = math.max(maxWindowMs, decision.windowSizeMs)

    minWatermarkMs = math.min(minWatermarkMs, decision.watermarkDelayMs)
    maxWatermarkMs = math.max(maxWatermarkMs, decision.watermarkDelayMs)
  }


  // ============================================================
  // Processing latency
  // ============================================================

  def recordProcessingLatency(latencyMs: Double): Unit = {
    processingOperations += 1
    accumulatedProcessingLatencyMs += latencyMs
  }


  // ============================================================
  // Derived Metrics
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
    else accumulatedEventLatencyMs / totalEvents

  def averageEventsPerWindow: Double =
    if (totalWindows == 0) 0.0
    else totalWindowEvents.toDouble / totalWindows

  def normalizedWindowFillRatio: Double = {
    val expectedEvents = if (eventRate <= 0) 1.0 
                         else eventRate * adaptiveWindowSizeMs / 1000.0
    if (expectedEvents <= 0) 0.0 
    else averageEventsPerWindow / expectedEvents
  }

  def interactionRate: Double =
    if (totalEvents == 0) 0.0
    else totalInteractions.toDouble / totalEvents

  def averageProcessingLatencyMs: Double =
    if (processingOperations == 0) 0.0
    else accumulatedProcessingLatencyMs / processingOperations

  def averageInferenceLatencyMs: Double =
    if (adaptationCount == 0) 0.0
    else totalInferenceLatencyMs / adaptationCount

  def windowChangeRate: Double =
    if (adaptationCount == 0) 0.0
    else windowChangeCount.toDouble / adaptationCount

  def watermarkChangeRate: Double =
    if (adaptationCount == 0) 0.0
    else watermarkChangeCount.toDouble / adaptationCount

  def windowOscillationRate: Double =
    if (adaptationCount == 0) 0.0
    else windowOscillation.toDouble / adaptationCount

  def watermarkOscillationRate: Double =
    if (adaptationCount == 0) 0.0
    else watermarkOscillation.toDouble / adaptationCount

  def avgAdaptationIntervalMs: Double =
    if (adaptationCount <= 1) 0.0
    else adaptationIntervalSumMs / (adaptationCount - 1)


  // ============================================================
  // Interaction metrics
  // ============================================================

  def collisionRate: Double =
    collisionCount.toDouble / math.max(totalInteractions, 1L)

  def proximityRate: Double =
    proximityCount.toDouble / math.max(totalInteractions, 1L)

  def swarmRate: Double =
    swarmCount.toDouble / math.max(totalInteractions, 1L)

  def conflictRate: Double =
    conflictCount.toDouble / math.max(totalInteractions, 1L)


  // ============================================================
  // Watermark metrics
  // ============================================================

  def watermarkLagMs: Long =
    if (currentWatermark == Long.MinValue) 0L
    else math.max(0L, lastEventTimestamp - currentWatermark)


  // ============================================================
  // Stream lag metrics
  // ============================================================

  def ingestionLagMs: Long = {

    val now =
      System.currentTimeMillis()

    if (totalEvents == 0) 0L
    else math.max(0L, now - lastEventTimestamp)
  }


  // ============================================================
  // Snapshot for ML
  // ============================================================

  def snapshot(): StreamFeatures = {

    val now = System.currentTimeMillis()
    val safeInteractions = math.max(totalInteractions, 1L)

    // Event‑time watermark lag (Layer 3)
    val eventTimeWatermarkLag =
      if (currentWatermark == Long.MinValue) 0L
      else math.max(0L, lastEventTimestamp - currentWatermark)

    // Ingestion lag
    val ingestionLag =
      if (totalEvents == 0) 0L
      else math.max(0L, now - lastEventTimestamp)

    StreamFeatures(
      profile = profile,
      ratePattern = ratePattern,
      motionMode = motionMode,

      eventRate = eventRate,
      disorderRatio = disorderRatio,
      lateEventRatio = lateEventRatio,
      averageLatencyMs = averageLatencyMs,

      windowFillRatio = normalizedWindowFillRatio,

      interactionRate = interactionRate,
      collisionRate = collisionCount.toDouble / safeInteractions,
      proximityRate = proximityCount.toDouble / safeInteractions,
      swarmRate = swarmCount.toDouble / safeInteractions,
      conflictRate = conflictCount.toDouble / safeInteractions,

      watermarkLagMs = eventTimeWatermarkLag,

      processingLatencyMs = averageProcessingLatencyMs,
      mlInferenceLatencyMs = averageInferenceLatencyMs,
      ingestionLagMs = ingestionLag,

      adaptiveWindowSizeMs = adaptiveWindowSizeMs,
      adaptiveWatermarkDelayMs = adaptiveWatermarkDelayMs,

      timestamp = now
    )
  }


  // ============================================================
  // LOGGING
  // ============================================================

  def logSnapshot(): Unit = {

    println(
      "[METRIC] " +
        s"event_rate=${eventRate.formatted("%.2f")} " +
        s"disorder_ratio=${disorderRatio.formatted("%.4f")} " +
        s"late_event_ratio=${lateEventRatio.formatted("%.4f")} " +
        s"avg_latency_ms=${averageLatencyMs.formatted("%.2f")} " +

        s"window_fill_ratio=${normalizedWindowFillRatio.formatted("%.4f")} " +
        s"avg_events_per_window=${averageEventsPerWindow.formatted("%.2f")} " +

        s"interaction_rate=${interactionRate.formatted("%.4f")} " +
        s"collision_rate=${collisionRate.formatted("%.4f")} " +
        s"proximity_rate=${proximityRate.formatted("%.4f")} " +
        s"swarm_rate=${swarmRate.formatted("%.4f")} " +
        s"conflict_rate=${conflictRate.formatted("%.4f")} " +

        s"watermark_lag_ms=$watermarkLagMs " +
        s"ingestion_lag_ms=$ingestionLagMs " +

        s"processing_latency_ms=${averageProcessingLatencyMs.formatted("%.2f")} " +
        s"ml_inference_ms=${averageInferenceLatencyMs.formatted("%.3f")} " +

        s"adaptive_window_ms=$adaptiveWindowSizeMs " +
        s"adaptive_watermark_ms=$adaptiveWatermarkDelayMs " +

        s"collision_count=$collisionCount " +
        s"proximity_count=$proximityCount " +
        s"swarm_count=$swarmCount " +
        s"conflict_count=$conflictCount " +

        s"total_events=$totalEvents " +
        s"total_interactions=$totalInteractions " +

        // Article 4 adaptive control metrics
        s"adaptation_count=$adaptationCount " +
        s"window_change_rate=${windowChangeRate.formatted("%.4f")} " +
        s"watermark_change_rate=${watermarkChangeRate.formatted("%.4f")} " +

        s"window_oscillation=$windowOscillation " +
        s"watermark_oscillation=$watermarkOscillation " +

        s"window_oscillation_rate=${windowOscillationRate.formatted("%.4f")} " +
        s"watermark_oscillation_rate=${watermarkOscillationRate.formatted("%.4f")} " +

        s"min_window_ms=$minWindowMs " +
        s"max_window_ms=$maxWindowMs " +

        s"min_watermark_ms=$minWatermarkMs " +
        s"max_watermark_ms=$maxWatermarkMs " +

        s"avg_adaptation_interval_ms=${avgAdaptationIntervalMs.formatted("%.2f")} "
    )
  }
}
