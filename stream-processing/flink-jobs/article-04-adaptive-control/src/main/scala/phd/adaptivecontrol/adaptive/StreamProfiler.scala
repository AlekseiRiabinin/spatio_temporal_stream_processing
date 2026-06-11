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

  private var lastEventTimestamp: Long = Long.MinValue
  private var accumulatedLatencyMs: Double = 0.0

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
  // Controller metrics
  // ============================================================

  private var previousWindowSizeMs: Long = 0L
  private var previousWatermarkDelayMs: Long = 0L

  private var windowAdjustments: Long = 0L
  private var watermarkAdjustments: Long = 0L

  private var cumulativeWindowDelta: Double = 0.0
  private var cumulativeWatermarkDelta: Double = 0.0

  // ============================================================
  // Processing metrics
  // ============================================================

  private var processingOperations: Long = 0L
  private var accumulatedProcessingLatencyMs: Double = 0.0

  private var inferenceCalls: Long = 0L
  private var accumulatedInferenceLatencyMs: Double = 0.0

  // ============================================================
  // Configuration
  // ============================================================

  def setConfig(cfg: AdaptiveConfig): Unit = {
    config = Some(cfg)

    adaptiveWindowSizeMs = cfg.adaptiveWindowSizeMs
    adaptiveWatermarkDelayMs = cfg.adaptiveWatermarkDelayMs

    previousWindowSizeMs = adaptiveWindowSizeMs
    previousWatermarkDelayMs = adaptiveWatermarkDelayMs
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

    totalEvents += 1

    recentEventTimes.enqueue(now)

    while (
      recentEventTimes.nonEmpty &&
      now - recentEventTimes.front > EventRateWindowMs
    ) {
      recentEventTimes.dequeue()
    }

    if (event.timestamp < lastEventTimestamp)
      outOfOrderEvents += 1

    lastEventTimestamp =
      math.max(lastEventTimestamp, event.timestamp)

    val latency = now - event.timestamp

    if (latency > 0) {

      accumulatedLatencyMs += latency

      latencyEMA =
        if (latencyEMA == 0.0)
          latency.toDouble
        else
          alpha * latency + (1.0 - alpha) * latencyEMA
    }

    val effectiveWatermark =
      if (currentWatermark != Long.MinValue)
        currentWatermark
      else
        (now - latencyEMA * 1.5).toLong

    if (event.timestamp < effectiveWatermark)
      lateEvents += 1
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
        case Conflict  =>  conflictCount += 1
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

  def updateAdaptiveParameters(
      windowSizeMs: Long,
      watermarkDelayMs: Long
  ): Unit = {

    if (windowSizeMs != previousWindowSizeMs) {

      windowAdjustments += 1

      cumulativeWindowDelta +=
        math.abs(windowSizeMs - previousWindowSizeMs)

      previousWindowSizeMs = windowSizeMs
    }

    if (watermarkDelayMs != previousWatermarkDelayMs) {

      watermarkAdjustments += 1

      cumulativeWatermarkDelta +=
        math.abs(watermarkDelayMs - previousWatermarkDelayMs)

      previousWatermarkDelayMs = watermarkDelayMs
    }

    adaptiveWindowSizeMs = windowSizeMs
    adaptiveWatermarkDelayMs = watermarkDelayMs
  }

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

    lastPredictedWindowMs =
      decision.windowSizeMs

    lastPredictedWatermarkMs =
      decision.watermarkDelayMs

    adaptiveWindowSizeMs =
      decision.windowSizeMs

    adaptiveWatermarkDelayMs =
      decision.watermarkDelayMs

    lastInferenceLatencyMs =
      inferenceLatencyMs

    totalInferenceLatencyMs +=
      inferenceLatencyMs

    val now = System.currentTimeMillis()

    // adaptation interval
    if (lastAdaptationTs != 0L) {
      adaptationIntervalSumMs += (now - lastAdaptationTs)
    }
    lastAdaptationTs = now

    // window bounds
    minWindowMs = math.min(minWindowMs, decision.windowSizeMs)
    maxWindowMs = math.max(maxWindowMs, decision.windowSizeMs)

    // watermark bounds
    minWatermarkMs = math.min(minWatermarkMs, decision.watermarkDelayMs)
    maxWatermarkMs = math.max(maxWatermarkMs, decision.watermarkDelayMs)

    // oscillation detection (direction flips only)
    val windowDir = 
      math.signum(decision.windowSizeMs - lastPredictedWindowMs).toInt
    if (
      windowDir != 0 && 
      lastWindowDirection != 0 && 
      windowDir != lastWindowDirection
    )
      windowOscillation += 1
    lastWindowDirection = windowDir

    val watermarkDir = 
      math.signum(decision.watermarkDelayMs - lastPredictedWatermarkMs).toInt
    if (
      watermarkDir != 0 && 
      lastWatermarkDirection != 0 && 
      watermarkDir != lastWatermarkDirection
    )
      watermarkOscillation += 1
    lastWatermarkDirection = watermarkDir
  }


  // ============================================================
  // Processing latency
  // ============================================================

  def recordProcessingLatency(latencyMs: Double): Unit = {
    processingOperations += 1
    accumulatedProcessingLatencyMs += latencyMs
  }

  // ============================================================
  // ONNX latency
  // ============================================================

  def recordInferenceLatency(latencyMs: Double): Unit = {
    inferenceCalls += 1
    accumulatedInferenceLatencyMs += latencyMs
  }

  // ============================================================
  // ML features
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

  def minWindow: Long =
    if (minWindowMs == Long.MaxValue) 0L 
    else minWindowMs
  
  def maxWindow: Long =
    if (maxWindowMs == Long.MinValue) 0L
    else maxWindowMs

  def minWatermark: Long =
    if (minWatermarkMs == Long.MaxValue) 0L
    else minWatermarkMs

  def maxWatermark: Long =
    if (maxWatermarkMs == Long.MinValue) 0L
    else maxWatermarkMs

  def avgAdaptationIntervalMs: Double =
    if (adaptationCount <= 1) 0.0
    else adaptationIntervalSumMs / (adaptationCount - 1)


  // ============================================================
  // Snapshot for ML
  // ============================================================

  def snapshot(): StreamFeatures = {

    val now = System.currentTimeMillis()

    val safeInteractions =
      math.max(totalInteractions, 1L)

    StreamFeatures(
      eventRate = eventRate,
      disorderRatio = disorderRatio,
      lateEventRatio = lateEventRatio,
      averageLatencyMs = averageLatencyMs,
      windowFillRatio = averageWindowFill,
      interactionRate = interactionRate,
      collisionRate = collisionCount.toDouble / safeInteractions,
      proximityRate = proximityCount.toDouble / safeInteractions,
      swarmRate = swarmCount.toDouble / safeInteractions,
      conflictRate = conflictCount.toDouble / safeInteractions,
      watermarkLagMs =
        if (currentWatermark == Long.MinValue) 0L
        else now - currentWatermark,
      processingLatencyMs =
        if (processingOperations == 0)
          (now - lastEventTimestamp)
        else
          averageProcessingLatencyMs.toLong,
      adaptiveWindowSizeMs = adaptiveWindowSizeMs,
      adaptiveWatermarkDelayMs = adaptiveWatermarkDelayMs,
      timestamp = now
    )
  }


  // ============================================================
  // LOGGING
  // ============================================================

  def logSnapshot(): Unit = {

    val now = System.currentTimeMillis()

    println(
      "[METRIC] " +
        s"event_rate=${eventRate.formatted("%.2f")} " +
        s"disorder_ratio=${disorderRatio.formatted("%.4f")} " +
        s"late_event_ratio=${lateEventRatio.formatted("%.4f")} " +
        s"avg_latency_ms=${averageLatencyMs.formatted("%.2f")} " +
        s"window_fill_ratio=${averageWindowFill.formatted("%.2f")} " +
        s"interaction_rate=${interactionRate.formatted("%.4f")} " +
        s"collision_rate=${
          if (totalInteractions == 0) 0.0
          else collisionCount.toDouble / totalInteractions
        } " +
        s"proximity_rate=${
          if (totalInteractions == 0) 0.0
          else proximityCount.toDouble / totalInteractions
        } " +
        s"swarm_rate=${
          if (totalInteractions == 0) 0.0
          else swarmCount.toDouble / totalInteractions
        } " +
        s"conflict_rate=${
          if (totalInteractions == 0) 0.0
          else conflictCount.toDouble / totalInteractions
        } " +
        s"watermark_lag_ms=${
          if (currentWatermark == Long.MinValue) 0L
          else now - currentWatermark
        } " +
        s"processing_latency_ms=${now - lastEventTimestamp} " +
        s"adaptive_window_ms=$adaptiveWindowSizeMs " +
        s"adaptive_watermark_ms=$adaptiveWatermarkDelayMs " +
        s"collision_count=$collisionCount " +
        s"proximity_count=$proximityCount " +
        s"swarm_count=$swarmCount " +
        s"conflict_count=$conflictCount " +
        s"total_events=$totalEvents " +
        s"total_interactions=$totalInteractions " +

        // Article 4 metrics
        s"adaptation_count=$adaptationCount " +
        s"ml_inference_ms=${lastInferenceLatencyMs.formatted("%.3f")} " +
        s"avg_ml_inference_ms=${averageInferenceLatencyMs.formatted("%.3f")} " +
        s"window_change_rate=${windowChangeRate.formatted("%.4f")} " +
        s"watermark_change_rate=${watermarkChangeRate.formatted("%.4f")} " +
        s"window_oscillation=${windowOscillation} " +
        s"watermark_oscillation=${watermarkOscillation} " +
        s"window_oscillation_rate=${windowOscillationRate.formatted("%.4f")} " +
        s"watermark_oscillation_rate=${watermarkOscillationRate.formatted("%.4f")} " +
        s"min_window_ms=${minWindow} " +
        s"max_window_ms=${maxWindow} " +
        s"min_watermark_ms=${minWatermark} " +
        s"max_watermark_ms=${maxWatermark} " +
        s"avg_adaptation_interval_ms=${avgAdaptationIntervalMs.formatted("%.2f")} "
    )
  }
}
