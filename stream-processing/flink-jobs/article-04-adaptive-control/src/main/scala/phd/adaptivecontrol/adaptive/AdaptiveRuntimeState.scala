package phd.adaptivecontrol.adaptive


/**
 * AdaptiveRuntimeState
 *
 * Shared runtime state for adaptive control.
 *
 * Unlike AdaptiveConfig, this object is intended
 * to hold continuously changing parameters that
 * are updated by AdaptiveController and consumed
 * by runtime operators.
 */
object AdaptiveRuntimeState {

  // ============================================================
  // Adaptive window
  // ============================================================

  @volatile
  private var currentWindowSizeMs: Long = 5000L

  // ============================================================
  // Adaptive watermark
  // ============================================================

  @volatile
  private var currentWatermarkDelayMs: Long = 3000L

  // ============================================================
  // Initialization
  // ============================================================

  def initialize(
    windowSizeMs: Long,
    watermarkDelayMs: Long
  ): Unit = synchronized {

    currentWindowSizeMs = math.max(1000L, windowSizeMs)
    currentWatermarkDelayMs = math.max(0L, watermarkDelayMs)

    println(
      "[ADAPTIVE RUNTIME] action=initialize " +
      s"windowMs=$currentWindowSizeMs " +
      s"watermarkMs=$currentWatermarkDelayMs"
    )
  }

  // ============================================================
  // Updates
  // ============================================================

  def updateWindowSize(windowSizeMs: Long): Unit =
    currentWindowSizeMs = math.max(1000L, windowSizeMs)

  def updateWatermarkDelay(watermarkDelayMs: Long): Unit =
    currentWatermarkDelayMs = math.max(0L, watermarkDelayMs)

  def update(windowSizeMs: Long, watermarkDelayMs: Long): Unit = {

    updateWindowSize(windowSizeMs)
    updateWatermarkDelay(watermarkDelayMs)

    println(
      "[ADAPTIVE RUNTIME] action=update " +
      s"windowMs=$currentWindowSizeMs " +
      s"watermarkMs=$currentWatermarkDelayMs"
    )
  }

  // ============================================================
  // Accessors
  // ============================================================

  def windowSizeMs: Long =
    currentWindowSizeMs

  def watermarkDelayMs: Long =
    currentWatermarkDelayMs
}
