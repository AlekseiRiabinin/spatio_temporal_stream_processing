package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.config.StrategyMode


object AdaptiveRuntimeState {

  // ============================================================
  // Runtime mode
  // ============================================================

  sealed trait Mode
  case object Fixed extends Mode
  case object Adaptive extends Mode

  @volatile
  private var mode: Mode = Fixed

  def fromStrategyMode(m: StrategyMode): Mode = m match {
    case StrategyMode.Fixed    => Fixed
    case StrategyMode.Adaptive => Adaptive
  }

  def setMode(m: Mode): Unit = {
    mode = m

    println(
      s"[ADAPTIVE RUNTIME] action=mode_set mode=$mode"
    )
  }

  def setModeFromStrategy(m: StrategyMode): Unit = {
    setMode(fromStrategyMode(m))
  }

  def isAdaptive: Boolean =
    mode == Adaptive

  def isFixed: Boolean =
    mode == Fixed

  // ============================================================
  // Runtime state
  // ============================================================

  @volatile
  private var currentWindowSizeMs: Long = 5000L

  @volatile
  private var currentWatermarkDelayMs: Long = 3000L

  // ============================================================
  // Initialization
  // ============================================================

  def initialize(
    windowSizeMs: Long,
    watermarkDelayMs: Long
  ): Unit = synchronized {

    currentWindowSizeMs =
      math.max(1000L, windowSizeMs)

    currentWatermarkDelayMs =
      math.max(0L, watermarkDelayMs)

    println(
      "[ADAPTIVE RUNTIME] action=initialize " +
        s"windowMs=$currentWindowSizeMs " +
        s"watermarkMs=$currentWatermarkDelayMs"
    )
  }

  // ============================================================
  // Updates
  // ============================================================

  def updateWindowSize(windowSizeMs: Long): Unit = {
    if (!isAdaptive) return

    currentWindowSizeMs =
      math.max(1000L, windowSizeMs)
  }

  def updateWatermarkDelay(watermarkDelayMs: Long): Unit = {
    if (!isAdaptive) return

    currentWatermarkDelayMs =
      math.max(0L, watermarkDelayMs)
  }

  def update(
    windowSizeMs: Long,
    watermarkDelayMs: Long
  ): Unit = {

    if (!isAdaptive) return

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

  def currentMode: Mode =
    mode
}
