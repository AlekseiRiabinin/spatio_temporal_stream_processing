package phd.adaptivecontrol.adaptive


object AdaptiveRuntimeState {

  // ============================================================
  // Runtime mode
  // ============================================================

  sealed trait Mode
  case object Fixed extends Mode
  case object Adaptive extends Mode

  @volatile
  private var mode: Mode = Fixed

  def setMode(m: Mode): Unit = {
    mode = m

    println(
      s"[ADAPTIVE RUNTIME] action=mode_set mode=$mode"
    )
  }

  def isAdaptive: Boolean =
    mode == Adaptive

  def isFixed: Boolean =
    mode == Fixed


  // ============================================================
  // Adaptive state
  // ============================================================

  @volatile
  private var currentWindowSizeMs: Long = _
  
  @volatile
  private var currentWatermarkDelayMs: Long = _


  // ============================================================
  // Initialization
  // ============================================================

  def initialize(
    windowSizeMs: Long,
    watermarkDelayMs: Long
  ): Unit = synchronized {

    if (!isAdaptive) return

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

  def update(windowSizeMs: Long, watermarkDelayMs: Long): Unit = {
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

  def windowSizeMs: Long = {
    if (isAdaptive) currentWindowSizeMs
    else throw new IllegalStateException(
      "windowSizeMs accessed in FIXED mode - use config.windowSizeMs instead"
    )
  }

  def watermarkDelayMs: Long = {
    if (isAdaptive) currentWatermarkDelayMs
    else throw new IllegalStateException(
      "watermarkDelayMs accessed in FIXED mode - use config.watermarkDelayMs instead"
    )
  }
}
