package cityrover.pipeline

import cityrover.util.ConfigLoader


/**
  * Lightweight latency profiler for operator‑chain measurements.
  *
  * The profiler samples every Nth event (configurable) and returns
  * an optional processingStartNs timestamp. Downstream operators compute:
  *
  *   latency = System.nanoTime() - processingStartNs
  *
  * TelemetryEvent is immutable; latency metadata is therefore external.
  */
object LatencyProfiler {

  private val enabled: Boolean =
    ConfigLoader.latencyProfilerEnabled

  private val sampleRate: Int =
    ConfigLoader.latencyProfilerSampleRate

  /**
    * Compute processingStartNs if sampling is enabled.
    *
    * @param index Position in stream (monotonic counter)
    * @return Some(timestamp) if sampled, otherwise None
    */
  def annotate(index: Long): Option[Long] = {
    if (!enabled) return None

    if (index % sampleRate == 0)
      Some(System.nanoTime())
    else
      None
  }
}
