package cityrover.pipeline

import cityrover.model.GeoEvent
import cityrover.util.ConfigLoader


/**
  * Lightweight latency profiler for operator‑chain measurements.
  *
  * The profiler samples every Nth event (configurable) and attaches
  * a processing timestamp. Downstream operators can compute:
  *
  *   latency = System.nanoTime() - event.processingStartNs
  *
  * This avoids allocations and keeps GC pressure minimal.
  */
object LatencyProfiler {

  private val enabled: Boolean =
    ConfigLoader.latencyProfilerEnabled

  private val sampleRate: Int =
    ConfigLoader.latencyProfilerSampleRate

  /**
    * Annotate event with processing timestamp if sampling is enabled.
    *
    * @param event Incoming GeoEvent
    * @param index Position in stream (monotonic counter)
    * @return GeoEvent with optional processingStartNs set
    */
  def annotate(event: GeoEvent, index: Long): GeoEvent = {
    if (!enabled) return event

    // Sample every Nth event
    if (index % sampleRate == 0) {
      event.copy(processingStartNs = Some(System.nanoTime()))
    } else {
      event
    }
  }
}
