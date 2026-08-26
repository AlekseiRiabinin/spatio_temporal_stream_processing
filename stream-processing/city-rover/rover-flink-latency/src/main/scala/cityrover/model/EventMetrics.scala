package cityrover.model

import java.time.Instant


final case class EventMetrics(
  eventTime: Instant,
  ingestionTime: Instant,
  processingTime: Instant,
  currentWatermark: Long,
  ingestionLagMs: Long,
  processingLagMs: Long,
  watermarkLagMs: Long
)

object EventMetrics {

  /**
   * Compute latency metrics for a given GeoEvent.
   *
   * @param event            GeoEvent flowing through the pipeline
   * @param processingTime   timestamp when the operator processes the event
   * @param currentWatermark current watermark at the processing moment
   */
  def compute(
    event: GeoEvent,
    processingTime: Instant,
    currentWatermark: Long
  ): EventMetrics = {

    val eventEpochMs: Long      = event.eventTime.toEpochMilli
    val ingestionEpochMs: Long  = event.ingestionTime.toEpochMilli
    val processingEpochMs: Long = processingTime.toEpochMilli

    val calculatedIngestionLagMs: Long  = ingestionEpochMs - eventEpochMs
    val calculatedProcessingLagMs: Long = processingEpochMs - eventEpochMs
    val calculatedWatermarkLagMs: Long  = currentWatermark - eventEpochMs

    EventMetrics(
      eventTime = event.eventTime,
      ingestionTime = event.ingestionTime,
      processingTime = processingTime,
      currentWatermark = currentWatermark,

      ingestionLagMs = calculatedIngestionLagMs,
      processingLagMs = calculatedProcessingLagMs,
      watermarkLagMs = calculatedWatermarkLagMs
    )
  }
}
