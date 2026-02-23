package phd.streammodels.util


/**
 * Time-related utility functions.
 *
 * All timestamps are normalized to milliseconds since epoch.
 */
object TimeUtils {

  /**
   * Normalize timestamp to milliseconds.
   *
   * Allows future extensions (seconds, nanos, etc.).
   */
  def toMillis(ts: Long): Long = ts

  /**
   * Current processing time.
   *
   * Corresponds to t (system time) in the paper.
   */
  def processingTime(): Long =
    System.currentTimeMillis()

}
