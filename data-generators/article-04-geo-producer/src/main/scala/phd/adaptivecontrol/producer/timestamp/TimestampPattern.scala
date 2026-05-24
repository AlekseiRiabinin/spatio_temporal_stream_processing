package phd.adaptivecontrol.producer.timestamp

import java.time.Instant
import scala.util.Random


/**
  * BASE event-time semantic model.
  *
  * IMPORTANT:
  * This layer defines ONLY the semantic mapping
  * between processing time and event time.
  *
  * It does NOT simulate:
  *   - skew
  *   - disorder
  *   - jitter
  *   - delay
  *
  * Those effects are handled later by:
  *   - TimestampSkewInjector
  *   - DisorderSimulator
  */
sealed trait TimestampPattern {

  def nextTimestamp(
    currentProcessingTime: Long,
    rand: Random
  ): Instant
}


/**
  * Baseline real-time event-time model.
  *
  * Event time is strictly aligned with processing time.
  */
case object RealTimeTimestamp extends TimestampPattern {

  override def nextTimestamp(
    currentProcessingTime: Long,
    rand: Random
  ): Instant = {

    Instant.ofEpochMilli(currentProcessingTime)
  }
}


/**
  * Factory for selecting timestamp semantic model.
  */
object TimestampPattern {

  def fromEnv(): TimestampPattern = {

    sys.env
      .getOrElse("TIMESTAMP_PATTERN", "realtime")
      .toLowerCase match {

      // ------------------------------------------------------
      // Baseline model (ONLY supported mode)
      // ------------------------------------------------------
      case "realtime" =>
        RealTimeTimestamp

      // ------------------------------------------------------
      // Fallback safety
      // ------------------------------------------------------
      case other =>
        println(
          s"[TimestampPattern] Unknown pattern '$other', using realtime"
        )
        RealTimeTimestamp
    }
  }
}
