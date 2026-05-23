package phd.adaptivecontrol.producer.timestamp

import java.time.Instant
import scala.util.Random


sealed trait TimestampPattern {
  def nextTimestamp(currentProcessingTime: Long, rand: Random): Instant
}


case object RealTimeTimestamp extends TimestampPattern {

  override def nextTimestamp(currentProcessingTime: Long, rand: Random): Instant = {
    Instant.ofEpochMilli(currentProcessingTime)
  }
}


final case class SkewedTimestamp(maxSkewMs: Long) extends TimestampPattern {
  override def nextTimestamp(currentProcessingTime: Long, rand: Random): Instant = {

    val skew =
      ((rand.nextDouble() * 2.0) - 1.0) * maxSkewMs

    Instant.ofEpochMilli(currentProcessingTime + skew.toLong)
  }
}


final case class LateEventsTimestamp(maxDelayMs: Long) extends TimestampPattern {

  override def nextTimestamp(currentProcessingTime: Long, rand: Random): Instant = {

    val delay =
      rand.nextDouble() * maxDelayMs

    Instant.ofEpochMilli(currentProcessingTime - delay.toLong)
  }
}


final case class OutOfOrderTimestamp(
  disorderProbability: Double,
  maxDisorderMs: Long
) extends TimestampPattern {

  override def nextTimestamp(currentProcessingTime: Long, rand: Random): Instant = {

    val shouldDisorder =
      rand.nextDouble() < disorderProbability

    if (shouldDisorder) {

      val disorder =
        rand.nextDouble() * maxDisorderMs

      Instant.ofEpochMilli(currentProcessingTime - disorder.toLong)

    } else {

      Instant.ofEpochMilli(currentProcessingTime)
    }
  }
}


final case class MixedDisorderTimestamp(
  maxSkewMs: Long,
  maxDelayMs: Long,
  disorderProbability: Double
) extends TimestampPattern {

  override def nextTimestamp(currentProcessingTime: Long, rand: Random): Instant = {

    val skew =
      ((rand.nextDouble() * 2.0) - 1.0) * maxSkewMs

    val delayed =
      rand.nextDouble() < disorderProbability

    val delay =
      if (delayed)
        rand.nextDouble() * maxDelayMs
      else
        0.0

    Instant.ofEpochMilli(currentProcessingTime + skew.toLong - delay.toLong)
  }
}


object TimestampPattern {

  def fromEnv(): TimestampPattern = {

    sys.env
      .getOrElse("TIMESTAMP_PATTERN", "realtime")
      .toLowerCase match {

      case "realtime" =>
        RealTimeTimestamp

      case "skewed" =>
        SkewedTimestamp(
          maxSkewMs =
            sys.env
              .getOrElse("MAX_SKEW_MS", "2000")
              .toLong
        )

      case "late" =>
        LateEventsTimestamp(
          maxDelayMs =
            sys.env
              .getOrElse("MAX_DELAY_MS", "5000")
              .toLong
        )

      case "out_of_order" | "out-of-order" =>
        OutOfOrderTimestamp(
          disorderProbability =
            sys.env
              .getOrElse("DISORDER_PROBABILITY", "0.2")
              .toDouble,

          maxDisorderMs =
            sys.env
              .getOrElse("MAX_DISORDER_MS", "3000")
              .toLong
        )

      case "mixed" =>
        MixedDisorderTimestamp(
          maxSkewMs =
            sys.env
              .getOrElse("MAX_SKEW_MS", "2000")
              .toLong,

          maxDelayMs =
            sys.env
              .getOrElse("MAX_DELAY_MS", "5000")
              .toLong,

          disorderProbability =
            sys.env
              .getOrElse("DISORDER_PROBABILITY", "0.3")
              .toDouble
        )

      case other =>
        println(s"[TimestampPattern] Unknown '$other', using realtime")

        RealTimeTimestamp
    }
  }
}
