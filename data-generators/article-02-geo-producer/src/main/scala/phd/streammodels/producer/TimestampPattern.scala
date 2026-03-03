package phd.streammodels.producer

import scala.util.Random


sealed trait TimestampPattern {
  def nextTimestamp(now: Long, rand: Random): Long
}

final case object RealTimeTimestamp extends TimestampPattern {
  override def nextTimestamp(now: Long, rand: Random): Long = now
}

final case class SkewedTimestamp(maxSkewMs: Long) extends TimestampPattern {
  override def nextTimestamp(now: Long, rand: Random): Long = {
    val skew = (rand.nextDouble() * 2 - 1) * maxSkewMs
    now + skew.toLong
  }
}

final case class LateEventsTimestamp(maxDelayMs: Long) extends TimestampPattern {
  override def nextTimestamp(now: Long, rand: Random): Long = {
    val delay = rand.nextDouble() * maxDelayMs
    now - delay.toLong
  }
}

object TimestampPattern {

  def fromEnv(): TimestampPattern = {
    val pattern = sys.env.getOrElse("TIMESTAMP_PATTERN", "realtime").toLowerCase

    pattern match {
      case "realtime" =>
        RealTimeTimestamp

      case "skewed" =>
        val maxSkew = sys.env.getOrElse("MAX_SKEW_MS", "2000").toLong
        SkewedTimestamp(maxSkew)

      case "late" | "late-events" =>
        val maxDelay = sys.env.getOrElse("MAX_DELAY_MS", "5000").toLong
        LateEventsTimestamp(maxDelay)

      case other =>
        println(s"[TimestampPattern] Unknown pattern '$other', falling back to realtime")
        RealTimeTimestamp
    }
  }
}
