package phd.spatialmethods.producer

import java.time.Instant
import scala.util.Random


sealed trait TimestampPattern {
  def nextInstant(now: Long, rand: Random): Instant
}


case object RealTimeTimestamp extends TimestampPattern {
  override def nextInstant(now: Long, rand: Random): Instant =
    Instant.ofEpochMilli(now)
}


final case class SkewedTimestamp(maxSkewMs: Long) extends TimestampPattern {
  override def nextInstant(now: Long, rand: Random): Instant = {
    val skew = (rand.nextDouble() * 2 - 1) * maxSkewMs
    Instant.ofEpochMilli(now + skew.toLong)
  }
}


final case class LateEventsTimestamp(maxDelayMs: Long) extends TimestampPattern {
  override def nextInstant(now: Long, rand: Random): Instant = {
    val delay = rand.nextDouble() * maxDelayMs
    Instant.ofEpochMilli(now - delay.toLong)
  }
}


object TimestampPattern {

  def fromEnv(): TimestampPattern = {
    sys.env.getOrElse("TIMESTAMP_PATTERN", "realtime").toLowerCase match {
      case "realtime" => RealTimeTimestamp
      case "skewed"   => SkewedTimestamp(sys.env.getOrElse("MAX_SKEW_MS", "2000").toLong)
      case "late"     => LateEventsTimestamp(sys.env.getOrElse("MAX_DELAY_MS", "5000").toLong)
      case other =>
        println(s"[TimestampPattern] Unknown '$other', using realtime")
        RealTimeTimestamp
    }
  }
}
