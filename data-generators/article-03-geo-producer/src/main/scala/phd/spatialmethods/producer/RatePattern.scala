package phd.spatialmethods.producer

import scala.util.Random


sealed trait RatePattern {
  def nextIntervalMs(now: Long): Long
}


final case class ConstantRate(eventsPerSec: Int) extends RatePattern {
  private val interval = math.max(1.0, 1000.0 / eventsPerSec)
  override def nextIntervalMs(now: Long): Long = interval.toLong
}


final case class BurstThenPause(
  burstRate: Int,
  burstDurationMs: Long,
  pauseMs: Long
) extends RatePattern {

  private var phaseStart = System.currentTimeMillis()
  private var inBurst = true

  override def nextIntervalMs(now: Long): Long = {
    val elapsed = now - phaseStart
    if (inBurst && elapsed >= burstDurationMs) {
      inBurst = false
      phaseStart = now
    } else if (!inBurst && elapsed >= pauseMs) {
      inBurst = true
      phaseStart = now
    }

    if (inBurst) {
      val interval = math.max(1.0, 1000.0 / burstRate)
      interval.toLong
    } else {
      pauseMs
    }
  }
}


final case class WaveRate(
  minRate: Int,
  maxRate: Int,
  periodMs: Long
) extends RatePattern {

  private val rand = new Random()

  override def nextIntervalMs(now: Long): Long = {
    val phase = (now % periodMs).toDouble / periodMs
    val rate = minRate + ((maxRate - minRate) * math.sin(2 * math.Pi * phase).abs).toInt
    val effectiveRate = math.max(1, rate)
    val jitter = rand.nextDouble() * 0.2 + 0.9
    (1000.0 / effectiveRate * jitter).toLong
  }
}


object RatePattern {

  def fromEnv(): RatePattern = {
    sys.env.getOrElse("EVENT_RATE_PATTERN", "constant").toLowerCase match {
      case "constant" =>
        ConstantRate(sys.env.getOrElse("EVENT_RATE", "50").toInt.max(1))

      case "burst" | "burst-then-pause" =>
        BurstThenPause(
          burstRate = sys.env.getOrElse("BURST_RATE", "100").toInt.max(1),
          burstDurationMs = sys.env.getOrElse("BURST_DURATION_MS", "10000").toLong,
          pauseMs = sys.env.getOrElse("PAUSE_MS", "5000").toLong
        )

      case "wave" =>
        WaveRate(
          minRate = sys.env.getOrElse("WAVE_MIN", "5").toInt.max(1),
          maxRate = sys.env.getOrElse("WAVE_MAX", "100").toInt.max(2),
          periodMs = sys.env.getOrElse("WAVE_PERIOD_MS", "10000").toLong
        )

      case other =>
        println(s"[RatePattern] Unknown '$other', using constant")
        ConstantRate(sys.env.getOrElse("EVENT_RATE", "50").toInt.max(1))
    }
  }
}
