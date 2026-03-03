package phd.streammodels.producer

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
      // during pause, no events → sleep long
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
    val jitter = rand.nextDouble() * 0.2 + 0.9 // 0.9–1.1
    (1000.0 / effectiveRate * jitter).toLong
  }
}

object RatePattern {

  def fromEnv(): RatePattern = {
    val pattern = sys.env.getOrElse("EVENT_RATE_PATTERN", "constant").toLowerCase

    pattern match {
      case "constant" =>
        val rate = sys.env.getOrElse("EVENT_RATE", "50").toInt.max(1)
        ConstantRate(rate)

      case "burst" | "burst-then-pause" =>
        val burstRate = sys.env.getOrElse("BURST_RATE", "100").toInt.max(1)
        val burstDurationMs = sys.env.getOrElse("BURST_DURATION_MS", "10000").toLong
        val pauseMs = sys.env.getOrElse("PAUSE_MS", "5000").toLong
        BurstThenPause(burstRate, burstDurationMs, pauseMs)

      case "wave" =>
        val minRate = sys.env.getOrElse("WAVE_MIN", "5").toInt.max(1)
        val maxRate = sys.env.getOrElse("WAVE_MAX", "100").toInt.max(minRate + 1)
        val periodMs = sys.env.getOrElse("WAVE_PERIOD_MS", "10000").toLong
        WaveRate(minRate, maxRate, periodMs)

      case other =>
        println(s"[RatePattern] Unknown pattern '$other', falling back to constant")
        val rate = sys.env.getOrElse("EVENT_RATE", "50").toInt.max(1)
        ConstantRate(rate)
    }
  }
}
