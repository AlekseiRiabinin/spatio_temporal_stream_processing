package phd.adaptivecontrol.producer.timestamp

import scala.util.Random
import phd.adaptivecontrol.producer.model.GeoEvent


object DisorderSimulator {

  // ==========================================================
  // Out-of-order / delayed delivery
  // ==========================================================
  def injectDisorder(
    event: GeoEvent,
    rand: Random,
    disorderProbability: Double,
    maxAdditionalDelayMs: Long
  ): GeoEvent = {

    val shouldDisorder =
      rand.nextDouble() < disorderProbability

    if (!shouldDisorder) {

      event

    } else {

      val additionalDelay =
        (rand.nextDouble() * maxAdditionalDelayMs).toLong

      event.copy(
        timestamp = event.timestamp - additionalDelay,
        attributes = event.attributes ++ Map(
          "disordered" -> "true",
          "disorderDelayMs" -> additionalDelay.toString
        )
      )
    }
  }


  // ==========================================================
  // Burst latency spikes
  // ==========================================================
  def injectBurstDelay(
    event: GeoEvent,
    rand: Random,
    burstProbability: Double,
    burstDelayMs: Long
  ): GeoEvent = {

    val shouldDelay =
      rand.nextDouble() < burstProbability

    if (!shouldDelay) {

      event

    } else {

      event.copy(
        timestamp = event.timestamp - burstDelayMs,
        attributes = event.attributes ++ Map(
          "burstDelayApplied" -> "true",
          "burstDelayMs" -> burstDelayMs.toString
        )
      )
    }
  }


  // ==========================================================
  // Timestamp jitter
  // ==========================================================
  def injectJitter(
    event: GeoEvent,
    rand: Random,
    maxJitterMs: Long
  ): GeoEvent = {

    if (maxJitterMs <= 0L) {
      return event
    }

    val jitter =
      (((rand.nextDouble() * 2.0) - 1.0) * maxJitterMs).toLong

    event.copy(
      timestamp = event.timestamp + jitter,
      attributes = event.attributes ++ Map(
        "jitterApplied" -> "true",
        "jitterMs" -> jitter.toString
      )
    )
  }


  // ==========================================================
  // GPS / telemetry dropout
  // ==========================================================
  def shouldDropEvent(
    rand: Random,
    dropoutProbability: Double
  ): Boolean = {

    rand.nextDouble() < dropoutProbability
  }


  // ==========================================================
  // Composite temporal disorder pipeline
  // ==========================================================
  def applyCompositeDisorder(
    event: GeoEvent,
    rand: Random
  ): Option[GeoEvent] = {

    // --------------------------------------------------------
    // GPS dropout simulation
    // --------------------------------------------------------
    val dropoutProbability =
      sys.env
        .getOrElse("GPS_DROPOUT_PROBABILITY", "0.0")
        .toDouble

    if (shouldDropEvent(rand, dropoutProbability)) {

      None

    } else {

      // ------------------------------------------------------
      // Disorder configuration
      // ------------------------------------------------------
      val disorderProbability =
        sys.env
          .getOrElse("DISORDER_PROBABILITY", "0.2")
          .toDouble

      val maxDisorderMs =
        sys.env
          .getOrElse("MAX_DISORDER_MS", "3000")
          .toLong

      val burstProbability =
        sys.env
          .getOrElse("BURST_DELAY_PROBABILITY", "0.05")
          .toDouble

      val burstDelayMs =
        sys.env
          .getOrElse("BURST_DELAY_MS", "10000")
          .toLong

      val maxJitterMs =
        sys.env
          .getOrElse("MAX_JITTER_MS", "500")
          .toLong

      // ------------------------------------------------------
      // Composite pipeline
      // ------------------------------------------------------
      val disordered =
        injectDisorder(
          event,
          rand,
          disorderProbability,
          maxDisorderMs
        )

      val burstDelayed =
        injectBurstDelay(
          disordered,
          rand,
          burstProbability,
          burstDelayMs
        )

      val jittered =
        injectJitter(
          burstDelayed,
          rand,
          maxJitterMs
        )

      // ------------------------------------------------------
      // Final annotation
      // ------------------------------------------------------
      Some(
        jittered.copy(
          attributes = jittered.attributes ++ Map(
            "disorderPipeline" -> "enabled"
          )
        )
      )
    }
  }
}
