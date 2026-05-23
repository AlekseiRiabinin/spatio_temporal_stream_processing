package phd.adaptivecontrol.producer.timestamp

import scala.util.Random
import phd.adaptivecontrol.producer.model.GeoEvent


object TimestampSkewInjector {

  def injectPositiveSkew(event: GeoEvent, rand: Random, maxSkewMs: Long): GeoEvent = {

    val skew =
      (rand.nextDouble() * maxSkewMs).toLong

    event.copy(
      timestamp = event.timestamp + skew,
      attributes = event.attributes ++ Map(
        "positiveSkewMs" -> skew.toString
      )
    )
  }


  def injectNegativeSkew(event: GeoEvent, rand: Random, maxSkewMs: Long): GeoEvent = {

    val skew =
      (rand.nextDouble() * maxSkewMs).toLong

    event.copy(
      timestamp = event.timestamp - skew,
      attributes = event.attributes ++ Map(
        "negativeSkewMs" -> skew.toString
      )
    )
  }


  def injectBidirectionalSkew(event: GeoEvent, rand: Random, maxSkewMs: Long): GeoEvent = {

    val skew =
      (((rand.nextDouble() * 2.0) - 1.0) * maxSkewMs).toLong

    event.copy(
      timestamp = event.timestamp + skew,
      attributes = event.attributes ++ Map(
        "bidirectionalSkewMs" -> skew.toString
      )
    )
  }


  def applySkew(event: GeoEvent, rand: Random): GeoEvent = {

    val enabled =
      sys.env
        .getOrElse("ENABLE_TIMESTAMP_SKEW", "true")
        .toBoolean

    if (!enabled) {
      return event
    }

    val skewMode =
      sys.env
        .getOrElse("TIMESTAMP_SKEW_MODE", "bidirectional")
        .toLowerCase

    val skewProbability =
      sys.env
        .getOrElse("SKEW_PROBABILITY", "0.2")
        .toDouble

    val maxSkewMs =
      sys.env
        .getOrElse("MAX_SKEW_MS", "2000")
        .toLong

    val shouldApply =
      rand.nextDouble() < skewProbability

    if (!shouldApply) {
      event
    } else {

      val skewedEvent =
        skewMode match {

          case "positive" =>
            injectPositiveSkew(event, rand, maxSkewMs)

          case "negative" =>
            injectNegativeSkew(event, rand, maxSkewMs)

          case "bidirectional" =>
            injectBidirectionalSkew(event, rand, maxSkewMs)

          case other =>

            println(
              s"[TimestampSkewInjector] Unknown skew mode '$other', using bidirectional"
            )

            injectBidirectionalSkew(event, rand, maxSkewMs)
        }

      skewedEvent.copy(
        attributes = skewedEvent.attributes ++ Map(
          "timestampSkewApplied" -> "true",
          "timestampSkewMode" -> skewMode
        )
      )
    }
  }
}
