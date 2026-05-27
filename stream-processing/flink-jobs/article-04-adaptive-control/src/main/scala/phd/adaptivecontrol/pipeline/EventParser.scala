package phd.adaptivecontrol.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import phd.adaptivecontrol.model.GeoEvent


/**
 * EventParser
 *
 * Centralized parsing and validation layer for incoming
 * spatio-temporal stream events.
 *
 * Responsibilities:
 *   - JSON deserialization
 *   - schema validation
 *   - malformed event filtering
 *   - ingestion diagnostics
 *
 * Supported input format:
 * {
 *   "id": "...",
 *   "objectId": "...",
 *   "timestamp": 1710000000000,
 *   "lon": 37.62,
 *   "lat": 55.75,
 *   "wkt": "POINT(...)",
 *   "speed": 12.5,
 *   "heading": 180.0,
 *   "attributes": { ... }
 * }
 */
object EventParser {

  // ------------------------------------------------------------
  // Shared Jackson mapper
  // ------------------------------------------------------------
  private val mapper = new ObjectMapper()

  mapper.registerModule(DefaultScalaModule)

  // ------------------------------------------------------------
  // Parse JSON → GeoEvent
  // ------------------------------------------------------------
  def parse(json: String): Option[GeoEvent] = {

    try {

      val event =
        mapper.readValue(json, classOf[GeoEvent])

      if (event.isValid) {

        println(
          s"[EventParser] action=parsed " +
          s"objectId=${event.objectId} " +
          s"timestamp=${event.timestamp}"
        )

        Some(event)

      } else {

        println(
          s"[EventParser] action=invalidEvent " +
          s"payload=$json"
        )

        None
      }

    } catch {

      case ex: Exception =>

        println(
          s"[EventParser] action=parseError " +
          s"message=${ex.getMessage} " +
          s"payload=$json"
        )

        None
    }
  }
}
