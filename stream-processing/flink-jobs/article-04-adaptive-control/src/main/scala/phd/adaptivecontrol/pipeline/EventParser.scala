package phd.adaptivecontrol.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import phd.adaptivecontrol.model.GeoEvent


/**
  * EventParser
  *
  * Pipeline-level JSON parser for:
  *   - Kafka ingestion
  *   - stream deserialization
  *   - debug serialization
  */
object EventParser {

  // ============================================================
  // Jackson Mapper
  // ============================================================
  private val mapper = new ObjectMapper()

  mapper.registerModule(DefaultScalaModule)

  // ============================================================
  // JSON -> GeoEvent
  // ============================================================
  def parseGeoEvent(json: String): Option[GeoEvent] = {

    try {

      val event =
        mapper.readValue(json, classOf[GeoEvent])

      if (event.isValid) Some(event)
      else None

    } catch {

      case ex: Exception =>

        println(
          s"[EventParser] parse failure: ${ex.getMessage}"
        )

        None
    }
  }

  // ============================================================
  // GeoEvent -> JSON
  // ============================================================
  def toJson(event: GeoEvent): String = {
    mapper.writeValueAsString(event)
  }
}
