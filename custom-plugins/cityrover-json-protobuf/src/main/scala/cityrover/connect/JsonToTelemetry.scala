package cityrover.connect

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import scala.util.Try


object JsonToTelemetry {

  private val mapper = new ObjectMapper()

  def parse(json: String): Either[String, TelemetryData] = {

    Try(mapper.readTree(json)).toEither.left
      .map(error => s"Invalid JSON: ${error.getMessage}")
      .flatMap(parseNode)
  }

  private def parseNode(
    node: JsonNode
  ): Either[String, TelemetryData] = {

    if (node == null || !node.isObject) {
      return Left("Telemetry JSON must be a JSON object")
    }

    for {
      roverId <- requiredString(node, "roverId")
      ts      <- requiredLong(node, "ts")
    } yield {

      TelemetryData(
        roverId = roverId,
        ts = ts,
        lat = optionalDouble(node, "lat"),
        lon = optionalDouble(node, "lon"),
        speed = optionalDouble(node, "speed"),
        heading = optionalDouble(node, "heading"),
        edgeId = optionalString(node, "edgeId"),
        routeId = optionalString(node, "routeId")
      )
    }
  }

  private def requiredString(
    node: JsonNode,
    field: String
  ): Either[String, String] = {

    val value = node.get(field)

    if (value == null || value.isNull) {
      Left(s"Missing required field: $field")
    } else if (!value.isTextual) {
      Left(s"Field '$field' must be a string")
    } else {
      Right(value.asText())
    }
  }

  private def requiredLong(
    node: JsonNode,
    field: String
  ): Either[String, Long] = {

    val value = node.get(field)

    if (value == null || value.isNull) {
      Left(s"Missing required field: $field")
    } else if (!value.isIntegralNumber) {
      Left(s"Field '$field' must be an integer")
    } else {
      Right(value.asLong())
    }
  }

  private def optionalString(
    node: JsonNode,
    field: String
  ): Option[String] = {

    val value = node.get(field)

    if (value == null || value.isNull) {
      None
    } else if (value.isTextual) {
      Some(value.asText())
    } else {
      None
    }
  }

  private def optionalDouble(
    node: JsonNode,
    field: String
  ): Option[Double] = {

    val value = node.get(field)

    if (value == null || value.isNull) {
      None
    } else if (value.isNumber) {
      Some(value.asDouble())
    } else {
      None
    }
  }
}
