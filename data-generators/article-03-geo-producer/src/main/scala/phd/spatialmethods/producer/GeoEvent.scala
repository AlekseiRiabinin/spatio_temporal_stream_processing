package phd.spatialmethods.producer

import java.time.Instant


case class GeoEvent(
  id: String,
  objectId: String,
  timestamp: Instant,
  lon: Double,
  lat: Double,
  wkt: String,
  speed: Double,
  heading: Double,
  attributes: Map[String, String] = Map.empty
) {

  def toJson: String = {
    val attrsJson =
      if (attributes.isEmpty) ""
      else attributes.map { case (k, v) => s""""$k": "$v"""" }.mkString(", ")

    s"""{
       |  "id": "$id",
       |  "objectId": "$objectId",
       |  "timestamp": "${timestamp.toString}",
       |  "lon": $lon,
       |  "lat": $lat,
       |  "wkt": "$wkt",
       |  "speed": $speed,
       |  "heading": $heading,
       |  "attributes": { $attrsJson }
       |}""".stripMargin
  }
}
