package phd.spatialmethods.producer


case class GeoEvent(
  id: String,
  objectId: String,
  timestamp: Long,     // epoch millis
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
       |  "timestamp": $timestamp,
       |  "lon": $lon,
       |  "lat": $lat,
       |  "wkt": "$wkt",
       |  "speed": $speed,
       |  "heading": $heading,
       |  "attributes": { $attrsJson }
       |}""".stripMargin
  }
}
