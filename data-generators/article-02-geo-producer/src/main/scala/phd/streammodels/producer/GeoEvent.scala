package phd.streammodels.producer


case class GeoEvent(
  id: String,
  wkt: String,
  timestamp: Long,
  attributes: Map[String, String]
) {
  def toJson: String =
    s"""{
       |  "id": "$id",
       |  "wkt": "$wkt",
       |  "timestamp": $timestamp,
       |  "attributes": {
       |    "speed": "${attributes("speed")}"
       |  }
       |}""".stripMargin
}
