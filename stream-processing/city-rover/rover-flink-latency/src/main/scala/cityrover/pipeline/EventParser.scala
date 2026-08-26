package cityrover.pipeline

import cityrover.model.GeoEvent
import cityrover.util.JsonMapper


object EventParser {

  /**
    * Parse a JSON string into a GeoEvent.
    *
    * @param json Raw JSON from Kafka
    * @return Parsed GeoEvent instance
    */
  def parse(json: String): GeoEvent =
    JsonMapper.fromJson(json, classOf[GeoEvent])
}
