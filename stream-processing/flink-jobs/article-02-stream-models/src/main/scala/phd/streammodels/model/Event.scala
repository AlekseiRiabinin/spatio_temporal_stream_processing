package phd.streammodels.model

import org.locationtech.jts.geom.Geometry


/**
 * Spatio-temporal event:
 *
 * e = { id, g, t, A }
 *
 * id : unique object identifier
 * g  : spatial geometry (POINT / LINESTRING / POLYGON)
 * t  : event-time timestamp (milliseconds since epoch)
 * A  : optional attribute map
 */
case class Event(
  id: String,
  geometry: Geometry,
  eventTime: Long,
  attributes: Map[String, String] = Map.empty
)
