package cityrover.graph.speed

import cityrover.graph.builder.RoadGraphBuilder.{RoadGraph, GraphEdge}


/**
  * SpeedLimitResolver enriches GraphEdge with a numeric speed limit (m/s).
  *
  * Priority:
  *   1. maxspeed tag (e.g., "60", "60 km/h", "50 mph")
  *   2. highway defaults (Russian rules)
  *   3. fallback = 13.9 m/s (50 km/h)
  */
object SpeedLimitResolver {

  // Russian defaults (km/h)
  private val highwayDefaults: Map[String, Int] = Map(
    "motorway"       -> 110,
    "trunk"          -> 90,
    "primary"        -> 60,
    "secondary"      -> 60,
    "tertiary"       -> 50,
    "residential"    -> 40,
    "living_street"  -> 20,
    "service"        -> 20
  )

  /** Convert km/h → m/s */
  private def kmhToMps(kmh: Double): Double =
    kmh / 3.6

  /** Parse maxspeed tag into m/s */
  private def parseMaxSpeed(tag: String): Option[Double] = {
    val cleaned = tag.trim.toLowerCase

    if (cleaned.matches("""\d+"""))
      return Some(kmhToMps(cleaned.toDouble))

    if (cleaned.matches("""\d+\s*km/h""")) {
      val num = cleaned.replace("km/h", "").trim.toDouble
      return Some(kmhToMps(num))
    }

    if (cleaned.matches("""\d+\s*mph""")) {
      val num = cleaned.replace("mph", "").trim.toDouble
      return Some(num * 0.44704)
    }

    None
  }

  /** Apply speed limits to all edges in the graph */
  def apply(graph: RoadGraph): RoadGraph = {

    val enrichedEdges: Map[String, GraphEdge] =
      graph.edges.map { case (id, edge) =>

        val tags = edge.tags

        // 1. maxspeed tag
        val fromMaxSpeed =
          tags.get("maxspeed").flatMap(parseMaxSpeed)

        // 2. highway default
        val highwayType = tags.getOrElse("highway", "residential")
        val defaultSpeed =
          highwayDefaults.get(highwayType).map(k => kmhToMps(k.toDouble))

        // 3. fallback
        val finalSpeed =
          fromMaxSpeed
            .orElse(defaultSpeed)
            .getOrElse(kmhToMps(50))

        val enriched = edge.copy(
          tags = edge.tags + ("speed_mps" -> finalSpeed.toString)
        )

        id -> enriched
      }.toMap

    graph.copy(edges = enrichedEdges)
  }
}
