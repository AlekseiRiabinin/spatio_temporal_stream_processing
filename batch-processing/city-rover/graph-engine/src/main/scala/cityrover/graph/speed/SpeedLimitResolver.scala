package cityrover.graph.speed

import cityrover.graph.builder.RoadGraphBuilder.{RoadGraph, GraphEdge}


/**
  * SpeedLimitResolver enriches GraphEdge with a numeric speed limit (m/s).
  *
  * Priority:
  *   1. maxspeed tag (e.g., "60", "60 km/h", "50 mph")
  *   2. pedestrian defaults (footway, path, corridor, indoor)
  *   3. service defaults (alley, driveway, laneway, parking_aisle)
  *   4. slow-road defaults (residential, living_street, unclassified, road)
  *   5. fallback = 13.9 m/s (50 km/h)
  *
  * This resolver matches EXACTLY the tag universe produced by
  * core.populate_osm_dubai_rover().
  */
object SpeedLimitResolver {

  // ---------------------------------------------------------------------------
  // Dubai pedestrian defaults (m/s)
  // ---------------------------------------------------------------------------

  private val pedestrianDefaults: Map[String, Double] = Map(
    "footway"       -> 1.4,   // ~5 km/h
    "path"          -> 1.4,
    "pedestrian"    -> 1.4,
    "corridor"      -> 1.3,
    "steps"         -> 1.0,
    "indoor"        -> 1.2,
    "cycleway"      -> 4.0,
    "track"         -> 2.0,
    "living_street" -> 2.0
  )

  // ---------------------------------------------------------------------------
  // Dubai service defaults (m/s)
  // ---------------------------------------------------------------------------

  private val serviceDefaults: Map[String, Double] = Map(
    "alley"         -> 2.0,
    "driveway"      -> 3.0,
    "laneway"       -> 2.0,
    "parking_aisle" -> 3.0,
    "parking road"  -> 3.0,
    "yard"          -> 2.0,
    "footway"       -> 1.4,   // service=footway
    "مدخل الجامع"   -> 1.2    // mosque entrance
  )

  // ---------------------------------------------------------------------------
  // Slow-road defaults (km/h → m/s)
  // ---------------------------------------------------------------------------

  private val slowRoadDefaultsKmh: Map[String, Int] = Map(
    "residential"   -> 40,
    "service"       -> 20,
    "unclassified"  -> 30,
    "road"          -> 30
  )

  private def kmhToMps(kmh: Double): Double = kmh / 3.6

  // ---------------------------------------------------------------------------
  // Parse maxspeed tag
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Apply speed limits to graph
  // ---------------------------------------------------------------------------

  def apply(graph: RoadGraph): RoadGraph = {

    val enrichedEdges: Map[String, GraphEdge] =
      graph.edges.map { case (id, edge) =>

        val tags = edge.tags

        // 1. maxspeed tag
        val fromMaxSpeed =
          tags.get("maxspeed").flatMap(parseMaxSpeed)

        // 2. pedestrian defaults
        val highwayType = tags.getOrElse("highway", "")
        val fromPedestrian = 
          pedestrianDefaults
            .get(highwayType)
            .orElse {
              if (tags.get("indoor").contains("true"))
                Some(pedestrianDefaults("indoor"))
              else None
            }

        // 3. service defaults
        val serviceType = tags.getOrElse("service_type", "")
        val fromService = serviceDefaults.get(serviceType)

        // 4. slow-road defaults (km/h → m/s)
        val fromSlowRoad =
          slowRoadDefaultsKmh.get(highwayType).map(k => kmhToMps(k.toDouble))

        // 5. fallback
        val fallback = kmhToMps(50) // 13.9 m/s

        val finalSpeed =
          fromMaxSpeed
            .orElse(fromPedestrian)
            .orElse(fromService)
            .orElse(fromSlowRoad)
            .getOrElse(fallback)

        val enriched = edge.copy(
          tags = edge.tags + ("speed_mps" -> finalSpeed.toString)
        )

        id -> enriched
      }.toMap

    graph.copy(edges = enrichedEdges)
  }
}
