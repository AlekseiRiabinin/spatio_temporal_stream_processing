package phd.adaptivecontrol.producer.geometry

import scala.util.Random


sealed trait GeometryPattern {
  def initialPoint(rand: Random): (Double, Double)
}


// ============================================================
// Random Spatial Distribution
// ============================================================
final case class RandomPointRegion(
  minLon: Double,
  maxLon: Double,
  minLat: Double,
  maxLat: Double
) extends GeometryPattern {

  override def initialPoint(rand: Random): (Double, Double) = {

    val lon =
      minLon + rand.nextDouble() * (maxLon - minLon)

    val lat =
      minLat + rand.nextDouble() * (maxLat - minLat)

    (lon, lat)
  }
}


// ============================================================
// Clustered Spatial Distribution
// ============================================================
final case class ClusteredPoints(
  centers: Seq[(Double, Double)],
  stdDev: Double
) extends GeometryPattern {

  override def initialPoint(rand: Random): (Double, Double) = {

    val (clon, clat) =
      centers(rand.nextInt(centers.size))

    val lon =
      clon + rand.nextGaussian() * stdDev

    val lat =
      clat + rand.nextGaussian() * stdDev

    (lon, lat)
  }
}


// ============================================================
// Corridor Spatial Distribution
// ============================================================
final case class CorridorPoints(
  corridors: Seq[((Double, Double), (Double, Double))],
  corridorWidth: Double
) extends GeometryPattern {

  override def initialPoint(rand: Random): (Double, Double) = {

    val ((lon1, lat1), (lon2, lat2)) =
      corridors(rand.nextInt(corridors.size))

    // Position along corridor
    val t =
      rand.nextDouble()

    val baseLon =
      lon1 + (lon2 - lon1) * t

    val baseLat =
      lat1 + (lat2 - lat1) * t

    // Perpendicular noise
    val noiseLon =
      rand.nextGaussian() * corridorWidth

    val noiseLat =
      rand.nextGaussian() * corridorWidth

    (
      baseLon + noiseLon,
      baseLat + noiseLat
    )
  }
}


// ============================================================
// Factory
// ============================================================
object GeometryPattern {

  def fromEnv(): GeometryPattern = {

    sys.env
      .getOrElse("GEOMETRY_PATTERN", "random")
      .toLowerCase match {

      // ======================================================
      // Random Region
      // ======================================================
      case "random" =>

        RandomPointRegion(
          minLon = 29.5,
          maxLon = 31.5,
          minLat = 59.7,
          maxLat = 60.1
        )

      // ======================================================
      // Clustered Saint Petersburg
      // ======================================================
      case "clustered" =>

        ClusteredPoints(
          centers = Seq(
            (30.3158, 59.9390),
            (30.3180, 59.9550),
            (30.2920, 59.9485),
            (30.3200, 59.9700),
            (30.3800, 59.9300)
          ),
          stdDev = 0.001
        )

      // ======================================================
      // Corridor Geometry
      // ======================================================
      case "corridor" =>

        CorridorPoints(

          corridors = Seq(

            // Nevsky Prospekt
            (
              (30.3000, 59.9300),
              (30.3800, 59.9500)
            ),

            // Moskovsky Prospekt
            (
              (30.3000, 59.8800),
              (30.3300, 59.9700)
            ),

            // Vasilievsky Island corridor
            (
              (30.2400, 59.9300),
              (30.3000, 59.9500)
            )
          ),

          // ~20-40 meters depending on latitude
          corridorWidth = 0.0003
        )

      // ======================================================
      // Fallback
      // ======================================================
      case other =>

        println(
          s"[GeometryPattern] Unknown '$other', using random"
        )

        RandomPointRegion(
          minLon = 29.5,
          maxLon = 31.5,
          minLat = 59.7,
          maxLat = 60.1
        )
    }
  }
}
