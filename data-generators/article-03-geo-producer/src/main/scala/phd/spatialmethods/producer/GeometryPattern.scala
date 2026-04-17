package phd.spatialmethods.producer

import scala.util.Random


sealed trait GeometryPattern {
  def initialPoint(rand: Random): (Double, Double)
}

final case class RandomPointRegion(
  minLon: Double,
  maxLon: Double,
  minLat: Double,
  maxLat: Double
) extends GeometryPattern {

  override def initialPoint(rand: Random): (Double, Double) = {
    val lon = minLon + rand.nextDouble() * (maxLon - minLon)
    val lat = minLat + rand.nextDouble() * (maxLat - minLat)
    (lon, lat)
  }
}

final case class ClusteredPoints(
  centers: Seq[(Double, Double)],
  stdDev: Double
) extends GeometryPattern {

  override def initialPoint(rand: Random): (Double, Double) = {
    val (clon, clat) = centers(rand.nextInt(centers.size))
    val lon = clon + rand.nextGaussian() * stdDev
    val lat = clat + rand.nextGaussian() * stdDev
    (lon, lat)
  }
}

object GeometryPattern {

  def fromEnv(): GeometryPattern = {
    sys.env.getOrElse("GEOMETRY_PATTERN", "random").toLowerCase match {

      // Northwestern Federal District (Saint Petersburg) random region
      case "random" =>
        RandomPointRegion(
          minLon = 29.5,
          maxLon = 31.5,
          minLat = 59.7,
          maxLat = 60.1
        )

      // Saint Petersburg clustered geometry
      case "clustered" =>
        ClusteredPoints(
          centers = Seq(
            (30.3158, 59.9390),  // Admiralteysky / Nevsky
            (30.3180, 59.9550),  // Liteyny / Chernyshevskaya
            (30.2920, 59.9485),  // Vasilievsky Island
            (30.3200, 59.9700),  // Petrogradsky
            (30.3800, 59.9300)   // Moskovsky
          ),
          stdDev = 0.001        // ~110m cluster radius
        )

      case other =>
        println(s"[GeometryPattern] Unknown '$other', using random")
        RandomPointRegion(
          minLon = 29.5,
          maxLon = 31.5,
          minLat = 59.7,
          maxLat = 60.1
        )
    }
  }
}
