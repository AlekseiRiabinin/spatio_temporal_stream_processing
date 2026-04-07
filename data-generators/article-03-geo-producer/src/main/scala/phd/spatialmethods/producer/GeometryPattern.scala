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
      case "random" =>
        RandomPointRegion(30.0, 50.0, 50.0, 60.0)

      case "clustered" =>
        ClusteredPoints(
          centers = Seq(
            (55.27, 25.20), // Dubai-ish
            (37.62, 55.75), // Moscow-ish
            (30.52, 50.45)  // Kyiv-ish
          ),
          stdDev = 0.03
        )

      case other =>
        println(s"[GeometryPattern] Unknown '$other', using random")
        RandomPointRegion(30.0, 50.0, 50.0, 60.0)
    }
  }
}
