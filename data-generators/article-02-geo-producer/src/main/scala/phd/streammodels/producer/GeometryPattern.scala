package phd.streammodels.producer

import scala.util.Random


sealed trait GeometryPattern {
  def nextWkt(rand: Random): String
}

final case class RandomPointRegion(
  minLon: Double,
  maxLon: Double,
  minLat: Double,
  maxLat: Double
) extends GeometryPattern {
  override def nextWkt(rand: Random): String = {
    val lon = minLon + rand.nextDouble() * (maxLon - minLon)
    val lat = minLat + rand.nextDouble() * (maxLat - minLat)
    f"POINT($lon%.6f $lat%.6f)"
  }
}

final case class ClusteredPoints(
  centers: Seq[(Double, Double)],
  stdDev: Double
) extends GeometryPattern {
  override def nextWkt(rand: Random): String = {
    val (clon, clat) = centers(rand.nextInt(centers.size))
    val lon = clon + rand.nextGaussian() * stdDev
    val lat = clat + rand.nextGaussian() * stdDev
    f"POINT($lon%.6f $lat%.6f)"
  }
}

object GeometryPattern {

  def fromEnv(): GeometryPattern = {
    val pattern = sys.env.getOrElse("GEOMETRY_PATTERN", "random").toLowerCase

    pattern match {
      case "random" =>
        RandomPointRegion(
          minLon = 30.0,
          maxLon = 50.0,
          minLat = 50.0,
          maxLat = 60.0
        )

      case "clustered" =>
        val centers = Seq(
          (37.62, 55.75), // Moscow-ish
          (30.52, 50.45), // Kyiv-ish
          (27.57, 53.90)  // Minsk-ish
        )
        ClusteredPoints(centers, stdDev = 0.1)

      case other =>
        println(s"[GeometryPattern] Unknown pattern '$other', falling back to random")
        RandomPointRegion(
          minLon = 30.0,
          maxLon = 50.0,
          minLat = 50.0,
          maxLat = 60.0
        )
    }
  }
}
