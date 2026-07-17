package cityrover.graph.loader

import java.io.FileInputStream

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.openstreetmap.osmosis.osmbinary.BinaryParser
import org.openstreetmap.osmosis.osmbinary.Osmformat._
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream


/**
  * OSMLoader using OSM-Binary (Osmosis) to parse .osm.pbf files.
  *
  * Output:
  *   RawOSMData(
  *     nodes: Map[Long, RawNode],
  *     ways:  Seq[RawWay]
  *   )
  *
  * Only drivable roads (highway=* tags) are kept.
  */
object OSMLoader {

  case class RawNode(id: Long, lat: Double, lon: Double)
  case class RawWay(id: Long, nodeIds: Seq[Long], tags: Map[String, String])
  case class RawOSMData(nodes: Map[Long, RawNode], ways: Seq[RawWay])

  // ---------------------------------------------------------------------------
  // Coordinate conversion helpers
  // ---------------------------------------------------------------------------

  private def parseLat(latNano: Long): Double = latNano * 1e-9
  private def parseLon(lonNano: Long): Double = lonNano * 1e-9

  // ---------------------------------------------------------------------------
  // Loader entry point
  // ---------------------------------------------------------------------------

  def load(path: String): RawOSMData = {

    val nodes = mutable.Map.empty[Long, RawNode]
    val ways  = mutable.ArrayBuffer.empty[RawWay]

    val parser = new BinaryParser() {

      // -----------------------------------------------------------------------
      // Header
      // -----------------------------------------------------------------------

      override protected def parse(header: HeaderBlock): Unit = {
        // ignore
      }

      // -----------------------------------------------------------------------
      // Dense nodes
      // -----------------------------------------------------------------------

      override protected def parseDense(dense: DenseNodes): Unit = {

        var id = 0L
        var lat = 0L
        var lon = 0L

        val ids  = dense.getIdList
        val lats = dense.getLatList
        val lons = dense.getLonList

        for (i <- 0 until ids.size()) {

          id  += ids.get(i)
          lat += lats.get(i)
          lon += lons.get(i)

          nodes += id -> RawNode(
            id,
            parseLat(lat),
            parseLon(lon)
          )
        }
      }

      // -----------------------------------------------------------------------
      // Normal nodes
      // -----------------------------------------------------------------------

      override protected def parseNodes(list: java.util.List[Node]): Unit = {

        list.asScala.foreach { n =>
          nodes += n.getId -> RawNode(
            n.getId,
            parseLat(n.getLat),
            parseLon(n.getLon)
          )
        }
      }

      // -----------------------------------------------------------------------
      // Ways
      // -----------------------------------------------------------------------

      override protected def parseWays(list: java.util.List[Way]): Unit = {

        list.asScala.foreach { w =>

          val tags = extractTags(w)

          if (tags.contains("highway")) {

            var ref = 0L
            val refs =
              w.getRefsList.asScala.map { delta =>
                ref += delta
                ref
              }

            ways += RawWay(
              w.getId,
              refs.toSeq,
              tags
            )
          }
        }
      }

      // -----------------------------------------------------------------------
      // Relations (ignored)
      // -----------------------------------------------------------------------

      override protected def parseRelations(
        list: java.util.List[Relation]
      ): Unit = {}

      // -----------------------------------------------------------------------
      // Tag extraction
      // -----------------------------------------------------------------------

      private def extractTags(w: Way): Map[String,String] = {

        val m = mutable.Map[String,String]()

        val keys = w.getKeysList
        val vals = w.getValsList

        for(i <- 0 until keys.size()) {
          m +=
            getStringById(keys.get(i)) ->
            getStringById(vals.get(i))
        }

        m.toMap
      }

      override def complete(): Unit = {}
    }

    val stream =
      new BlockInputStream(
        new FileInputStream(path),
        parser
      )

    stream.process()

    RawOSMData(nodes.toMap, ways.toSeq)
  }
}
