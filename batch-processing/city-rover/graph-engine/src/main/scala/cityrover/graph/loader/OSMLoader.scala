package cityrover.graph.loader

import com.typesafe.config.ConfigFactory

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.locationtech.jts.geom.LineString
import org.locationtech.jts.io.WKBReader

import org.openstreetmap.osmosis.osmbinary.BinaryParser
import org.openstreetmap.osmosis.osmbinary.Osmformat._
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream


/**
  * OSMLoader supporting two modes:
  *
  * 1. PRIMARY: Load rover-ready OSM data from PostgreSQL (api.v_osm_dubai_rover)
  * 2. BACKUP: Parse .osm.pbf using Osmosis BinaryParser
  *
  * Output:
  *   RawOSMData(
  *     nodes: Map[Long, RawNode],
  *     ways:  Seq[RawWay]
  *   )
  */
object OSMLoader {

  private val config = ConfigFactory.load()
  private val pg = config.getConfig("cityrover.graph-engine.postgres")

  private val pgHost     = pg.getString("host")
  private val pgPort     = pg.getInt("port")
  private val pgDatabase = pg.getString("database")
  private val pgUser     = pg.getString("user")
  private val pgPassword = pg.getString("password")

  private val jdbcUrl =
    s"jdbc:postgresql://$pgHost:$pgPort/$pgDatabase"

  // ---------------------------------------------------------------------------
  // Data models
  // ---------------------------------------------------------------------------

  case class RawNode(id: Long, lat: Double, lon: Double)

  case class RawWay(
      id: Long,
      nodeIds: Seq[Long],
      tags: Map[String, String]
  )

  case class RawOSMData(
      nodes: Map[Long, RawNode],
      ways: Seq[RawWay]
  )

  // ---------------------------------------------------------------------------
  // Coordinate conversion helpers (used only in PBF mode)
  // ---------------------------------------------------------------------------

  private def parseLat(latNano: Long): Double = latNano * 1e-9
  private def parseLon(lonNano: Long): Double = lonNano * 1e-9

  private val wkbReader = new WKBReader()

  // ---------------------------------------------------------------------------
  // Unified loader entry point
  // ---------------------------------------------------------------------------

  def load(path: String): RawOSMData = {
    try {
      println("OSMLoader: Attempting to load OSM data from PostgreSQL…")
      loadFromPostgres()
    } catch {
      case ex: Exception =>
        println(
          s"OSMLoader: PostgreSQL load failed (${ex.getMessage()}). " +
          s"Falling back to PBF parsing."
        )
        loadFromPbf(path)
    }
  }

  // ---------------------------------------------------------------------------
  // PRIMARY MODE: Load rover-ready OSM data from PostgreSQL
  // ---------------------------------------------------------------------------

  def loadFromPostgres(): RawOSMData = {

    val conn = DriverManager.getConnection(jdbcUrl, pgUser, pgPassword)

    val sql =
      """
      SELECT
        rover_way_id,
        osm_id,
        ST_AsBinary(geom) AS geom_wkb,
        highway,
        name,
        surface,
        lanes,
        oneway,
        foot_access,
        bicycle_access,
        indoor,
        service_type
      FROM api.v_osm_dubai_rover;
      """

    val stmt = conn.prepareStatement(sql)
    val rs = stmt.executeQuery()

    val nodes = mutable.Map.empty[Long, RawNode]
    val ways  = mutable.ArrayBuffer.empty[RawWay]

    while (rs.next()) {

      val roverWayId = rs.getLong("rover_way_id")
      val osmId      = rs.getLong("osm_id")

      val geomBytes  = rs.getBytes("geom_wkb")
      val geom       = wkbReader.read(geomBytes).asInstanceOf[LineString]

      // Convert geometry to node list (RawWay expects node IDs)
      val coords = geom.getCoordinates
      val nodeIds = coords.indices.map(i => roverWayId * 1000 + i) // synthetic node IDs

      // Populate RawNode map
      coords.zip(nodeIds).foreach { case (c, nid) =>
        nodes += nid -> RawNode(nid, c.y, c.x)
      }

      // Flatten tags into RawWay.tags
      val tags = Map(
        "highway"       -> rs.getString("highway"),
        "name"          -> rs.getString("name"),
        "surface"       -> rs.getString("surface"),
        "lanes"         -> Option(rs.getInt("lanes")).filter(_ != 0).map(_.toString).orNull,
        "oneway"        -> rs.getBoolean("oneway").toString,
        "foot_access"   -> rs.getString("foot_access"),
        "bicycle_access"-> rs.getString("bicycle_access"),
        "indoor"        -> rs.getBoolean("indoor").toString,
        "service"       -> rs.getString("service_type")
      ).filter(_._2 != null)

      ways += RawWay(
        id = roverWayId,
        nodeIds = nodeIds,
        tags = tags
      )
    }

    conn.close()
    RawOSMData(nodes.toMap, ways.toSeq)
  }

  // ---------------------------------------------------------------------------
  // BACKUP MODE: Parse .osm.pbf using Osmosis BinaryParser
  // ---------------------------------------------------------------------------

  def loadFromPbf(path: String): RawOSMData = {

    val nodes = mutable.Map.empty[Long, RawNode]
    val ways  = mutable.ArrayBuffer.empty[RawWay]

    val parser = new BinaryParser() {

      override protected def parse(header: HeaderBlock): Unit = {}

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

          nodes += id -> RawNode(id, parseLat(lat), parseLon(lon))
        }
      }

      override protected def parseNodes(list: java.util.List[Node]): Unit = {
        list.asScala.foreach { n =>
          nodes += n.getId -> RawNode(
            n.getId,
            parseLat(n.getLat),
            parseLon(n.getLon)
          )
        }
      }

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

      override protected def parseRelations(list: java.util.List[Relation]): Unit = {}

      private def extractTags(w: Way): Map[String,String] = {
        val m = mutable.Map[String,String]()
        val keys = w.getKeysList
        val vals = w.getValsList

        for(i <- 0 until keys.size()) {
          m += getStringById(keys.get(i)) -> getStringById(vals.get(i))
        }

        m.toMap
      }

      override def complete(): Unit = {}
    }

    val stream = new BlockInputStream(new FileInputStream(path), parser)
    stream.process()

    RawOSMData(nodes.toMap, ways.toSeq)
  }
}
