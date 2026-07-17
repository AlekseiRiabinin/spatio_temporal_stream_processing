name := "cityrover-graph-engine"

version := "0.1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // OSM4J (complete PBF reader)
  "org.openstreetmap.osmosis" % "osmosis-osm-binary" % "0.47",

  // Geospatial utilities (distance, bearings, projections)
  "org.locationtech.jts" % "jts-core" % "1.19.0",

  // Hadoop (required for Parquet Path)
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",

  // Parquet output for nodes/edges
  "org.apache.parquet" % "parquet-avro" % "1.13.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.13.1",

  // Config loader
  "com.typesafe" % "config" % "1.4.3",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.14"
)

fork := true

mainClass in Compile := Some("cityrover.graph.GraphEngineMain")
