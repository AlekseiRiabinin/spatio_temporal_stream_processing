ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "cityrover"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cityrover-graph-engine",

    libraryDependencies ++= Seq(
      "org.openstreetmap.osmosis" % "osmosis-osm-binary" % "0.47",
      "org.locationtech.jts" % "jts-core" % "1.19.0",
      "org.apache.hadoop" % "hadoop-common" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
      "org.apache.parquet" % "parquet-avro" % "1.13.1",
      "org.apache.parquet" % "parquet-hadoop" % "1.13.1",
      "com.typesafe" % "config" % "1.4.3",
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      "org.postgresql" % "postgresql" % "42.7.3"
    ),

    fork := true,

    Compile / mainClass := Some("cityrover.graph.GraphEngineMain"),

    assembly / assemblyJarName := "cityrover-graph-engine-assembly-0.1.0.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )
