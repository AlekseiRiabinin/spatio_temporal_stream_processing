ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "cityrover"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cityrover-graph-engine",

    libraryDependencies ++= Seq(
      // OSM PBF parser
      "org.openstreetmap.osmosis" % "osmosis-osm-binary" % "0.47",

      // Geometry
      "org.locationtech.jts" % "jts-core" % "1.19.0",

      // Minimal Hadoop FS layer (exclude heavy transitive deps)
      "org.apache.hadoop" % "hadoop-common" % "3.3.6"
        exclude("org.postgresql", "postgresql")
        exclude("org.eclipse.jetty", "jetty-server")
        exclude("org.eclipse.jetty", "jetty-util")
        exclude("org.eclipse.jetty", "jetty-io")
        exclude("org.apache.curator", "curator-framework")
        exclude("org.apache.curator", "curator-recipes"),

      // Required by parquet-hadoop
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",

      // Parquet (safe)
      "org.apache.parquet" % "parquet-avro" % "1.13.1",
      "org.apache.parquet" % "parquet-hadoop" % "1.13.1",

      // Config
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.14",

      // PostgreSQL JDBC (your authoritative driver)
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
