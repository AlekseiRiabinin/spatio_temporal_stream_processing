ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "cityrover"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cityrover-trajectory-visualizer-job",

    libraryDependencies ++= Seq(
      // Spark core + SQL
      "org.apache.spark" %% "spark-core" % "3.5.4" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.4" % "provided",

      // Kafka + Structured Streaming (MUST match Spark version)
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.4",

      // Geometry (for trajectories, WKT/GeoJSON)
      "org.locationtech.jts" % "jts-core" % "1.19.0",

      // JSON (optional, for GeoJSON export)
      "io.circe" %% "circe-core"    % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser"  % "0.14.6",

      // Config
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.14"
    ),

    fork := true,

    Compile / mainClass := Some("cityrover.spark.trajectory.TrajectoryVisualizerMain"),

    assembly / assemblyJarName := "cityrover-trajectory-visualizer-job-assembly-0.1.0.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("application.conf") => MergeStrategy.concat
      case PathList("reference.conf")   => MergeStrategy.concat
      case PathList("META-INF", _*)     => MergeStrategy.discard
      case _                            => MergeStrategy.first
    }
  )
