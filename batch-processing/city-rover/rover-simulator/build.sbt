ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "cityrover"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cityrover-rover-simulator",

    libraryDependencies ++= Seq(
      // JSON serialization
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // Kafka producer API
      "org.apache.kafka" % "kafka-clients" % "3.8.0",

      // Config loader
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.14",

      // Parquet + Avro
      "org.apache.parquet" % "parquet-avro" % "1.13.1",
      "org.apache.parquet" % "parquet-hadoop" % "1.13.1",
      "org.apache.avro" % "avro" % "1.11.3",

      // Hadoop FS (minimal)
      "org.apache.hadoop" % "hadoop-common" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",

      // Geometry
      "org.locationtech.jts" % "jts-core" % "1.19.0"
    ),

    fork := true,

    Compile / mainClass := Some("cityrover.sim.runtime.RoverSimulatorMain"),

    dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-3",

    assembly / assemblyJarName := "cityrover-rover-simulator-assembly-0.1.0.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("application.conf") => MergeStrategy.concat
      case PathList("reference.conf")   => MergeStrategy.concat
      case PathList("META-INF", _*)     => MergeStrategy.discard
      case _                            => MergeStrategy.first
    }
  )
