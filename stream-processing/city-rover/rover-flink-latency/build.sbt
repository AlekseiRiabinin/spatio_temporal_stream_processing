ThisBuild / scalaVersion := "3.3.8"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "cityrover.flink"

lazy val flinkVersion = "2.3.0"
lazy val magnoliaVersion = "1.3.0"
lazy val jacksonVersion = "2.17.2"
lazy val kryoVersion = "5.6.0"

lazy val root = (project in file("."))
  .settings(
    name := "rover-flink-latency",

    libraryDependencies ++= Seq(
      // -----------------------------
      // Flink Core
      // -----------------------------
      "org.apache.flink" % "flink-streaming-java" % flinkVersion,
      "org.apache.flink" % "flink-clients" % flinkVersion,
      "org.apache.flink" % "flink-runtime" % flinkVersion,
      "org.apache.flink" % "flink-metrics-prometheus" % flinkVersion,

      // -----------------------------
      // Kafka Source (new unified source)
      // -----------------------------
      "org.apache.flink" % "flink-connector-kafka" % flinkVersion,

      // -----------------------------
      // Serialization
      // -----------------------------
      "com.softwaremill.magnolia" %% "magnolia" % magnoliaVersion,
      "com.esotericsoftware" % "kryo" % kryoVersion,

      // -----------------------------
      // JSON (Jackson)
      // -----------------------------
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,

      // -----------------------------
      // Logging
      // -----------------------------
      "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.23.1",

      // -----------------------------
      // Testing
      // -----------------------------
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),

    // -----------------------------
    // Assembly settings
    // -----------------------------
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
