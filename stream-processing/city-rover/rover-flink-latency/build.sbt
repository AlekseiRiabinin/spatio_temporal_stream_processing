// ------------------------------------------------------------
// Global versions
// ------------------------------------------------------------
ThisBuild / scalaVersion := "3.3.8"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "cityrover.flink"

// ------------------------------------------------------------
// Dependency versions
// ------------------------------------------------------------
lazy val flinkVersion        = "2.3.0"
lazy val flinkKafkaVersion   = "3.1.0-1.18"  // Kafka connector
lazy val magnoliaVersion     = "1.3.0"
lazy val jacksonVersion      = "2.17.2"
lazy val kryoVersion         = "5.6.0"
lazy val log4jVersion        = "2.23.1"
lazy val scalatestVersion    = "3.2.19"

// ------------------------------------------------------------
// Project definition
// ------------------------------------------------------------
lazy val root = (project in file("."))
  .settings(
    name := "rover-flink-latency",

    libraryDependencies ++= Seq(
      // ------------------------------------------------------------
      // Flink Core (Java API only)
      // ------------------------------------------------------------
      "org.apache.flink" % "flink-streaming-java"      % flinkVersion,
      "org.apache.flink" % "flink-clients"             % flinkVersion,
      "org.apache.flink" % "flink-runtime"             % flinkVersion,
      "org.apache.flink" % "flink-core"                % flinkVersion,
      "org.apache.flink" % "flink-metrics-prometheus"  % flinkVersion,

      // ------------------------------------------------------------
      // Kafka Source (correct connector for Flink 2.x)
      // ------------------------------------------------------------
      "org.apache.flink" % "flink-connector-kafka"     % flinkKafkaVersion,

      // ------------------------------------------------------------
      // Kryo + Magnolia Serialization
      // ------------------------------------------------------------
      "com.esotericsoftware" % "kryo"                  % kryoVersion,
      "com.softwaremill.magnolia1_3" %% "magnolia"     % magnoliaVersion,

      // ------------------------------------------------------------
      // JSON (Jackson)
      // ------------------------------------------------------------
      "com.fasterxml.jackson.core" % "jackson-databind"          % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala"   % jacksonVersion,

      // ------------------------------------------------------------
      // Typesafe Config (required for ConfigLoader.scala)
      // ------------------------------------------------------------
      "com.typesafe" % "config" % "1.4.3",

      // ------------------------------------------------------------
      // Logging
      // ------------------------------------------------------------
      "org.apache.logging.log4j" % "log4j-api"          % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core"         % log4jVersion,

      // ------------------------------------------------------------
      // Testing
      // ------------------------------------------------------------
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),

    // ------------------------------------------------------------
    // Assembly settings
    // ------------------------------------------------------------
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", "MANIFEST.MF")       => MergeStrategy.discard
      case _                                         => MergeStrategy.first
    }
  )
