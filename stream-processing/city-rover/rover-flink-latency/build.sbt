import sbtprotoc.ProtocPlugin.autoImport._
import scalapb.compiler.Version.scalapbVersion

// ============================================================
// Global versions
// ============================================================

ThisBuild / scalaVersion := "3.3.8"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "cityrover.flink"

// ============================================================
// Dependency versions
// ============================================================

lazy val flinkVersion      = "2.3.0"
lazy val flinkKafkaVersion = "5.0.0-2.2"

lazy val jacksonVersion   = "2.17.2"
lazy val kryoVersion      = "5.6.0"
lazy val log4jVersion     = "2.23.1"
lazy val scalatestVersion = "3.2.19"
lazy val scalapbVersion   = "0.11.13"

// ============================================================
// Project
// ============================================================

lazy val root = (project in file("."))
  .settings(

    name := "rover-flink-latency",

    // ========================================================
    // Flink
    // ========================================================

    libraryDependencies ++= Seq(

      // Flink Java APIs
      "org.apache.flink" % "flink-streaming-java"     % flinkVersion,
      "org.apache.flink" % "flink-clients"            % flinkVersion,
      "org.apache.flink" % "flink-runtime"            % flinkVersion,
      "org.apache.flink" % "flink-core"               % flinkVersion,
      "org.apache.flink" % "flink-metrics-prometheus" % flinkVersion,

      // Kafka connector
      "org.apache.flink" % "flink-connector-kafka" % flinkKafkaVersion,

      // Kryo
      "com.esotericsoftware" % "kryo" % kryoVersion,

      // Jackson (still used for config)
      "com.fasterxml.jackson.core"   % "jackson-databind"      % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,

      // Typesafe Config
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "org.apache.logging.log4j" % "log4j-api"  % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

      // ScalaPB runtime
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",

      // Tests
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),

    // ========================================================
    // ScalaPB / Protobuf code generation
    // ========================================================

    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value
    ),

    // ========================================================
    // Assembly
    // ========================================================

    assembly / assemblyMergeStrategy := {

      case PathList("META-INF", "services", _*) =>
        MergeStrategy.concat

      case PathList("META-INF", _*) =>
        MergeStrategy.discard

      case _ =>
        MergeStrategy.first
    }
  )
