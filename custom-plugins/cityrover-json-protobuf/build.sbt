import sbtprotoc.ProtocPlugin.autoImport.*
import scalapb.compiler.Version.scalapbVersion

// ============================================================
// CityRover JSON → Protobuf Kafka Connect Plugin
// ============================================================

ThisBuild / scalaVersion := "3.3.8"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "cityrover.kafka.connect"

// ============================================================
// Dependency versions
// ============================================================

lazy val kafkaVersion     = "3.7.0"
lazy val connectVersion   = "3.7.0"
lazy val jacksonVersion   = "2.17.2"
lazy val protobufVersion  = "3.25.3"
lazy val scalapbVersion_  = "0.11.13"
lazy val scalatestVersion = "3.2.19"

// ============================================================
// Project
// ============================================================

lazy val root = (project in file("."))
  .settings(

    name := "cityrover-json-protobuf-connect",

    // ========================================================
    // Dependencies
    // ========================================================

    libraryDependencies ++= Seq(

      "org.apache.kafka" % "connect-api" %
        connectVersion,

      "org.apache.kafka" % "connect-transforms" %
        connectVersion,

      "org.apache.kafka" % "kafka-clients" %
        kafkaVersion,

      "com.fasterxml.jackson.core" % "jackson-databind" %
        jacksonVersion,

      "com.google.protobuf" % "protobuf-java" %
        protobufVersion,

      "com.thesamet.scalapb" %% "scalapb-runtime" %
        scalapbVersion_ % "protobuf",

      "org.scalatest" %% "scalatest" %
        scalatestVersion % Test
    ),

    // ========================================================
    // ScalaPB code generation
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
