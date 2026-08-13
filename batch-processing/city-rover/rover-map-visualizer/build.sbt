ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "cityrover"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "rover-map-visualizer",

    libraryDependencies ++= Seq(
      // --- HTTP server ---
      "com.typesafe.akka" %% "akka-actor"       % "2.6.21",
      "com.typesafe.akka" %% "akka-stream"      % "2.6.21",
      "com.typesafe.akka" %% "akka-http"        % "10.2.10",

      // --- JSON ---
      "io.circe" %% "circe-core"    % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser"  % "0.14.6",

      // --- Config ---
      "com.typesafe" % "config" % "1.4.3",

      // --- Logging ---
      "ch.qos.logback" % "logback-classic" % "1.4.14",

      // --- Testing ---
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    fork := true,

    Compile / mainClass := Some("cityrover.visualizer.RoverMapVisualizerMain"),

    // --- Assembly JAR ---
    assembly / assemblyJarName := "rover-map-visualizer-assembly-0.1.0.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("application.conf") => MergeStrategy.concat
      case PathList("reference.conf")   => MergeStrategy.concat
      case PathList("META-INF", _*)     => MergeStrategy.discard
      case _                            => MergeStrategy.first
    }
  )
