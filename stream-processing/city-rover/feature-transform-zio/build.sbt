import sbtassembly.AssemblyPlugin.autoImport.*
import sbtassembly.MergeStrategy

import scalapb.compiler.Version.scalapbVersion


ThisBuild / scalaVersion := "3.9.0"
ThisBuild / javacOptions ++= Seq("--release", "25")

lazy val root = (project in file("."))
  .settings(
    name := "feature-transform-zio",
    version := "0.1.0-SNAPSHOT",

    // ---- Assembly ----
    assembly / mainClass := Some("cityrover.Main"),

    assembly / assemblyJarName :=
      "feature-transform-zio-assembly-0.1.0-SNAPSHOT.jar",

    assembly / assemblyOption ~= (_.withIncludeScala(true)),

    // ---- ScalaPB ----
    Compile / PB.targets := Seq(
      scalapb.gen(
        flatPackage = true,
        scala3Sources = scalaBinaryVersion.value == "3"
      ) -> (Compile / sourceManaged).value / "scalapb"
    ),

    // ---- Dependencies ----
    libraryDependencies ++= Seq(
      // ZIO core and streams
      "dev.zio" %% "zio" % "2.1.27",
      "dev.zio" %% "zio-streams" % "2.1.27",

      // Kafka integration
      "dev.zio" %% "zio-kafka" % "3.0.0",

      // Configuration
      "dev.zio" %% "zio-config" % "4.0.0",
      "dev.zio" %% "zio-config-typesafe" % "4.0.0",
      "dev.zio" %% "zio-config-magnolia" % "4.0.0",

      // Prometheus metrics
      "dev.zio" %% "zio-metrics-connectors-prometheus" % "2.5.4",

      // HTTP server (for Prometheus /metrics endpoint)
      // Reference version in official docs is 3.8.1; latest stable is 3.11.6
      "dev.zio" %% "zio-http" % "3.11.6",

      // ScalaPB runtime
      "com.thesamet.scalapb" %% "scalapb-runtime" %
        scalapbVersion % "protobuf",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.5.18"
    ),

    // ---- Assembly merge strategy ----
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") =>
        MergeStrategy.discard

      case PathList("META-INF", "services", _*) =>
        MergeStrategy.filterDistinctLines

      case PathList("META-INF", xs @ _*)
          if xs.exists(x =>
            x.endsWith(".SF") ||
            x.endsWith(".DSA") ||
            x.endsWith(".RSA") ||
            x.endsWith(".EC")
          ) =>
        MergeStrategy.discard

      case "reference.conf" =>
        MergeStrategy.concat

      case "application.conf" =>
        MergeStrategy.concat

      case x if x.endsWith(".proto") =>
        MergeStrategy.deduplicate

      case x =>
        MergeStrategy.deduplicate
    },

    // ---- Runtime ----
    Compile / run / mainClass := Some("cityrover.Main"),

    run / fork := true,

    run / javaOptions ++= Seq(
      "-Xmx2G",
      "-XX:+UseG1GC"
    )
  )
