import sbtassembly.AssemblyPlugin.autoImport.*
import sbtassembly.MergeStrategy

import scalapb.compiler.Version.scalapbVersion


ThisBuild / scalaVersion := "3.9.0"
ThisBuild / javacOptions ++= Seq("--release", "25")

lazy val root = (project in file("."))
  .settings(
    name := "feature-writer-zio",
    version := "0.1.0-SNAPSHOT",

    // ---- Assembly ----
    assembly / mainClass := Some("cityrover.Main"),
    assembly / assemblyJarName :=
      "feature-writer-zio-assembly-0.1.0-SNAPSHOT.jar",
    assembly / assemblyOption ~= (_.withIncludeScala(true)),

    // ---- ScalaPB ----
    // The writer only needs EnrichedTelemetryEvent from telemetry.proto.
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

      // Kafka integration (consumer only — no producer)
      "dev.zio" %% "zio-kafka" % "3.0.0",

      // Configuration
      "dev.zio" %% "zio-config" % "4.0.0",
      "dev.zio" %% "zio-config-typesafe" % "4.0.0",
      "dev.zio" %% "zio-config-magnolia" % "4.0.0",

      // Prometheus metrics
      "dev.zio" %% "zio-metrics-connectors-prometheus" % "2.5.4",

      // HTTP server (for Prometheus /metrics endpoint)
      "dev.zio" %% "zio-http" % "3.11.6",

      // Cassandra driver (DataStax Java Driver 4.x)
      // — executeAsync per record, prepared statements per window
      "com.datastax.oss" % "java-driver-core"           % "4.17.0",
      "com.datastax.oss" % "java-driver-query-builder"  % "4.17.0",

      // ScalaPB runtime (EnrichedTelemetryEvent.parseFrom)
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

      // Discard signature files invalidated by assembling the JAR.
      case PathList("META-INF", xs @ _*)
          if xs.exists(x =>
            x.endsWith(".SF") ||
            x.endsWith(".DSA") ||
            x.endsWith(".RSA") ||
            x.endsWith(".EC")
          ) =>
        MergeStrategy.discard

      // Netty jars ship duplicate version metadata; pick one.
      case PathList("META-INF", "io.netty.versions.properties") =>
        MergeStrategy.first

      // INDEX.LIST is an optimization hint; safe to discard.
      case PathList("META-INF", "INDEX.LIST") =>
        MergeStrategy.discard

      // module-info.class files conflict across multi-release jars.
      case "module-info.class" =>
        MergeStrategy.discard
      case PathList("META-INF", "versions", _, "module-info.class") =>
        MergeStrategy.discard

      // Scala 3's scala-library and unroll-annotation both ship
      // scala/annotation/unroll.*
      case PathList("scala", "annotation", _*) =>
        MergeStrategy.first

      case "reference.conf" =>
        MergeStrategy.concat

      case "application.conf" =>
        MergeStrategy.concat

      case x if x.endsWith(".proto") =>
        MergeStrategy.deduplicate

      // Catch-all: first wins instead of erroring on differing contents.
      case x =>
        MergeStrategy.first
    },

    // ---- Runtime ----
    Compile / run / mainClass := Some("cityrover.Main"),
    run / fork := true,
    run / javaOptions ++= Seq(
      "-Xmx2G",
      "-XX:+UseG1GC"
    )
  )
