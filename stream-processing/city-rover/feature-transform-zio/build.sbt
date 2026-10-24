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
    // Generate Scala sources from .proto files in src/main/protobuf.
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
      // Verify resolved kafka-clients version and broker compatibility.
      "dev.zio" %% "zio-kafka" % "3.0.0",

      // Configuration
      "dev.zio" %% "zio-config" % "4.0.0",
      "dev.zio" %% "zio-config-typesafe" % "4.0.0",
      "dev.zio" %% "zio-config-magnolia" % "4.0.0",

      // Prometheus metrics
      "dev.zio" %% "zio-metrics-connectors-prometheus" % "2.5.4",

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

      // Discard signature files invalidated by assembling the JAR.
      case PathList("META-INF", xs @ _*)
          if xs.exists(x =>
            x.endsWith(".SF") ||
            x.endsWith(".DSA") ||
            x.endsWith(".RSA") ||
            x.endsWith(".EC")
          ) =>
        MergeStrategy.discard

      // Preserve configuration contributed by dependencies.
      case "reference.conf" =>
        MergeStrategy.concat

      case "application.conf" =>
        MergeStrategy.concat

      // Protobuf definitions may be duplicated across dependencies.
      case x if x.endsWith(".proto") =>
        MergeStrategy.deduplicate

      // Avoid silently choosing arbitrary duplicate classes/resources.
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
