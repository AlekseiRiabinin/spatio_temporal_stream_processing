ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "phd.spatio.temporal"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val core = (project in file("."))
  .settings(
    name := "flink-spatio-temporal-core",

    libraryDependencies ++= Seq(
      // Flink APIs (provided by runtime)
      "org.apache.flink" %% "flink-streaming-scala" % "1.17.1" % Provided,
      "org.apache.flink" %% "flink-scala" % "1.17.1" % Provided,

      // Spatial foundations
      "org.locationtech.jts" % "jts-core" % "1.19.0",
      "org.locationtech.spatial4j" % "spatial4j" % "0.8"
        exclude("commons-collections", "commons-collections"),

      // Math
      "org.apache.commons" % "commons-math3" % "3.6.1",

      // Logging API only
      "org.slf4j" % "slf4j-api" % "1.7.36"
    )
  )
