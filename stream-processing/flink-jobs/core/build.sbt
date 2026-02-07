lazy val core = (project in file("."))
  .settings(
    name := "flink-spatio-temporal-core",
    version := "0.1.0",
    scalaVersion := "2.12.20",

    libraryDependencies ++= Seq(
      // Flink API (используется в job'ах, не пакуем в fat-jar)
      "org.apache.flink" %% "flink-streaming-scala" % "1.17.1" % "provided",
      "org.apache.flink" %% "flink-scala" % "1.17.1" % "provided",

      // Spatial stack (GeoFlink foundation)
      "org.locationtech.jts" % "jts-core" % "1.19.0",
      "org.locationtech.spatial4j" % "spatial4j" % "0.8",
      "org.apache.commons" % "commons-math3" % "3.6.1",

      // Logging
      "org.slf4j" % "slf4j-api" % "1.7.36"
    )
  )
