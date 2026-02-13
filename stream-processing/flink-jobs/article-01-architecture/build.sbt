ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "phd.spatio.temporal"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .dependsOn(
    // Optional dependency: only use what is stable
    ProjectRef(file("../core"), "core")
  )
  .settings(
    name := "article-01-architecture",

    libraryDependencies ++= Seq(
      // Flink runtime
      "org.apache.flink" %% "flink-streaming-scala" % "1.17.1" % Provided,
      "org.apache.flink" %  "flink-clients" % "1.17.1",

      // Kafka (architecture paper needs event source)
      "org.apache.flink" % "flink-connector-kafka" % "3.0.0-1.17",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",

      // Logging binding (job-level decision)
      "org.slf4j" % "slf4j-simple" % "1.7.36",

      // HTTP server for metrics
      "org.nanohttpd" % "nanohttpd" % "2.3.1"

    ),

    // Fat-jar for deployment
    assembly / assemblyJarName := "article-01-architecture.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x                        => MergeStrategy.first
    }
  )
