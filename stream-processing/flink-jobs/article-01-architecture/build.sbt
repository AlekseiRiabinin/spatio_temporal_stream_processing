ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "phd.spatio.temporal"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .dependsOn(
    ProjectRef(file("../core"), "core")
  )
  .settings(
    name := "article-01-architecture",

    libraryDependencies ++= Seq(
      // Flink runtime (provided by cluster)
      "org.apache.flink" %% "flink-streaming-scala" % "1.17.1" % Provided,
      "org.apache.flink" %  "flink-clients" % "1.17.1" % Provided,

      // Kafka (provided by cluster)
      "org.apache.flink" % "flink-connector-kafka" % "3.0.0-1.17" % Provided,
      "org.apache.kafka" % "kafka-clients" % "3.7.0" % Provided,

      // Logging
      "org.slf4j" % "slf4j-simple" % "1.7.36",

      // Prometheus metrics
      "io.prometheus" % "simpleclient" % "0.16.0",
      "io.prometheus" % "simpleclient_hotspot" % "0.16.0",
      "io.prometheus" % "simpleclient_httpserver" % "0.16.0"
    ),

    // Exclude Flink, Kafka, and commons-collections from fat JAR
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { jar =>
        val name = jar.data.getName
        name.startsWith("flink-") ||
        name.startsWith("kafka-") ||
        name.startsWith("commons-collections")
      }
    },

    // Fat-jar for deployment
    assembly / assemblyJarName := "article-01-architecture.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )
