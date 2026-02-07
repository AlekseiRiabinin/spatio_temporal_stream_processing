ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "article-01-architecture",

    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-streaming-scala" % "1.17.1" % "provided",
      "org.apache.flink" % "flink-clients" % "1.17.1"
    ),

    assembly / assemblyJarName := "article-01-architecture.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
