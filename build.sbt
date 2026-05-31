lazy val root = (project in file("."))
  .aggregate(
    core,
    article01,
    article02,
    article03,
    article04
  )
  .settings(
    name := "spatio-temporal-stream-processing"
  )

lazy val core = (project in file("stream-processing/flink-jobs/core"))

lazy val article01 = (project in file("stream-processing/flink-jobs/article-01-architecture"))
  .dependsOn(core)

lazy val article02 = (project in file("stream-processing/flink-jobs/article-02-stream-models"))
  .dependsOn(core)

lazy val article03 = (project in file("stream-processing/flink-jobs/article-03-spatial-methods"))
  .dependsOn(core)

lazy val article04 = (project in file("stream-processing/flink-jobs/article-04-adaptive-control"))
  .dependsOn(core)
