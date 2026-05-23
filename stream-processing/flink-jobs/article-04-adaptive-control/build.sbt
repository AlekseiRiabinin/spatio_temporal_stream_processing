val scala3Version = "3.8.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "article-04-adaptive-control",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.3.0" % Test
  )
