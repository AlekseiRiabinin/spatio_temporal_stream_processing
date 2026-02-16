name := "article-01-geo-producer"

version := "0.1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.8.0",
  "ch.qos.logback" % "logback-classic" % "1.4.14"
)

Compile / mainClass := Some("phd.architecture.producer.GeoEventProducer")

assembly / assemblyJarName := "geo-producer.jar"

ThisBuild / assemblyMergeStrategy := {
  case x if x.endsWith("module-info.class") => MergeStrategy.discard
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}
