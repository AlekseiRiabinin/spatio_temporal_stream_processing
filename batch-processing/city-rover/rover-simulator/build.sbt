name := "cityrover-rover-simulator"

version := "0.1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // JSON serialization (circe)
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",

  // Kafka producer API
  "org.apache.kafka" % "kafka-clients" % "3.8.0",

  // Config loader
  "com.typesafe" % "config" % "1.4.3",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.14"
)

fork := true

mainClass in Compile := Some("cityrover.sim.runtime.RoverSimulatorMain")
