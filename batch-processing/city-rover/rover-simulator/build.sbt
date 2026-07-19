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
  "ch.qos.logback" % "logback-classic" % "1.4.14",

  // Parquet + Avro (must match graph-engine)
  "org.apache.parquet" % "parquet-avro" % "1.13.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.13.1",
  "org.apache.avro" % "avro" % "1.11.3",

  // Hadoop FS
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",

  // JTS geometry
  "org.locationtech.jts" % "jts-core" % "1.19.0"
)

fork := true

mainClass in Compile := Some("cityrover.sim.runtime.RoverSimulatorMain")

// ---------------------------------------------------------------------------
// Resolve Kafka vs Parquet zstd-jni conflict
// ---------------------------------------------------------------------------
dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-3"
