ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "cityrover"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cityrover-trajectory-lakehouse-writer-job",

    libraryDependencies ++= Seq(

      // ------------------------------------------------------------
      // Spark
      // ------------------------------------------------------------

      "org.apache.spark" %% "spark-core" % "3.5.4" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.4" % "provided",

      // ------------------------------------------------------------
      // Kafka / Structured Streaming
      // ------------------------------------------------------------

      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.4" % "provided",

      // ------------------------------------------------------------
      // Iceberg
      //
      // Spark runtime contains the Iceberg Spark integration.
      // ------------------------------------------------------------

      "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.6.0",
      "org.apache.iceberg" % "iceberg-aws-bundle" % "1.6.0",

      // ------------------------------------------------------------
      // Typesafe Config
      // ------------------------------------------------------------

      "com.typesafe" % "config" % "1.4.3",

      // ------------------------------------------------------------
      // Logging
      // ------------------------------------------------------------

      "ch.qos.logback" % "logback-classic" % "1.4.14"
    ),

    // --------------------------------------------------------------
    // Application entry point
    // --------------------------------------------------------------

    Compile / mainClass :=
      Some(
        "cityrover.spark.lakehouse.LakehouseWriterMain"
      ),

    // --------------------------------------------------------------
    // Assembly
    // --------------------------------------------------------------

    assembly / assemblyJarName :=
      "cityrover-trajectory-lakehouse-writer-job-assembly-0.1.0.jar",

    assembly / assemblyMergeStrategy := {

      case PathList("META-INF", _*) => MergeStrategy.discard
      case PathList("reference.conf") => MergeStrategy.concat
      case PathList("application.conf") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
