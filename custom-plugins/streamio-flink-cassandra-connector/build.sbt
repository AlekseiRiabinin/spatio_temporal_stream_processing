ThisBuild / scalaVersion := "3.3.8"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "streamio.flink.cassandra"

// ============================================================
// Dependency versions
// ============================================================

lazy val flinkVersion            = "2.3.0"
lazy val cassandraDriverVersion  = "4.17.0"
lazy val scalatestVersion        = "3.2.19"

// ============================================================
// Project
// ============================================================

lazy val root = (project in file("."))
  .settings(

    name := "streamio-flink-cassandra-connector",

    // ========================================================
    // Dependencies
    // ========================================================

    libraryDependencies ++= Seq(

      // ------------------------------------------------------
      // Flink Table API (required for DynamicTableFactory)
      // ------------------------------------------------------
      "org.apache.flink" % "flink-table-common"    % flinkVersion % Provided,
      "org.apache.flink" % "flink-table-api-java"  % flinkVersion % Provided,
      "org.apache.flink" % "flink-table-runtime"   % flinkVersion % Provided,

      // ------------------------------------------------------
      // Flink Streaming API (for optional DataStream sink)
      // ------------------------------------------------------
      "org.apache.flink" % "flink-streaming-java"  % flinkVersion % Provided,

      // ------------------------------------------------------
      // Cassandra Java Driver (DataStax Java Driver 4.x)
      // ------------------------------------------------------
      "com.datastax.oss" % "java-driver-core"           % cassandraDriverVersion,
      "com.datastax.oss" % "java-driver-query-builder"  % cassandraDriverVersion,
      "com.datastax.oss" % "java-driver-mapper-runtime" % cassandraDriverVersion,

      // ------------------------------------------------------
      // Tests
      // ------------------------------------------------------
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),

    // ========================================================
    // Assembly
    // ========================================================

    // We need META-INF/services to be preserved for Flink SPI discovery
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) =>
        MergeStrategy.concat

      // Drop other META-INF junk
      case PathList("META-INF", _*) =>
        MergeStrategy.discard

      case _ =>
        MergeStrategy.first
    }
  )
