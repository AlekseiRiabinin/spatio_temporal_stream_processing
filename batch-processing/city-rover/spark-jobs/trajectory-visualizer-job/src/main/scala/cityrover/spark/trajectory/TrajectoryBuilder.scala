package cityrover.spark.trajectory

import org.apache.spark.sql.{DataFrame, functions => F}


object TrajectoryBuilder {

  def build(df: DataFrame): DataFrame = {

    val sortedDf = df
      .withColumn("coord", F.array(F.col("lon"), F.col("lat")))
      .groupBy("roverId")
      .agg(
        F.sort_array(
          F.collect_list(
            F.struct(
              F.col("ts"),
              F.col("lat"),
              F.col("lon"),
              F.col("speed"),
              F.col("heading"),
              F.col("edgeId"),
              F.col("routeId"),
              F.col("coord")
            )
          )
        ).as("sorted")
      )

    sortedDf
      // Existing GeoJSON support
      .withColumn(
        "coords",
        F.transform(
          F.col("sorted"),
          x => x.getField("coord")
        )
      )

      // Replay support
      .withColumn(
        "positions",
        F.transform(
          F.col("sorted"),
          x =>
            F.struct(
              x.getField("ts").as("ts"),
              x.getField("lat").as("lat"),
              x.getField("lon").as("lon"),
              x.getField("speed").as("speed"),
              x.getField("heading").as("heading"),
              x.getField("edgeId").as("edgeId"),
              x.getField("routeId").as("routeId")
            )
        )
      )

      .select(
        "roverId",
        "coords",
        "positions"
      )
  }
}
