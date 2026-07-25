package cityrover.spark.trajectory

import org.apache.spark.sql.{DataFrame, functions => F}


object TrajectoryBuilder {

  def build(df: DataFrame): DataFrame = {
    df
      .withColumn("coord", F.array(F.col("lon"), F.col("lat")))
      .groupBy("roverId")
      .agg(
        F.sort_array(
          F.collect_list(
            F.struct(F.col("ts"), F.col("coord"))
          )
        ).as("sorted")
      )
      .withColumn("coords", F.transform(F.col("sorted"), x => x.getField("coord")))
      .select("roverId", "coords")
  }
}
