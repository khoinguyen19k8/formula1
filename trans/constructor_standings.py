# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(
        F.sum("points").alias("total_points"),
        F.count(F.when(F.col("position") == 1, True)).alias("wins")
    )

# COMMAND ----------

constructorRank_spec = Window \
    .partitionBy("race_year") \
    .orderBy(
        F.desc("total_points"), 
        F.desc("wins")
    )
final_constructor_standings_df = constructor_standings_df.withColumn("rank", F.rank().over(constructorRank_spec))

# COMMAND ----------

display(final_constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

final_constructor_standings_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
