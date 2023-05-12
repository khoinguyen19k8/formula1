# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(
        F.sum(F.col("points")).alias("total_points"), 
        F.count(F.when(F.col("position") == 1, True)).alias("wins")
    )

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(F.desc("total_points"), F.desc("wins"))
final_driver_standings_df = driver_standings_df.withColumn("rank", F.rank().over(driverRankSpec))

# COMMAND ----------

display(final_driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

final_driver_standings_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
