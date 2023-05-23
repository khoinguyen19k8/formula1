# Databricks notebook source
# MAGIC %md
# MAGIC # Import modules and read required Dataframes

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_funcs

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

race_result_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year") \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list = [race_year.race_year for race_year in race_result_list]

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(F.col("race_year").isin(race_year_list))

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

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

# MAGIC %md
# MAGIC # Write to presentation layer

# COMMAND ----------

merge_conditions = "tgt.race_year = upd.race_year AND tgt.team = upd.team"
merge_delta_data(final_constructor_standings_df, "f1_presentation", "constructor_standings", merge_conditions, "race_year")
