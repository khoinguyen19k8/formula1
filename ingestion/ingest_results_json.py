# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_funcs

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC # Define schema

# COMMAND ----------

results_schema = StructType(
    [
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), False),
        StructField("positionOrder", IntegerType(), False),
        StructField("points", DoubleType(), False),
        StructField("laps", IntegerType(), False),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("statusId", IntegerType(), False),
    ]
)

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(
    f"{raw_folder_path}/{v_file_date}/results.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns

# COMMAND ----------

results_transformed_df = add_ingestion_date(
    results_df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop unneeded columns

# COMMAND ----------

results_final_df = results_transformed_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to table

# COMMAND ----------

merge_conditions = "tgt.result_id = upd.result_id"
merge_delta_data(results_final_df, "f1_processed", "results", merge_conditions, "race_id")
