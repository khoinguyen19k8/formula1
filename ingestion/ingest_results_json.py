# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

# MAGIC %run ../includes/configuration
# MAGIC %run ../includes/common_funcs

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import current_timestamp, col, concat

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
    f"{raw_folder_path}/results.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns

# COMMAND ----------

results_transformed_df = (
    results_df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop unneeded columns

# COMMAND ----------

results_final_df = results_transformed_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(
    f"{processed_folder_path}/results/"
)
