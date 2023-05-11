# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times folder

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
from pyspark.sql.functions import col

# COMMAND ----------

lap_times_schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("lap", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

lap_times_final_df = add_ingestion_date(
    lap_times_df.withColumnRenamed("raceId", "race_id").withColumnRenamed(
        "driverId", "driver_id"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(
    f"{processed_folder_path}/lap_times/"
)
