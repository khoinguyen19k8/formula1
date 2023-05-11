# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops JSON file

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

pit_stops_schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("stop", IntegerType(), False),
        StructField("lap", IntegerType(), False),
        StructField("time", StringType(), False),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).json(
    f"{raw_folder_path}/pit_stops.json", multiLine=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(
    pit_stops_df.withColumnRenamed("raceId", "race_id").withColumnRenamed(
        "driverId", "driver_id"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet(
    f"{processed_folder_path}/pit_stops/"
)
