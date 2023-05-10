# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops JSON file

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import current_timestamp, col

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

pit_stops_df = spark.read.schema(pit_stops_schema).json("dbfs:/mnt/formula1dlkhoinguyen19k8/raw/pit_stops.json", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("dbfs:/mnt/formula1dlkhoinguyen19k8/processed/pit_stops/")
