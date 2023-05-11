# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

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
# MAGIC # Define the schema and read data

# COMMAND ----------

qualifying_schema = StructType(
    [
        StructField("qualifyId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    ]
)

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(f"{raw_folder_path}/qualifying", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

qualifying_transformed_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

qualifying_final_df = qualifying_transformed_df

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying/")
