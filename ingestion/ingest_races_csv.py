# Databricks notebook source
# MAGIC %md
# MAGIC # Import functions

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, concat

# COMMAND ----------

# MAGIC %md
# MAGIC # Read raw csv

# COMMAND ----------

races_schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

races_df = (
    spark.read.option("header", True)
    .schema(races_schema)
    .csv("dbfs:/mnt/formula1dlkhoinguyen19k8/raw/races.csv")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Rename columns

# COMMAND ----------

races_df.columns

# COMMAND ----------

races_renamed_df = (
    races_df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("year", "race_year")
    .withColumnRenamed("circuitId", "circuit_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform columns

# COMMAND ----------

races_transformed_df = races_renamed_df.withColumn(
    "race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select necessary columns 

# COMMAND ----------

races_selected_df = races_transformed_df.select(
    col("race_id"),
    col("race_year"),
    col("round"),
    col("circuit_id"),
    col("name"),
    col("race_timestamp"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add ingestion date

# COMMAND ----------

races_final_df = races_selected_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

races_final_df.write.partitionBy("race_year").parquet(
    "dbfs:/mnt/formula1dlkhoinguyen19k8/processed/races", mode="overwrite"
)
