# Databricks notebook source
# MAGIC %md
# MAGIC # Import functions

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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
    TimestampType,
)
from pyspark.sql.functions import col, to_timestamp, lit, concat

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
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Rename columns

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
).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC # Add ingestion date

# COMMAND ----------

races_final_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")
