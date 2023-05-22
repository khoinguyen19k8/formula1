# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
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
)
from pyspark.sql.functions import col, concat, lit

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

qualifying_df = spark.read.schema(qualifying_schema).json(
    f"{raw_folder_path}/{v_file_date}/qualifying", multiLine=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

qualifying_transformed_df = add_ingestion_date(
    qualifying_df.withColumnRenamed("qualifyId", "qualify_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to table

# COMMAND ----------

qualifying_final_df = qualifying_transformed_df

# COMMAND ----------

merge_conditions = "tgt.qualify_id = upd.qualify_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", merge_conditions, "race_id")
