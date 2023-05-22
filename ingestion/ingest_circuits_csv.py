# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

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
)

# COMMAND ----------

circuits_schema = StructType(
    [
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

circuits_df = (
    spark.read.option("header", True)
    .schema(circuits_schema)
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the column as required

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitRef", "circuit_ref")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("lng", "longitude")
    .withColumnRenamed("alt", "altitude")
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
