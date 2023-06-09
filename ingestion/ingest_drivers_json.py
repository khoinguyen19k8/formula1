# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

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
    DateType,
)
from pyspark.sql.functions import col, to_timestamp, lit, concat

# COMMAND ----------

name_schema = StructType(
    [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)
drivers_schema = StructType(
    [
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add new columns

# COMMAND ----------

drivers_transformed_df = add_ingestion_date(
    drivers_df.withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_transformed_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")
