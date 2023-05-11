# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
)
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, concat

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
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(
    f"{raw_folder_path}/drivers.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add new columns

# COMMAND ----------

drivers_transformed_df = (
    drivers_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorREF", "constructor_ref")
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers/")
