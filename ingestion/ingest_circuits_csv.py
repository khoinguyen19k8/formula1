# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType([
    StructField('circuitId', IntegerType(), False), 
    StructField('circuitRef', StringType(), True),
    StructField('name', StringType(), True), 
    StructField('location', StringType(), True), 
    StructField('country', StringType(), True), 
    StructField('lat', DoubleType(), True), 
    StructField('lng', DoubleType(), True), 
    StructField('alt', IntegerType(), True), 
    StructField('url', StringType(), True)
])

# COMMAND ----------

circuits_df = spark \
    .read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv("dbfs:/mnt/formula1dlkhoinguyen19k8/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the column as required

# COMMAND ----------

circuits_selected_df.columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC # Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to datalake as parquet

# COMMAND ----------

circuits_final_df \
    .write \
    .parquet("dbfs:/mnt/formula1dlkhoinguyen19k8/processed/circuits", mode="overwrite")
