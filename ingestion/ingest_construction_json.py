# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

# COMMAND ----------

# MAGIC %run ../includes/configuration
# MAGIC %run ../includes/common_funcs

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read json file along with schema

# COMMAND ----------

constructors_schema = "constructorId INT, constructorREF STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(
    f"{raw_folder_path}/constructors.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename columns and add ingestion date

# COMMAND ----------

constructors_final_df = (
    constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorREF", "constructor_ref")
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
