# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_funcs

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read json file along with schema

# COMMAND ----------

constructors_schema = "constructorId INT, constructorREF STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(
    f"{raw_folder_path}/{v_file_date}/constructors.json"
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

constructors_final_df = add_ingestion_date(
    constructors_dropped_df
        .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("constructorREF", "constructor_ref") \
        .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
