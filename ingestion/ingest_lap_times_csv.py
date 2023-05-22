# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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
from pyspark.sql.functions import col, lit

# COMMAND ----------

lap_times_schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("lap", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

lap_times_final_df = add_ingestion_date(
    lap_times_df \
        .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to table

# COMMAND ----------

merge_conditions = "tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.lap = upd.lap"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", merge_conditions , "race_id")
