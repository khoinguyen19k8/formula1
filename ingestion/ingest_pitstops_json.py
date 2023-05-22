# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops JSON file

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

pit_stops_schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("stop", IntegerType(), False),
        StructField("lap", IntegerType(), False),
        StructField("time", StringType(), False),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).json(
    f"{raw_folder_path}/{v_file_date}/pit_stops.json", multiLine=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(
    pit_stops_df \
        .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to table

# COMMAND ----------

merge_conditions = "tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id AND tgt.stop = upd.stop"
merge_delta_data(pit_stops_final_df, "f1_processed", "pit_stops", merge_conditions, "race_id")
