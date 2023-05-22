# Databricks notebook source
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

# COMMAND ----------

raw_folder_path = "dbfs:/mnt/formula1dlkhoinguyen19k8/raw"
processed_folder_path = "dbfs:/mnt/formula1dlkhoinguyen19k8/processed"
presentation_folder_path = "dbfs:/mnt/formula1dlkhoinguyen19k8/presentation"
