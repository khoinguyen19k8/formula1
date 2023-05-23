# Databricks notebook source
date_ids = ["2021-03-21", "2021-03-28", "2021-04-18"]

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./race_results", 0, {"p_file_date" : dt_id})

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./driver_standings", 0, {"p_file_date" : dt_id})

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./constructor_standings", 0, {"p_file_date" : dt_id})

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./calculated_race_results", 0, {"p_file_date" : dt_id})
