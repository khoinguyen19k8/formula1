# Databricks notebook source
# MAGIC %md
# MAGIC # Load constant data

# COMMAND ----------

 dbutils.notebook.run("./ingest_circuits_csv", 0, {"p_file_date" : "2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("./ingest_constructors_json", 0, {"p_file_date" : "2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("./ingest_drivers_json", 0, {"p_file_date" : "2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("./ingest_races_csv", 0, {"p_file_date" : "2021-03-21"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Load incremental data

# COMMAND ----------

date_ids = ["2021-03-21", "2021-03-28", "2021-04-18"]

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./ingest_results_json", 0, {"p_file_date" : dt_id})

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./ingest_lap_times_csv", 0, {"p_file_date" : dt_id})

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./ingest_pitstops_json", 0, {"p_file_date" : dt_id})

# COMMAND ----------

for dt_id in date_ids:
    dbutils.notebook.run("./ingest_qualifying_json", 0, {"p_file_date" : dt_id})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   race_id, COUNT(1)
# MAGIC FROM
# MAGIC   f1_processed.results
# MAGIC GROUP BY
# MAGIC   race_id
# MAGIC ORDER BY
# MAGIC   race_id DESC;
