# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
          (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
          )
          USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW race_result_updated
        AS
        SELECT
            races.race_year,
            constructors.name AS team_name,
            drivers.driver_id,
            drivers.name AS driver_name,
            races.race_id,
            results.position,
            results.points,
            11 - results.position AS calculated_points
        FROM
            f1_processed.results
        JOIN f1_processed.races ON (results.race_id = races.race_id)
        JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
        JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
        JOIN f1_processed.circuits ON (races.circuit_id = circuits.circuit_id)
        WHERE 
            results.position <= 10
            AND
            results.file_date = '{v_file_date}';
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results AS tgt
# MAGIC USING race_result_updated upd
# MAGIC ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.position = upd.position,
# MAGIC     tgt.points = upd.points,
# MAGIC     tgt.calculated_points = upd.calculated_points,
# MAGIC     tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     race_id,
# MAGIC     position,
# MAGIC     points,
# MAGIC     calculated_points,
# MAGIC     created_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.race_year,
# MAGIC     upd.team_name,
# MAGIC     upd.driver_id,
# MAGIC     upd.driver_name,
# MAGIC     upd.race_id,
# MAGIC     upd.position,
# MAGIC     upd.points,
# MAGIC     upd.calculated_points,
# MAGIC     current_timestamp
# MAGIC   );
