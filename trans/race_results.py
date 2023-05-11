# Databricks notebook source
# MAGIC %md
# MAGIC # Import modules and read required Dataframes

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_funcs

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

# COMMAND ----------

results_racesDriversConstructorsCircuits_df = results_df.join(races_df, results_df["race_id"] == races_df["race_id"]) \
    .join(drivers_df, results_df["driver_id"] == drivers_df["driver_id"]) \
    .join(constructors_df, results_df["constructor_id"] == constructors_df["constructor_id"]) \
    .join(circuits_df, races_df["circuit_id"] == circuits_df["circuit_id"]) \
    .select(
        races_df["race_year"], races_df["name"].alias("race_name"), races_df["race_timestamp"].alias("race_date"),
        circuits_df["location"].alias("circuit_location"),
        drivers_df["name"].alias("driver_name"), drivers_df["number"].alias("driver_number"), drivers_df["nationality"].alias("driver_nationality"),
        constructors_df["name"].alias("team"),
        results_df["grid"], results_df["fastest_lap"], results_df["time"].alias("race_time"), results_df["points"]   
    )

# COMMAND ----------

display(results_racesDriversConstructorsCircuits_df.filter(
    (F.col("race_year") == 2020) & \
    (F.col("race_name").contains("Abu Dhabi Grand Prix")) \
    ).orderBy(F.col("points").desc())
)

# COMMAND ----------

raceResults_report_df = add_ingestion_date(results_racesDriversConstructorsCircuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to presentation layer

# COMMAND ----------

raceResults_report_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
