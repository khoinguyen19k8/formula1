# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
)
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, concat

# COMMAND ----------

name_schema = StructType(
    [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)
drivers_schema = StructType(
    [
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------


