-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create tables for CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
  (
    circuitId INT,
    circuitRef STRING,
    name STRING,
    location STRING,
    country STRING,
    lat DOUBLE,
    lng DOUBLE,
    alt INT,
    url STRING
  )
USING csv
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Races tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
  (
    raceId INT,
    year INT,
    round INT,
    circuitId INT,
    name STRING,
    date STRING,
    time STRING,
    url STRING
  )
USING csv
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/races.csv", header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT, 
  constructorREF STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/constructors.json");

-- COMMAND ----------

SELECT * FROM f1_raw.constructors LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Drivers tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/drivers.json");

-- COMMAND ----------

SELECT * FROM f1_raw.drivers LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Results tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points DOUBLE,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING json
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pit stops tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/pit_stops.json", multiline true);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lap times tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Qualifying tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "/mnt/formula1dlkhoinguyen19k8/raw/qualifying", multiline true);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying LIMIT 100;
