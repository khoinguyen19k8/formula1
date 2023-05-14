-- Databricks notebook source
SHOW TABLES IN f1_presentation

-- COMMAND ----------

SELECT
  team,
  SUM(total_points) AS cumulative_points,
  SUM(wins) AS cumulative_wins,
  cumulative_points / cumulative_wins AS avg_points_per_win
FROM 
  f1_presentation.constructor_standings
WHERE
  race_year BETWEEN 2000 AND 2020
GROUP BY
  team
HAVING
  SUM(total_points) >= 10 AND SUM(wins) >= 3
ORDER BY avg_points_per_win DESC;

-- COMMAND ----------

SELECT
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) total_points,
  AVG(calculated_points) avg_points  
FROM
  f1_presentation.calculated_race_results
WHERE
  race_year BETWEEN 2000 AND 2020
GROUP BY
  team_name
HAVING
  COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) total_points,
  AVG(calculated_points) avg_points  
FROM
  f1_presentation.calculated_race_results
WHERE
  race_year BETWEEN 2011 AND 2020
GROUP BY
  team_name
HAVING
  COUNT(1) >= 100
ORDER BY avg_points DESC
