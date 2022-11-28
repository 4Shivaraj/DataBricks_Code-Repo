-- Databricks notebook source
CREATE DATABASE covid

-- COMMAND ----------

CREATE TEMPORARY VIEW covid_data
USING csv
OPTIONS (path '/FileStore/shared_uploads/4shivaraj.gowda@gmail.com/covid_data.csv', header 'true', mode 'FAILFAST')

-- COMMAND ----------

CREATE OR REPLACE TABLE covid.covid_data_delta
USING DELTA
LOCATION '/FileStore/shared_uploads/4shivaraj.gowda@gmail.com/covid_data_delta'
AS
SELECT iso_code, location, continent, date, new_deaths_per_million, people_fully_vaccinated, population FROM covid_data

-- COMMAND ----------

DELETE FROM covid.covid_data_delta WHERE population IS null OR people_fully_vaccinated IS null OR new_deaths_per_million IS null OR location IS null

-- COMMAND ----------

SELECT COUNT(*) FROM covid.covid_data_delta

-- COMMAND ----------

DELETE FROM covid.covid_data_delta

-- COMMAND ----------

SELECT * FROM covid.covid_data_delta VERSION AS OF 0;

-- COMMAND ----------

RESTORE TABLE covid.covid_data_delta TO VERSION AS OF 0

-- COMMAND ----------

UPDATE covid.covid_data_delta SET population = population * 1.2
WHERE continent = 'Asia';


-- COMMAND ----------

DELETE FROM covid.covid_data_delta WHERE continent = 'Europe';

-- COMMAND ----------


