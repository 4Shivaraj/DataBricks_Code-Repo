-- Databricks notebook source
-- MAGIC %scala
-- MAGIC val diamonds = spark.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC diamonds.createOrReplaceTempView("diamonds_view")

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS PowerBI

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE PowerBI.Diamond_Insights_PowerBI
-- MAGIC USING DELTA
-- MAGIC AS
-- MAGIC Select *
-- MAGIC From diamonds_view
