# Databricks notebook source
# MAGIC %python
# MAGIC covid_raw_data = spark.read.format('csv').option('header', True).option('inferSchema', True).load('/FileStore/shared_uploads/4shivaraj.gowda@gmail.com/covid_data.csv')

# COMMAND ----------

# MAGIC %python
# MAGIC display(covid_raw_data)

# COMMAND ----------

# MAGIC %python
# MAGIC covid_raw_data.count()

# COMMAND ----------

# MAGIC %python
# MAGIC covid_remove_duplicates = covid_raw_data.drop_duplicates()

# COMMAND ----------

# MAGIC %python
# MAGIC covid_remove_duplicates.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC covid_selected_columns = covid_remove_duplicates.select('iso_code','location','continent','date' ,'new_deaths_per_million','people_fully_vaccinated', 'population' )

# COMMAND ----------

# MAGIC %python
# MAGIC covid_clean_data = covid_selected_columns.na.drop()

# COMMAND ----------

# MAGIC %python
# MAGIC covid_clean_data.count()

# COMMAND ----------

# MAGIC %python
# MAGIC covid_clean_data.createOrReplaceTempView('covid_view')

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT iso_code, location, continent,
# MAGIC SUM(new_deaths_per_million) AS death_sum,
# MAGIC MAX(people_fully_vaccinated * 100 / population) AS percentage_vaccinated FROM covid_view
# MAGIC WHERE population > 1000000
# MAGIC GROUP BY iso_code, location, continent
# MAGIC ORDER BY death_sum DESC

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

dbutils.ls('/')

# COMMAND ----------


