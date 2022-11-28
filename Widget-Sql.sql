-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Dropdown widget

-- COMMAND ----------

CREATE WIDGET DROPDOWN dropdownwidget DEFAULT '1' CHOICES SELECT '1' UNION ALL SELECT '2'

-- COMMAND ----------

CREATE WIDGET TEXT textwidget DEFAULT 'NA'

-- COMMAND ----------

CREATE WIDGET COMBOBOX combowidget DEFAULT '1' CHOICES SELECT '1' UNION ALL SELECT '2'

-- COMMAND ----------

CREATE WIDGET MULTISELECT multiselectwidge DEFAULT '1' CHOICES SELECT '1' UNION ALL SELECT '2'

-- COMMAND ----------

SELECT getArgument('combowidget')

-- COMMAND ----------

REMOVE widget dropdownwidget

-- COMMAND ----------


