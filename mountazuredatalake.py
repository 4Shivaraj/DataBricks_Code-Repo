# Databricks notebook source
# MAGIC %scala
# MAGIC val appsecret = dbutils.secrets.get(scope="datalakekey98", key="appsecret")

# COMMAND ----------

# MAGIC %scala
# MAGIC val ApplicationID = dbutils.secrets.get(scope="datalakekey98", key="ApplicationID")

# COMMAND ----------

# MAGIC %scala
# MAGIC val DirectoryID = dbutils.secrets.get(scope="datalakekey98", key="DirectoryID")

# COMMAND ----------

# MAGIC %scala
# MAGIC val endpoint = "https://login.microsoftonline.com/" + DirectoryID + "/oauth2/token"

# COMMAND ----------

# MAGIC %scala
# MAGIC val configs = Map(
# MAGIC   "fs.azure.account.auth.type" -> "OAuth",
# MAGIC "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC "fs.azure.account.oauth2.client.id" -> ApplicationID,
# MAGIC "fs.azure.account.oauth2.client.secret" -> appsecret,
# MAGIC "fs.azure.account.oauth2.client.endpoint" -> endpoint)

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mount(
# MAGIC source = "abfss://databricks@98storageaccount.dfs.core.windows.net/", mountPoint = "/mnt/datalakestorage", extraConfigs = configs)

# COMMAND ----------

sp
