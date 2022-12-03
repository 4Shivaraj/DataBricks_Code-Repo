# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://azuredatabricks@98storageaccount.blob.core.windows.net/',
mount_point = "/mnt/blobstorage001",
extra_configs = {'fs.azure.account.key.98storageaccount.blob.core.windows.net': '9jnZMmDZFNO3JbAXt7porphCM3NapuIp64mnqEhsuLmhMRwx6YBmiRQJKtFZJq4sppGTbPTPgi3v+ASteDg20A=='})

# COMMAND ----------

dbutils.fs.ls("/mnt/blobstorage001")

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

input_schema = StructType([
            StructField('index', StringType(), nullable=True),
            StructField('tweet_id', StringType(), nullable=True),
            StructField('name', StringType(), nullable=True),
            StructField('tweets', StringType(), nullable=True),
            StructField('followers', IntegerType(), nullable=True),
            StructField('friends', IntegerType(), nullable=True),
            StructField('verified', StringType(), nullable=True),
            StructField('total_post', IntegerType(), nullable=True),
            StructField('profile_image', StringType(), nullable=True),
            StructField('location', StringType(), nullable=True)])

# COMMAND ----------

tweet_df = spark.read.format('csv') \
            .option('header', True) \
            .schema(input_schema) \
            .option('path', '/mnt/blobstorage001/input/tweet_data.csv') \
            .load()
tweet_df.printSchema()
tweet_df.show()

# COMMAND ----------

df = tweet_df.drop('index')
print(df.describe().show())
print(f"Twitter dataset: \n{df.show()}")

# COMMAND ----------

select_df = tweet_df.select('tweet_id', 'followers', 'friends', 'tweets', 'total_post', 'location')
select_df.show()

# COMMAND ----------

final_df = select_df.fillna(value='Not available', subset=['location'])
final_df.show()

# COMMAND ----------

filter_df = final_df.filter(final_df.total_post > 15000)
filter_df.show()

# COMMAND ----------

filter_df = final_df.filter(final_df.location == 'Not available' )
filter_df.show()

# COMMAND ----------

group_by_df = final_df.groupBy('tweet_id', 'location')
print(f"Group by: \n{group_by_df.count().show(200)}")

# COMMAND ----------

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import size, split, col, explode, count
import pyspark.sql.functions as F

# COMMAND ----------

final_df = final_df.withColumn('word_count', F.size(F.split(F.col('tweets'), ' ')))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.coalesce(1).write.mode('overwrite').option('header', True).csv('/mnt/blobstorage001/output/')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS twitter

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW twitter_data
# MAGIC USING CSV
# MAGIC OPTIONS (path '/mnt/blobstorage001/output/part-00000-tid-4996392688400369143-30324df7-e63c-4150-95ef-4d8217f75038-47-1-c000.csv', header 'true', mode 'FAILFAST')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE twitter.twitter_data_delta
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/user/hive/'
# MAGIC AS
# MAGIC SELECT tweet_id, followers, friends, tweets, total_post, location, word_count FROM twitter_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM twitter.twitter_data_delta WHERE location  == 'Not available'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM twitter_data_delta

# COMMAND ----------


