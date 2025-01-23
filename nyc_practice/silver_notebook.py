# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Acess

# COMMAND ----------

client_Id = "19999b06-de00-4a19-98ce-09d0e3cd3f7a"
directory_Id = "35691d70-ac1c-4bd1-8f57-a2d546f2adff"
secret_Id = "0b58Q~ANkZaFbXVz5ZIfoxLvz1dvVrip6JRFQb~a"

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.trafficsignalstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.trafficsignalstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.trafficsignalstorage.dfs.core.windows.net", "19999b06-de00-4a19-98ce-09d0e3cd3f7a")
spark.conf.set("fs.azure.account.oauth2.client.secret.trafficsignalstorage.dfs.core.windows.net", "0b58Q~ANkZaFbXVz5ZIfoxLvz1dvVrip6JRFQb~a")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.trafficsignalstorage.dfs.core.windows.net", "https://login.microsoftonline.com/35691d70-ac1c-4bd1-8f57-a2d546f2adff/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@trafficsignalstorage.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Type_data

# COMMAND ----------

df_type=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@trafficsignalstorage.dfs.core.windows.net/trip_type")
df_type.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Zone Data

# COMMAND ----------

df_zone=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@trafficsignalstorage.dfs.core.windows.net/trip_zone")
df_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Trip Data

# COMMAND ----------

myschema = '''
                VendorID BIGINT,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag STRING,
                RatecodeID BIGINT,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                passenger_count BIGINT,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type BIGINT,
                trip_type BIGINT,
                congestion_surcharge DOUBLE

      '''

# COMMAND ----------

df_trip=spark.read.format("parquet")\
                      .option("header","true")\
                      .schema(myschema)\
                      .option("recursiveFileLookup","True")\
                      .load("abfss://bronze@trafficsignalstorage.dfs.core.windows.net/raw23/")
df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Tranformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip_type

# COMMAND ----------

df_type=df_type.withColumnRenamed("description","Trip_description")
df_type.display()

# COMMAND ----------

df_type.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@trafficsignalstorage.dfs.core.windows.net/trip_type")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip_zone

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone=df_zone.withColumn("Zone1",split(col("Zone"),"/")[0])\
               .withColumn("Zone2",split(col("Zone"),"/")[1])

# COMMAND ----------

df_zone.show(5)

# COMMAND ----------

df_zone.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@trafficsignalstorage.dfs.core.windows.net/trip_zone")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip_data

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
                  .withColumn('trip_year',year('lpep_pickup_datetime'))\
                  .withColumn('trip_month',month('lpep_pickup_datetime'))
                  

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','fare_amount','total_amount')
df_trip.display()

# COMMAND ----------

df_zone.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@trafficsignalstorage.dfs.core.windows.net/trip_2023data")\
            .save()

# COMMAND ----------

