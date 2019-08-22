# Databricks notebook source
# MAGIC %md # Context of Data
# MAGIC Company - UK-based and registered non-store online retail
# MAGIC 
# MAGIC Products for selling - Mainly all-occasion gifts
# MAGIC 
# MAGIC Customers - Most are wholesalers (local or international)
# MAGIC 
# MAGIC Transactions Period - **1st Dec 2010 - 9th Dec 2011 (One year)**

# COMMAND ----------


# Add the Storage Account, Container, and SAS Token
STORAGE_ACCOUNT = "usewithdb"
CONTAINER = "testcontainer"
SASTOKEN = "?sv=2018-03-28&ss=b&srt=sco&sp=rwdlac&se=2019-12-07T00:00:23Z&st=2019-08-21T16:00:23Z&spr=https&sig=uAhWysD4%2BzM4bRTOcs18m%2F3LT7G4LeYa1t1hvRXB5Hk%3D"

# Do not change these values
SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
MOUNTPOINT = "/mnt/userzone"

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=MOUNTPOINT,
    extra_configs={URI:SASTOKEN})
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e
print("Success.")

# COMMAND ----------

# MAGIC %fs head /mnt/userzone/data.csv

# COMMAND ----------

# load file
df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/userzone/data.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md ## Remove rows with missing values

# COMMAND ----------

# df_new without missing values
df_new = df.dropna()

# COMMAND ----------

# MAGIC %md ## Remove Quantity with negative values

# COMMAND ----------

import pyspark.sql.functions as func
df_new = df_new.filter(func.col("Quantity") > 0)


# COMMAND ----------

# MAGIC %md ## Add the column - amount_spent

# COMMAND ----------

from pyspark.sql.functions import col
df_new = df_new.withColumn("AmountSpent",col("Quantity") * col("UnitPrice"))

# COMMAND ----------

df_new.show()

# COMMAND ----------

# MAGIC %md # Exploratory Data Analysis (EDA)

# COMMAND ----------

# MAGIC %md ## How many orders made by the customers?

# COMMAND ----------

df_group = df_new.groupBy("CustomerID").count().orderBy("CustomerID")
display(df_group)

# COMMAND ----------

# MAGIC %md ### Check TOP 5 most number of orders

# COMMAND ----------

df_group.orderBy('count',ascending=False).show(5)

# COMMAND ----------

display(df_group.orderBy('count',ascending=False).take(5))

# COMMAND ----------

# MAGIC %md ### Check TOP 10 highest money spent

# COMMAND ----------

df_group_money = df_new.groupBy("CustomerID").sum("AmountSpent")
display(df_group_money.orderBy("sum(AmountSpent)", ascending=False).take(10))
