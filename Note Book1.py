# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

from 

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv").show()

# COMMAND ----------

df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv").show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("instant"),col("dteday"),col("weekday")).show()

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv", header=True, inferSchema=True)

# COMMAND ----------

df.select("instant","dteday","weekday").show()

# COMMAND ----------

df.select(col("instant"),"dteday",df.weekday,df["temp"]).show()

# COMMAND ----------

df.select("yr".alias("year")).show()

# COMMAND ----------

df.select(col("yr").alias("year")).show()

# COMMAND ----------

df.take(5)

# COMMAND ----------

df.tail(5)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

display(df.describe("instant"))

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("*").show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.withColumn("temp",col("temp")+1).show()

# COMMAND ----------

df.withColumn("temp+1",col("temp")+1).show()

# COMMAND ----------

df.withColumnRenamed("yr","year").show()

# COMMAND ----------

len(df.columns)

# COMMAND ----------

df.count()

# COMMAND ----------

df.limit(5).show()

# COMMAND ----------

df.take(5)

# COMMAND ----------

display(df.take(4))

# COMMAND ----------

df.filter(col("weekday")==6).show()

# COMMAND ----------

df.filter("weekday==6").limit(5).show()

# COMMAND ----------

df.where(col("weekday")==6).show()

# COMMAND ----------

df.where(col("weekday")==6).limit(6).show()

# COMMAND ----------

df.where("weekday==6").limit(6).show()

# COMMAND ----------

df.select("*").filter(col("weekday").isNull()).show()

# COMMAND ----------

df.select("*").filter(col("weekday").isNotNull()).show()

# COMMAND ----------

df.drop("yr").show()

# COMMAND ----------

df.sort("temp").limit(5).show()

# COMMAND ----------

df.sort("temp", ascending=False).limit(5).show()

# COMMAND ----------

df.sort("temp",descending=True).limit(5).show()

# COMMAND ----------

df.sort("dteday","yr","mnth","temp",ascending=True).limit(5).show()

# COMMAND ----------

run "./nb2"

# COMMAND ----------

# MAGIC %run "./nb2"

# COMMAND ----------


