# Databricks notebook source
df=spark.read.csv("/FileStore/streamdata/input/jan.csv").show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("DepartmentID", IntegerType(), True),
    StructField("DepartmentName", StringType(), True),
    StructField("Manager", StringType(), True),
    StructField("Employees", IntegerType(), True)
])

# COMMAND ----------

df=spark.read.csv("/FileStore/streamdata/input")
df.display()

# COMMAND ----------

df=spark.readStream.option("header",True).schema(schema).csv("/FileStore/streamdata/input")

# COMMAND ----------

df.writeStream.option("format","parquet").outputMode("append").option("checkpointLocation","/FileStore/streamdata/checkpoint").option("path","/FileStore/streamdata/output").start()

# COMMAND ----------

df.display()

# COMMAND ----------


