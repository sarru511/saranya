# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.csv("/FileStore/csv/csvfile1.txt")

# COMMAND ----------

data=(
    
    [1, "Laptop", 1200],
    [2, "Smartphone", 800],
    [3, "Tablet", 450],
    [4, "Monitor", 300]
)
schema=["ID", "Product", "Price"]
df=spark.createDataFrame(data,schema=schema)
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database emp;
# MAGIC use database emp;

# COMMAND ----------

df.write.saveAsTable("Employee")

# COMMAND ----------

df.show()

# COMMAND ----------

df.withColumn("no.of",col("ID")+5).show()

# COMMAND ----------

df=spark.read.csv("/FileStore/csv/csvfile1.txt").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC use database emp;

# COMMAND ----------

dbutils.fs.mkdirs("/FileStores/tables/StreamData/input")

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/StreamData/input/office_emp.csv")
df.display()

# COMMAND ----------

df=spark.readStream.option("header",True).schema(schema).csv("/FileStore/tables/StreamData/input/")
df.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

# COMMAND ----------

df.writeStream.option("format","parquet").outputMode("append").option("checkpointLocation","/FileStore/tables/StreamData/checkpoint").option("path","/FileStore/tables/StreamData/output").start()

# COMMAND ----------

dfnew=spark.read.format("delta").load("/FileStore/tables/StreamData/output")
display(dfnew)

# COMMAND ----------


