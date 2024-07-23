# Databricks notebook source
input_file=("/FileStore/streamdata/input")

# COMMAND ----------

output_path=("/FileStore/streamdata/output")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database naval

# COMMAND ----------

# MAGIC %sql
# MAGIC use database naval

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("DepartmentID", IntegerType(), True),
    StructField("DepartmentName", StringType(), True),
    StructField("Manager", StringType(), True),
    StructField("Employees", IntegerType(), True)
])

# COMMAND ----------

df=spark.readStream.option("header",True).schema(schema).csv("/FileStore/streamdata/input")

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaLocation", f"{output_path}/naval/autoloader/schemalocation")
 .load(f"{input_file}")
 .writeStream
 .option("checkpointLocation", f"{output_path}/naval/autoloader/checkpoint")
 .format("delta")  # Specify the format as Delta
 .start(path=f"{output_path}/naval/autoloader/output", queryName="naval.autoloader")  # Start the streaming query with the table name
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.autoloader

# COMMAND ----------

df=spark.read.option("header",True).csv("/FileStore/streamdata/input").show()

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaLocation", f"{output_path}/naval/autoloader2/schemalocation")
 .load(f"{input_file}")
 .writeStream
 .option("checkpointLocation", f"{output_path}/naval/autoloader1/checkpoint")
 .format("delta")  # Specify the format as Delta
 .start(path=f"{output_path}/naval/autoloader1/output", queryName="naval.autoloader1") )  # Start the streaming query with the table name
 

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaLocation", f"{output_path}/naval/autoloader2/schemalocation")
 .load(f"{input_file}")
 .writeStream
 .option("checkpointLocation", f"{output_path}/naval/autoloader2/checkpoint")
 .format("delta")  # Specify the format as Delta
 .start(path=f"{output_path}/naval/autoloader2/output")  # Start the streaming query with the table name
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.autoloader

# COMMAND ----------


