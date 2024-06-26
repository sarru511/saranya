# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.option("multiline",True).json("/FileStore/testfiles/json/emp_details.txt")

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumn("sales_new",explode("sales")).display()

# COMMAND ----------

df.withColumn("sales_new",explode("sales"))\
.withColumn("sales_firstName",col("sales.firstName")).display()


# COMMAND ----------

df.withColumn("sales_new",explode("sales"))\
.withColumn("sales_firstName",col("sales.firstName"))\
.withColumn("sales_age",col("sales.age"))\
.withColumn("sales_lastName",col("sales.lastName")).drop("sales").display()


# COMMAND ----------

df.drop("sales").display()

# COMMAND ----------

df.withColumn("accounting_new",explode("accounting"))\
.withColumn("accounting_firstName",col("accounting.firstName"))\
.withColumn("accounting_age",col("accounting.age"))\
.withColumn("accounting_lastName",col("accounting.lastName"))\
    .drop("accounting").display()

# COMMAND ----------

df.withColumn("accounting",explode("accounting"))\
.withColumn("accounting_firstName",col("accounting.firstName"))\
.withColumn("accounting_age",col("accounting.age"))\
.withColumn("accounting_lastName",col("accounting.lastName"))\
.drop("accounting")\
.withColumn("sales",explode("sales"))\
.withColumn("sales_firstName",col("sales.firstName"))\
.withColumn("sales_age",col("sales.age"))\
.withColumn("sales_lastName",col("sales.lastName"))\
.drop("sales").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC use database emp;

# COMMAND ----------

df.write.mode("overwrite").parquet("emp_deatils")

# COMMAND ----------

df.write.saveAsTable("emp_details")

# COMMAND ----------

df=spark.read.parquet("/emp_deatils").display()

# COMMAND ----------


