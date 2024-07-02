# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/day.csv
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv")

# COMMAND ----------

df.write.saveAsTable("Bike")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.bike

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.bike where season=1

# COMMAND ----------

# MAGIC %sql
# MAGIC create view bikeseason1 as select * from hive_metastore.default.bike where season=1

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

df.createOrReplaceTempView("bikeview1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikeview1

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC create Temp view bikeview2 as select * from hive_metastore.default.bike where season=1

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

df.createOrReplaceGlobalTempView("bikeglobalview2")

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikeglobalview2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.bikeglobalview2

# COMMAND ----------


