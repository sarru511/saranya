# Databricks notebook source
# MAGIC %sql
# MAGIC use database naval

# COMMAND ----------

# MAGIC %sql
# MAGIC create table my_table1

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO naval.my_table1
# MAGIC FROM '/FileStore/streamdata/input'
# MAGIC FILEFORMAT = CSV
# MAGIC COPY_OPTIONS ('mergeSchema' ='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO naval.my_table1
# MAGIC FROM '/FileStore/streamdata/input'
# MAGIC FILEFORMAT = CSV
# MAGIC COPY_OPTIONS ('mergeSchema' ='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO naval.my_table
# MAGIC FROM '/FileStore/streamdata/input'
# MAGIC FILEFORMAT = CSV
# MAGIC COPY_OPTIONS ('mergeSchema' ='true')

# COMMAND ----------


