# Databricks notebook source
# MAGIC %sql
# MAGIC create database deltatables1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS deltatables1.people10m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended deltatables1.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS deltatables1.people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) location "dbfs:/user/hive/warehouse/deltatables1"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended deltatables1.people20m

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS deltatables1.people30m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) using PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended deltatables1.people30m

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table deltatables1.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table deltatables1.people20m

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS deltatables1.people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) location "dbfs:/user/hive/warehouse/deltatables1"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/dbacademy-users/temp/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS deltatables1.people40m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) location "dbfs:/mnt/dbacademy-users/temp/deltatables1/people40m"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table deltatables1.people40m

# COMMAND ----------

mkdir floder1

# COMMAND ----------

# MAGIC %sql
# MAGIC show * from floder1

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog catalog2

# COMMAND ----------

# MAGIC %sql
# MAGIC create database nly_dlt

# COMMAND ----------


