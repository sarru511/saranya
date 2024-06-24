# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------


df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

df.show(1)

# COMMAND ----------

df.show(1, vertical=True)

# COMMAND ----------

df.show(vertical=True)

# COMMAND ----------

df.columns

# COMMAND ----------

df.select("a", "b", "c").describe().show()

# COMMAND ----------

df.select("c").show()

# COMMAND ----------

df.select(df.c).show()

# COMMAND ----------

df.filter("a=1").show()

# COMMAND ----------

df.filter(col("c")=="string1").show()

# COMMAND ----------

df.head(1)

# COMMAND ----------

df.take(1)

# COMMAND ----------

df.tail(1)

# COMMAND ----------

df.withColumn("g",col("a")+5).show()

# COMMAND ----------

df.withColumn("upper_c",upper(df.c)).show()

# COMMAND ----------

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

# COMMAND ----------

df.groupby("color").avg().show()

# COMMAND ----------

df.selectExpr("v1").show()
df.select(expr('count(*)') > 10).show()

# COMMAND ----------

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', None , 2, 20], ['red', 'carrot', 3, 30],['blue', 'banana', 2, 20],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], [None, 'carrot', 6, 60],
    ['red', 'banana', 7, None], ['red', 'grape', None, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

# COMMAND ----------

df.fillna("pink",subset=["color"]).show()

# COMMAND ----------

df.dropna("any").show()

# COMMAND ----------

data = [ (1,"James","sales",3000),
        (2,None,"sales",2000),
        (3,"Michael","finance",8000),
        (4,"Jeff",None,900),
        (5,"jen","marketing",None),
        (6,"saif","finance",4000)
]
schema = ["id","emp_name","department","salary"]
df = spark.createDataFrame(data=data,schema=schema)


# COMMAND ----------



# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql import *
data = [
    {"color": "red", "fruit": "banana", "v1": 1, "v2": 10},
    {"color": "blue", "fruit": "banana", "v1": 2, "v2": 20},
    {"color": "red", "fruit": "carrot", "v1": 3, "v2": 30},
    {"color": "blue", "fruit": "grape", "v1": 4, "v2": 40},
    {"color": "red", "fruit": "carrot", "v1": 5, "v2": 50},
    {"color": "black", "fruit": "carrot", "v1": 6, "v2": 60},
    {"color": "red", "fruit": "banana", "v1": 7, "v2": 70},
    {"color": "red", "fruit": "grape", "v1": 8, "v2": 80}
]
schema = StructType([
    StructField("color", StringType(), True),
    StructField("fruit", StringType(), True),
    StructField("v1", IntegerType(), True),
    StructField("v2", IntegerType(), True)
])
df = spark.createDataFrame(data=data,schema=schema).show()

# COMMAND ----------

df.write.csv("/FileStore/csv/csvfile1.txt")

# COMMAND ----------

df=spark.read.option("header",True).csv("/FileStore/csv/csvfile1.txt").show()

# COMMAND ----------

df=spark.read.option("header",True).csv("/FileStore/csv/data.csv").show()


# COMMAND ----------

df=spark.read.option("header",True).json("/FileStore/json/data.json").show()

# COMMAND ----------


