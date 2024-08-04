# Databricks notebook source
# MAGIC %sh
# MAGIC rm -r /dbfs/formula1dl/
# MAGIC mkdir /dbfs/formula1dl/
# MAGIC wget -O /dbfs/formula1dl/circuits.csv https://raw.githubusercontent.com/pradeepdekhane/Formula1dl/main/raw/circuits.csv
# MAGIC wget -O /dbfs/formula1dl/races.csv https://raw.githubusercontent.com/pradeepdekhane/Formula1dl/main/raw/races.csv

# COMMAND ----------

#move to container /importGithub/order_stream folder
#this command need not be needed in latest Databricks Edition as mount dbfs is created in local %sh for databricks file system
dbutils.fs.mv(f"file://///dbfs/formula1dl/", f"/importGithub/formula1dl", recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /importGithub/formula1dl

# COMMAND ----------

# import required libraries

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# create Schema

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# Create Data frame

df = spark.read.csv("/importGithub/formula1dl/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

# Write data in Parquest format
df.write.format("delta").mode("overwrite").save("/formula1dl/stg")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS FORMULA1DL_STG;
# MAGIC
# MAGIC DROP TABLE IF EXISTS FORMULA1DL_STG.CIRCUITS;
# MAGIC
# MAGIC CREATE TABLE FORMULA1DL_STG.CIRCUITS
# MAGIC USING DELTA
# MAGIC LOCATION '/formula1dl/stg'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Parameterized Query
# MAGIC
# MAGIC Using DataFrame API:
# MAGIC
# MAGIC 1] filter method:
# MAGIC
# MAGIC df_filtered = df.filter(df["Age"] > age_param)

# COMMAND ----------

v_filter = "bahrain"

df_filtered = df.filter(col("circuitRef") == v_filter)

display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC Using DataFrame API:
# MAGIC
# MAGIC 2] where methods:
# MAGIC
# MAGIC df_filtered = df.filter(col("Age") > age_param)

# COMMAND ----------

v_filter = "bahrain"

df_filtered = df.where(col("circuitRef") == v_filter)

display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC Parameterized Query
# MAGIC
# MAGIC Using Spark SQL
# MAGIC
# MAGIC 1] pass parameters

# COMMAND ----------

v_filter = "bahrain"

query = f"SELECT * FROM FORMULA1DL_STG.CIRCUITS WHERE circuitRef = '{v_filter}'"

df = spark.sql(query)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Parameterized Query
# MAGIC
# MAGIC 2] using format

# COMMAND ----------

query = "SELECT * FROM FORMULA1DL_STG.CIRCUITS WHERE circuitRef = '{v_filter}'"

df = spark.sql(query.format(v_filter = "bahrain"))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Parameterized Query
# MAGIC 3] using args

# COMMAND ----------

query = "SELECT * FROM FORMULA1DL_STG.CIRCUITS WHERE circuitRef = :v_filter"

df = spark.sql(query, args={"v_filter": "bahrain"})

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC End of file
