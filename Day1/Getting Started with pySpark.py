# Databricks notebook source
print("Hello")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Core
# MAGIC
# MAGIC DataFrame: Structured API
# MAGIC

# COMMAND ----------

spark

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema="id int, name string, age int"
df=spark.createDataFrame(data, schema)
df.show()

df.display()


# COMMAND ----------

df.display()

# COMMAND ----------

df.select(*)

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()
