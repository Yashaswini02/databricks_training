# Databricks notebook source
# MAGIC %run /Workspace/Users/yash.nikhil0221@gmail.com/Day1/includes

# COMMAND ----------

# DBTITLE 1,Read
df_order=spark.read.csv(f"{input_path}order_dates.csv",header=True,inferSchema=True)

# COMMAND ----------

df2=add_ingestion(df_order)

# COMMAND ----------

df2.display()

# COMMAND ----------

# DBTITLE 1,Write
df2.write.mode("overwrite").saveAsTable("order")
