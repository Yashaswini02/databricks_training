# Databricks notebook source
df_sales=spark.read.csv("/Volumes/yr_databricks/default/raw/sales.csv",header=True,inferSchema=True)
df_sales.display()

# COMMAND ----------

df_customers=spark.read.json("/Volumes/yr_databricks/default/raw/customers.json")
df_customers.display()

# COMMAND ----------

df_customers.write.saveAsTable("customers")

# COMMAND ----------

df_sales.write.mode("overwrite").saveAsTable("sales")
