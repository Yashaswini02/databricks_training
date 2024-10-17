# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_join=df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"],"inner")

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_product=spark.table("products")

# COMMAND ----------

df_join1=df_sales.join(df_product,"product_id","inner")

# COMMAND ----------

df_join1.display()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.filter("customer_id=2").display()

# COMMAND ----------

from pyspark.sql.functions import *
df_sales.where(col("customer_id")==2).display()

# COMMAND ----------

df_customers.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_join.groupBy("sales.customer_id").count().orderBy("sales.customer_id").display()

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales, customers where sales.customer_id=customers.customer_id

# COMMAND ----------


