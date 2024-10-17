# Databricks notebook source
# MAGIC %sql
# MAGIC create table customers as
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/yr_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/yr_databricks/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products
