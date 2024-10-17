# Databricks notebook source
from pyspark.sql.functions import *

input_path="/Volumes/yr_databricks/default/raw/"

def add_ingestion(df):
    df1 = df.withColumn("ingestion_date",current_timestamp())
    return df1
