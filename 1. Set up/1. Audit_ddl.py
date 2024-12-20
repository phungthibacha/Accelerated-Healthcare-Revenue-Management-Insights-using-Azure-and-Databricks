# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG `healthcare-unity-catalog`;
# MAGIC CREATE schema IF NOT EXISTS audit; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS audit.load_logs (
# MAGIC   data_source STRING,
# MAGIC   tablename STRING,
# MAGIC   numberofrowscopied INT,
# MAGIC   watermarkcolumnname STRING,
# MAGIC   loaddate TIMESTAMP
# MAGIC );
