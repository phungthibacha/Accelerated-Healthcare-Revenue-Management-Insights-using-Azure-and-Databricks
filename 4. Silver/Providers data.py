# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/providers")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/providers")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

df_merged.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC     ProviderID string,
# MAGIC     FirstName string,
# MAGIC     LastName string,
# MAGIC     Specialization string,
# MAGIC     DeptID string,
# MAGIC     NPI long,
# MAGIC     datasource string,
# MAGIC     is_quarantined boolean
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality check for providers data, if ProviderID, DepID is null then is_quarantined is True
# MAGIC insert into silver.providers
# MAGIC select distinct ProviderID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Specialization,
# MAGIC     DeptID,
# MAGIC     cast(NPI as INT) NPI,
# MAGIC     datasource,
# MAGIC     CASE
# MAGIC         WHEN ProviderID IS NULL
# MAGIC         OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC from providers
