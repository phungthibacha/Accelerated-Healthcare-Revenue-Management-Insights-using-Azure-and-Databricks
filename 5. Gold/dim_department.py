# Databricks notebook source
# MAGIC
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_department (
# MAGIC     Dept_Id string,
# MAGIC     SRC_Dept_Id string,
# MAGIC     Name string,
# MAGIC     datasource string
# MAGIC ) 

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC truncate TABLE gold.dim_department 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only insert new records into gold layer, if is_quarantined = False
# MAGIC insert into gold.dim_department
# MAGIC select distinct Dept_Id,
# MAGIC     SRC_Dept_Id,
# MAGIC     Name,
# MAGIC     datasource
# MAGIC from silver.departments
# MAGIC where is_quarantined = false 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from gold.dim_department
