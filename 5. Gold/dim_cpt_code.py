# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE schema IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_cpt_code (
# MAGIC     cpt_codes string,
# MAGIC     procedure_code_category string,
# MAGIC     procedure_code_descriptions string,
# MAGIC     code_status string,
# MAGIC     refreshed_at timestamp
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_cpt_code
# MAGIC select cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     current_timestamp() as refreshed_at
# MAGIC from silver.cptcodes
# MAGIC where is_quarantined = false
# MAGIC     and is_current = true 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from gold.dim_cpt_code
