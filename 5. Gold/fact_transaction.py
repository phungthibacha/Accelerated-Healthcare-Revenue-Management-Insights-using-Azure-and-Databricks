# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.fact_transactions
# MAGIC (
# MAGIC   TransactionID string,
# MAGIC   SRC_TransactionID string,
# MAGIC   EncounterID string,
# MAGIC   FK_PatientID string,
# MAGIC   FK_ProviderID string,
# MAGIC   FK_DeptID string,
# MAGIC   ICDCode string,
# MAGIC   ProcedureCode string,
# MAGIC   VisitType string,
# MAGIC   ServiceDate date,
# MAGIC   PaidDate date,
# MAGIC   Amount double,
# MAGIC   PaidAmount double,
# MAGIC   AmountType string,
# MAGIC   ClaimID string,
# MAGIC   datasource string,
# MAGIC   refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.fact_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.fact_transactions
# MAGIC select 
# MAGIC   t.TransactionID, 
# MAGIC   t.SRC_TransactionID,
# MAGIC   t.EncounterID,
# MAGIC   concat(t.PatientID,'-',t.datasource ) as FK_Patient_ID,
# MAGIC   case when t.datasource='hos-a' then concat('H1-',t.providerID) else concat('H2-',t.providerID ) end as FK_Provider_ID, 
# MAGIC   concat(t.DeptID,'-',t.datasource ) as FK_Dept_ID, 
# MAGIC   t.ICDCode,
# MAGIC   t.ProcedureCode CPT_Code,
# MAGIC   t.VisitType,
# MAGIC   t.ServiceDate, 
# MAGIC   t.PaidDate,
# MAGIC   t.Amount Charge_Amt, 
# MAGIC   t.PaidAmount Paid_Amt, 
# MAGIC   t.AmountType,
# MAGIC   t.ClaimID,
# MAGIC   t.datasource,
# MAGIC   current_timestamp()
# MAGIC   from silver.transactions t 
# MAGIC   where t.is_current=true and t.is_quarantined=false
# MAGIC
