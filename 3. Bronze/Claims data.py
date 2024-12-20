# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

claims_df=spark.read.csv("/mnt/landing/claims/*.csv",header=True)

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.input_file_name().contains("hospital1"), "hosa").when(f.input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

display(claims_df)

# COMMAND ----------

# DBTITLE 1,Parquet file creation
claims_df.write.format("parquet").mode("overwrite").save("/mnt/bronze/claims/")

# COMMAND ----------

claims_df.createOrReplaceTempView("claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality check for claims: ClaimID, TransactionID, PatientID, ServiceDate if null then is_quarantined = true
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT CONCAT(ClaimID, '-', datasource) AS ClaimID,
# MAGIC     ClaimID AS SRC_ClaimID,
# MAGIC     TransactionID,
# MAGIC     PatientID,
# MAGIC     EncounterID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     cast(ServiceDate as date) ServiceDate,
# MAGIC     cast(ClaimDate as date) ClaimDate,
# MAGIC     PayorID,
# MAGIC     ClaimAmount,
# MAGIC     PaidAmount,
# MAGIC     ClaimStatus,
# MAGIC     PayorType,
# MAGIC     Deductible,
# MAGIC     Coinsurance,
# MAGIC     Copay,
# MAGIC     cast(InsertDate as date) as SRC_InsertDate,
# MAGIC     cast(ModifiedDate as date) as SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE
# MAGIC         WHEN ClaimID IS NULL
# MAGIC         OR TransactionID IS NULL
# MAGIC         OR PatientID IS NULL
# MAGIC         or ServiceDate IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from quality_checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE schema IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC  %sql 
# MAGIC  CREATE TABLE IF NOT EXISTS silver.claims (
# MAGIC         ClaimID string,
# MAGIC         SRC_ClaimID string,
# MAGIC         TransactionID string,
# MAGIC         PatientID string,
# MAGIC         EncounterID string,
# MAGIC         ProviderID string,
# MAGIC         DeptID string,
# MAGIC         ServiceDate date,
# MAGIC         ClaimDate date,
# MAGIC         PayorID string,
# MAGIC         ClaimAmount string,
# MAGIC         PaidAmount string,
# MAGIC         ClaimStatus string,
# MAGIC         PayorType string,
# MAGIC         Deductible string,
# MAGIC         Coinsurance string,
# MAGIC         Copay string,
# MAGIC         SRC_InsertDate date,
# MAGIC         SRC_ModifiedDate date,
# MAGIC         datasource string,
# MAGIC         is_quarantined boolean,
# MAGIC         audit_insertdate timestamp,
# MAGIC         audit_modifieddate timestamp,
# MAGIC         is_current boolean
# MAGIC     ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO silver.claims AS target 
# MAGIC USING quality_checks AS source 
# MAGIC ON target.ClaimID = source.ClaimID
# MAGIC AND target.is_current = true
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC     target.SRC_ClaimID != source.SRC_ClaimID
# MAGIC     OR target.TransactionID != source.TransactionID
# MAGIC     OR target.PatientID != source.PatientID
# MAGIC     OR target.EncounterID != source.EncounterID
# MAGIC     OR target.ProviderID != source.ProviderID
# MAGIC     OR target.DeptID != source.DeptID
# MAGIC     OR target.ServiceDate != source.ServiceDate
# MAGIC     OR target.ClaimDate != source.ClaimDate
# MAGIC     OR target.PayorID != source.PayorID
# MAGIC     OR target.ClaimAmount != source.ClaimAmount
# MAGIC     OR target.PaidAmount != source.PaidAmount
# MAGIC     OR target.ClaimStatus != source.ClaimStatus
# MAGIC     OR target.PayorType != source.PayorType
# MAGIC     OR target.Deductible != source.Deductible
# MAGIC     OR target.Coinsurance != source.Coinsurance
# MAGIC     OR target.Copay != source.Copay
# MAGIC     OR target.SRC_InsertDate != source.SRC_InsertDate
# MAGIC     OR target.SRC_ModifiedDate != source.SRC_ModifiedDate
# MAGIC     OR target.datasource != source.datasource
# MAGIC     OR target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC UPDATE
# MAGIC SET target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp();

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO silver.claims AS target 
# MAGIC USING quality_checks AS source 
# MAGIC ON target.ClaimID = source.ClaimID
# MAGIC AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC         ClaimID,
# MAGIC         SRC_ClaimID,
# MAGIC         TransactionID,
# MAGIC         PatientID,
# MAGIC         EncounterID,
# MAGIC         ProviderID,
# MAGIC         DeptID,
# MAGIC         ServiceDate,
# MAGIC         ClaimDate,
# MAGIC         PayorID,
# MAGIC         ClaimAmount,
# MAGIC         PaidAmount,
# MAGIC         ClaimStatus,
# MAGIC         PayorType,
# MAGIC         Deductible,
# MAGIC         Coinsurance,
# MAGIC         Copay,
# MAGIC         SRC_InsertDate,
# MAGIC         SRC_ModifiedDate,
# MAGIC         datasource,
# MAGIC         is_quarantined,
# MAGIC         audit_insertdate,
# MAGIC         audit_modifieddate,
# MAGIC         is_current
# MAGIC     )
# MAGIC VALUES (
# MAGIC         source.ClaimID,
# MAGIC         source.SRC_ClaimID,
# MAGIC         source.TransactionID,
# MAGIC         source.PatientID,
# MAGIC         source.EncounterID,
# MAGIC         source.ProviderID,
# MAGIC         source.DeptID,
# MAGIC         source.ServiceDate,
# MAGIC         source.ClaimDate,
# MAGIC         source.PayorID,
# MAGIC         source.ClaimAmount,
# MAGIC         source.PaidAmount,
# MAGIC         source.ClaimStatus,
# MAGIC         source.PayorType,
# MAGIC         source.Deductible,
# MAGIC         source.Coinsurance,
# MAGIC         source.Copay,
# MAGIC         source.SRC_InsertDate,
# MAGIC         source.SRC_ModifiedDate,
# MAGIC         source.datasource,
# MAGIC         source.is_quarantined,
# MAGIC         current_timestamp(),
# MAGIC         current_timestamp(),
# MAGIC         true
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from silver.claims
