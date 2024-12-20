# Databricks notebook source
# MAGIC %sql 
# MAGIC -- Create temporary views for the parquet files
# MAGIC CREATE OR REPLACE TEMP VIEW hosa_encounters USING parquet OPTIONS (path "dbfs:/mnt/bronze/hosa/encounters");
# MAGIC CREATE OR REPLACE TEMP VIEW hosb_encounters USING parquet OPTIONS (path "dbfs:/mnt/bronze/hosb/encounters");
# MAGIC -- Union the two views
# MAGIC CREATE OR REPLACE TEMP VIEW encounters AS
# MAGIC SELECT *
# MAGIC FROM hosa_encounters
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM hosb_encounters;
# MAGIC -- Display the merged data
# MAGIC SELECT *
# MAGIC FROM encounters;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from encounters 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality check for encounter data, if EncounterID, PatientID are null then is_quarantined = True
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT concat(EncounterID, '-', datasource) as EncounterID,
# MAGIC     EncounterID SRC_EncounterID,
# MAGIC     PatientID,
# MAGIC     EncounterDate,
# MAGIC     EncounterType,
# MAGIC     ProviderID,
# MAGIC     DepartmentID,
# MAGIC     ProcedureCode,
# MAGIC     InsertedDate as SRC_InsertedDate,
# MAGIC     ModifiedDate as SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE
# MAGIC         WHEN EncounterID IS NULL
# MAGIC         OR PatientID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM encounters 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from quality_checks
# MAGIC where datasource = 'hos-b' 

# COMMAND ----------

# MAGIC %sql CREATE TABLE IF NOT EXISTS silver.encounters (
# MAGIC         EncounterID string,
# MAGIC         SRC_EncounterID string,
# MAGIC         PatientID string,
# MAGIC         EncounterDate date,
# MAGIC         EncounterType string,
# MAGIC         ProviderID string,
# MAGIC         DepartmentID string,
# MAGIC         ProcedureCode integer,
# MAGIC         SRC_InsertedDate date,
# MAGIC         SRC_ModifiedDate date,
# MAGIC         datasource string,
# MAGIC         is_quarantined boolean,
# MAGIC         audit_insertdate timestamp,
# MAGIC         audit_modifieddate timestamp,
# MAGIC         is_current boolean
# MAGIC     ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO silver.encounters AS target USING quality_checks AS source ON target.EncounterID = source.EncounterID
# MAGIC AND target.is_current = true
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC     target.SRC_EncounterID != source.SRC_EncounterID
# MAGIC     OR target.PatientID != source.PatientID
# MAGIC     OR target.EncounterDate != source.EncounterDate
# MAGIC     OR target.EncounterType != source.EncounterType
# MAGIC     OR target.ProviderID != source.ProviderID
# MAGIC     OR target.DepartmentID != source.DepartmentID
# MAGIC     OR target.ProcedureCode != source.ProcedureCode
# MAGIC     OR target.SRC_InsertedDate != source.SRC_InsertedDate
# MAGIC     OR target.SRC_ModifiedDate != source.SRC_ModifiedDate
# MAGIC     OR target.datasource != source.datasource
# MAGIC     OR target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC UPDATE
# MAGIC SET target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp() 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC     MERGE INTO silver.encounters AS target USING quality_checks AS source ON target.EncounterID = source.EncounterID
# MAGIC     AND target.is_current = true
# MAGIC     WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC         EncounterID,
# MAGIC         SRC_EncounterID,
# MAGIC         PatientID,
# MAGIC         EncounterDate,
# MAGIC         EncounterType,
# MAGIC         ProviderID,
# MAGIC         DepartmentID,
# MAGIC         ProcedureCode,
# MAGIC         SRC_InsertedDate,
# MAGIC         SRC_ModifiedDate,
# MAGIC         datasource,
# MAGIC         is_quarantined,
# MAGIC         audit_insertdate,
# MAGIC         audit_modifieddate,
# MAGIC         is_current
# MAGIC     )
# MAGIC VALUES (
# MAGIC         source.EncounterID,
# MAGIC         source.SRC_EncounterID,
# MAGIC         source.PatientID,
# MAGIC         source.EncounterDate,
# MAGIC         source.EncounterType,
# MAGIC         source.ProviderID,
# MAGIC         source.DepartmentID,
# MAGIC         source.ProcedureCode,
# MAGIC         source.SRC_InsertedDate,
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
# MAGIC select SRC_EncounterID,
# MAGIC     datasource,
# MAGIC     count(patientid)
# MAGIC from silver.encounters
# MAGIC group by all
# MAGIC order by 3 desc
