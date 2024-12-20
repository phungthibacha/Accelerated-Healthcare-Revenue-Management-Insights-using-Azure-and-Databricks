# Databricks notebook source
#Reading Hospital A patient data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/patients")
df_hosa.createOrReplaceTempView("patients_hosa")

#Reading Hospital B patient data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/patients")
df_hosb.createOrReplaceTempView("patients_hosb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from patients_hosa 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from patients_hosb 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Common Data Model for patients data, which is a union of patients_hosa and patients_hosb, with different schemas
# MAGIC CREATE OR REPLACE TEMP VIEW cdm_patients AS
# MAGIC SELECT CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
# MAGIC     *
# MAGIC FROM (
# MAGIC         SELECT PatientID AS SRC_PatientID,
# MAGIC             FirstName,
# MAGIC             LastName,
# MAGIC             MiddleName,
# MAGIC             SSN,
# MAGIC             PhoneNumber,
# MAGIC             Gender,
# MAGIC             DOB,
# MAGIC             Address,
# MAGIC             ModifiedDate,
# MAGIC             datasource
# MAGIC         FROM patients_hosa
# MAGIC         UNION ALL
# MAGIC         SELECT ID AS SRC_PatientID,
# MAGIC             F_Name AS FirstName,
# MAGIC             L_Name AS LastName,
# MAGIC             M_Name ASMiddleName,
# MAGIC             SSN,
# MAGIC             PhoneNumber,
# MAGIC             Gender,
# MAGIC             DOB,
# MAGIC             Address,
# MAGIC             Updated_Date AS ModifiedDate,
# MAGIC             datasource
# MAGIC         FROM patients_hosb
# MAGIC     ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from cdm_patients 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality checks for patients data, if SRC_PatientID, dob, firstname are null then is_quarantined is true
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT Patient_Key,
# MAGIC     SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     ModifiedDate As SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE
# MAGIC         WHEN SRC_PatientID IS NULL
# MAGIC         OR dob IS NULL
# MAGIC         OR firstname IS NULL
# MAGIC         or lower(firstname) = 'null' THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cdm_patients 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from quality_checks
# MAGIC order by is_quarantined desc 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS silver.patients (
# MAGIC         Patient_Key STRING,
# MAGIC         SRC_PatientID STRING,
# MAGIC         FirstName STRING,
# MAGIC         LastName STRING,
# MAGIC         MiddleName STRING,
# MAGIC         SSN STRING,
# MAGIC         PhoneNumber STRING,
# MAGIC         Gender STRING,
# MAGIC         DOB DATE,
# MAGIC         Address STRING,
# MAGIC         SRC_ModifiedDate TIMESTAMP,
# MAGIC         datasource STRING,
# MAGIC         is_quarantined BOOLEAN,
# MAGIC         inserted_date TIMESTAMP,
# MAGIC         modified_date TIMESTAMP,
# MAGIC         is_current BOOLEAN
# MAGIC     ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Step 1: Mark existing records as historical (is_current = false) for patients that will be updated
# MAGIC MERGE INTO silver.patients AS target USING quality_checks AS source ON target.Patient_Key = source.Patient_Key
# MAGIC AND target.is_current = true
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC     target.SRC_PatientID <> source.SRC_PatientID
# MAGIC     OR target.FirstName <> source.FirstName
# MAGIC     OR target.LastName <> source.LastName
# MAGIC     OR target.MiddleName <> source.MiddleName
# MAGIC     OR target.SSN <> source.SSN
# MAGIC     OR target.PhoneNumber <> source.PhoneNumber
# MAGIC     OR target.Gender <> source.Gender
# MAGIC     OR target.DOB <> source.DOB
# MAGIC     OR target.Address <> source.Address
# MAGIC     OR target.SRC_ModifiedDate <> source.SRC_ModifiedDate
# MAGIC     OR target.datasource <> source.datasource
# MAGIC     OR target.is_quarantined <> source.is_quarantined
# MAGIC ) THEN
# MAGIC UPDATE
# MAGIC SET target.is_current = false,
# MAGIC     target.modified_date = current_timestamp()
# MAGIC     WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC         Patient_Key,
# MAGIC         SRC_PatientID,
# MAGIC         FirstName,
# MAGIC         LastName,
# MAGIC         MiddleName,
# MAGIC         SSN,
# MAGIC         PhoneNumber,
# MAGIC         Gender,
# MAGIC         DOB,
# MAGIC         Address,
# MAGIC         SRC_ModifiedDate,
# MAGIC         datasource,
# MAGIC         is_quarantined,
# MAGIC         inserted_date,
# MAGIC         modified_date,
# MAGIC         is_current
# MAGIC     )
# MAGIC VALUES (
# MAGIC         source.Patient_Key,
# MAGIC         source.SRC_PatientID,
# MAGIC         source.FirstName,
# MAGIC         source.LastName,
# MAGIC         source.MiddleName,
# MAGIC         source.SSN,
# MAGIC         source.PhoneNumber,
# MAGIC         source.Gender,
# MAGIC         source.DOB,
# MAGIC         source.Address,
# MAGIC         source.SRC_ModifiedDate,
# MAGIC         source.datasource,
# MAGIC         source.is_quarantined,
# MAGIC         current_timestamp(),
# MAGIC         -- Set inserted_date to current timestamp
# MAGIC         current_timestamp(),
# MAGIC         -- Set modified_date to current timestamp
# MAGIC         true -- Mark as current
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO silver.patients AS target 
# MAGIC USING quality_checks AS source 
# MAGIC ON target.Patient_Key = source.Patient_Key
# MAGIC AND target.is_current = true 
# MAGIC -- Step 2: When is_current = Fale then Insert new and updated records into the Delta table, marking them as current
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC         Patient_Key,
# MAGIC         SRC_PatientID,
# MAGIC         FirstName,
# MAGIC         LastName,
# MAGIC         MiddleName,
# MAGIC         SSN,
# MAGIC         PhoneNumber,
# MAGIC         Gender,
# MAGIC         DOB,
# MAGIC         Address,
# MAGIC         SRC_ModifiedDate,
# MAGIC         datasource,
# MAGIC         is_quarantined,
# MAGIC         inserted_date,
# MAGIC         modified_date,
# MAGIC         is_current
# MAGIC     )
# MAGIC VALUES (
# MAGIC         source.Patient_Key,
# MAGIC         source.SRC_PatientID,
# MAGIC         source.FirstName,
# MAGIC         source.LastName,
# MAGIC         source.MiddleName,
# MAGIC         source.SSN,
# MAGIC         source.PhoneNumber,
# MAGIC         source.Gender,
# MAGIC         source.DOB,
# MAGIC         source.Address,
# MAGIC         source.SRC_ModifiedDate,
# MAGIC         source.datasource,
# MAGIC         source.is_quarantined,
# MAGIC         current_timestamp(),
# MAGIC         -- Set inserted_date to current timestamp
# MAGIC         current_timestamp(),
# MAGIC         -- Set modified_date to current timestamp
# MAGIC         true -- Mark as current
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),
# MAGIC     Patient_Key
# MAGIC from silver.patients
# MAGIC group by patient_key
# MAGIC order by 1 desc 
# MAGIC
# MAGIC
