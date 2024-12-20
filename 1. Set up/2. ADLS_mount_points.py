# Databricks notebook source
scopes = dbutils.secrets.listScopes()
display(scopes)

# COMMAND ----------

# Databricks notebook source
storageAccountName = "adlshealthcareprojectdev"
storageAccountAccessKey = dbutils.secrets.get('adlskv', 'adls-access-key')
mountPoints = ["gold", "silver", "bronze", "landing", "configs"]

for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
                source = f"wasbs://{mountPoint}@{storageAccountName}.blob.core.windows.net",
                mount_point = f"/mnt/{mountPoint}",
                extra_configs = {f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net": storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print(f"mount exception for {mountPoint}: {e}")

dbutils.fs.mounts()

# COMMAND ----------

# Databricks notebook source
storageAccountName = "adlshealthcareprojectdev"
storageAccountAccessKey = dbutils.secrets.get('adlskv', 'adls-access-key')
mountPoints = ["healthcareunitycatalog"]

for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
                source = f"wasbs://{mountPoint}@{storageAccountName}.blob.core.windows.net",
                mount_point = f"/mnt/{mountPoint}",
                extra_configs = {f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net": storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print(f"mount exception for {mountPoint}: {e}")

dbutils.fs.mounts()

# COMMAND ----------

display(dbutils.fs.mounts())
