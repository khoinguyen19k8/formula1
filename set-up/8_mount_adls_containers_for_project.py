# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Mount Azure Data Lake for the project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from key vault
    
    client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl-client-id")
    tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl-tenant-id")
    client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl-client-secret")

    # Set spark configuration

    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Mount the storage account container

    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

project_containers = ["raw", "processed", "presentation"]
for container in project_containers:
    mount_adls("formula1dlkhoinguyen19k8", container)

# COMMAND ----------

display(dbutils.fs.mounts())
