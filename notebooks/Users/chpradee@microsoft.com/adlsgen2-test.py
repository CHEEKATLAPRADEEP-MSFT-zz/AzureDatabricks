# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "06ec2f57-cf96-4dee-af4c-9bb8efbd60ef", #Enter <appId> = Application ID
       "fs.azure.account.oauth2.client.secret": "ArrIkkl0afp5qyxdp:Uoy7].vX7bMt]*", #Enter <password> = Client Secret created in AAD
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token", #Enter <tenant> = Tenant ID
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://azure@azurewalagen2.dfs.core.windows.net/", #Enter <container-name> = filesystem name <storage-account-name> = storage name
mount_point = "/mnt/azure",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("mnt/azure/")

# COMMAND ----------

dbutils.fs.ls("mnt/azure/")

# COMMAND ----------

df = spark.read.csv("/mnt/azure/sales.csv", header="true")
display(df)

# COMMAND ----------

