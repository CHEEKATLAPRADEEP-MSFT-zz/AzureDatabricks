# Databricks notebook source
dbutils.fs.ls("abfss://filesystem@chepragen2.dfs.core.windows.net/")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.chepragen2.dfs.core.windows.net", "48T0fl5dEP49Na6sV0RhGNKbZzVmjXLIAo8057VTs11kMYcQs3o70JLu0aDxro2WoD2wwOSMcHUyMY8f5oGd6Q==")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://filesystem@chepragen2.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

dbutils.fs.ls("abfss://filesystem@chepragen2.dfs.core.windows.net/azure")

# COMMAND ----------

dbutils.fs.ls("abfss://filesystem@chepragen2.dfs.core.windows.net/azure")