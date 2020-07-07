# Databricks notebook source
# MAGIC %md ### [Stackoverflow question - Databricks- Can we variablize the mount_point name during creation by passing the value from SQL lookup table](https://stackoverflow.com/questions/62726597/databricks-can-we-variablize-the-mount-point-name-during-creation-by-passing-th)

# COMMAND ----------

path = "dbfs:/mnt/{0}/{1}".format(mountname,csvname)

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://test@chepra.blob.core.windows.net/",
  mount_point = "/mnt/{0}".format(mountname),
  extra_configs = {"fs.azure.sas.test.chepra.blob.core.windows.net":"gv7nVISerl8wbK9mPGm8TC3CQIEjV3Z5dQ9sHjxnZ7DnUPlQyYnd1zUayGWoyE0CU7dWPTAqaXhKyb9hldlOiA=="})
print("=> Succeeded")

# COMMAND ----------

df = spark.read.format("csv").option("sep", ",").options(header= "true", inferschema='true').option('escape','"').load("{0}".format(path))

# COMMAND ----------

display(df)