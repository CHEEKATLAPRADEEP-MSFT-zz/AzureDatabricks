# Databricks notebook source
# MAGIC %md
# MAGIC #METHOD:2 Mount an Azure Blob Storage

# COMMAND ----------

# MAGIC %md ##To mount a Blob Storage container or a folder inside a container, use the following command:

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://sampledata@chepra.blob.core.windows.net/Azure",
  mount_point = "/mnt/chepra",
  extra_configs = {"fs.azure.sas.sampledata.chepra.blob.core.windows.net":dbutils.secrets.get(scope = "sadanandm", key = "sadanandm")})

# COMMAND ----------

# MAGIC %md ##Access files in your container as if they were local files, for example:

# COMMAND ----------

df = spark.read.csv("/mnt/chepra/AzureCostAnalysis.csv", header="true")
df.show()

# COMMAND ----------

# MAGIC %md ## Unmount a mount point

# COMMAND ----------

dbutils.fs.unmount("/mnt/chepra")

# COMMAND ----------

# MAGIC %md #METHOD3:  Access Azure Blob Storage directly
# MAGIC ### Access Azure Blob Storage using the DataFrame API

# COMMAND ----------

# Set up an account access key:
# Get Storage account Name and 

spark.conf.set("fs.azure.account.key.chepra.blob.core.windows.net", "gv7nVISerl8wbK9mPGm8TC3CQIEjV3Z5dQ9sHjxnZ7DnUPlQyYnd1zUayGWoyE0CU7dWPTAqaXhKyb9hldlOiA==")
df = spark.read.csv("wasbs://sampledata@chepra.blob.core.windows.net/Azure/AzureCostAnalysis.csv", header="true")
df.show()

# COMMAND ----------

# MAGIC %md ##Access Azure Blob Storage using the RDD API

# COMMAND ----------

# MAGIC  %md 
# MAGIC  
# MAGIC  Hadoop configuration options set using spark.conf.set(...) are not accessible via SparkContext. This means that while they are visible to the DataFrame and Dataset API, they are not visible to the RDD API. If you are using the RDD API to read from Azure Blob Storage, you must set the credentials using one of the following methods:
# MAGIC 
# MAGIC Specify the Hadoop credential configuration options as Spark options when you create the cluster. You must add the spark.hadoop. prefix to the corresponding Hadoop configuration keys to tell Spark to propagate them to the Hadoop configurations that are used for your RDD jobs:

# COMMAND ----------

# Using an account access key
spark.hadoop.fs.azure.account.key.<storage-account-name>.blob.core.windows.net <storage-account-access-key>

# Using a SAS token
spark.hadoop.fs.azure.sas.<container-name>.<storage-account-name>.blob.core.windows.net <complete-query-string-of-sas-for-the-container>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Scala users can set the credentials in spark.sparkContext.hadoopConfiguration:

# COMMAND ----------

// Using an account access key
spark.sparkContext.hadoopConfiguration.set(
  "fs.azure.account.key.<storage-account-name>.blob.core.windows.net",
  "<storage-account-access-key>"
)

// Using a SAS token
spark.sparkContext.hadoopConfiguration.set(
  "fs.azure.sas.<container-name>.<storage-account-name>.blob.core.windows.net",
  "<complete-query-string-of-sas-for-the-container>"
)

# COMMAND ----------

# MAGIC %md #Method4: mount the Azure storage container to Databricks with SAS Key

# COMMAND ----------

# MAGIC %scala
# MAGIC val storageAccount = "cheprasas"
# MAGIC val container = "carona"
# MAGIC val sasKey = "?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&st=2020-03-27T03%3A53%3A03Z&se=2020-03-28T03%3A53%3A03Z&sig=fFJPnTphXT5WQ9rNoCS9ESXZS9ooyrcLSkP%2BG893tGY%3D"
# MAGIC val mountPoint = s"/mnt/qa"
# MAGIC 
# MAGIC try {
# MAGIC   dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
# MAGIC } catch {
# MAGIC   case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
# MAGIC }
# MAGIC  
# MAGIC val sourceString = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
# MAGIC val confKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"
# MAGIC  
# MAGIC   dbutils.fs.mount(
# MAGIC     source = sourceString,
# MAGIC     mountPoint = mountPoint,
# MAGIC     extraConfigs = Map(confKey -> sasKey)
# MAGIC   )

# COMMAND ----------

