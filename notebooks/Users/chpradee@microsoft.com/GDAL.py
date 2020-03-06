# Databricks notebook source
# MAGIC %md **Setup GDAL Init Script
# MAGIC 1x Add the DBFS path dbfs:/databricks/scripts/gdal_install.sh to the cluster init scripts and restart the cluster after running this the first time.**

# COMMAND ----------

# --- Run 1x to setup the init script. ---
# Restart cluster after running.
dbutils.fs.put("/databricks/scripts/gdal_install.sh","""
#!/bin/bash
sudo add-apt-repository ppa:ubuntugis/ppa
sudo apt-get update
sudo apt-get install -y cmake gdal-bin libgdal-dev python3-gdal""",
True)

# COMMAND ----------

def file_exists(path):
  try:
    dbutils.fs.ls('dbfs:/myfolder2')
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

dbutils.fs.ls('dbfs:/myfolder2')

# COMMAND ----------

from py4j.protocol import Py4JJavaError
def path_exist(path):
    try:
        rdd = sc.textFile('/dbfs:/myfolder2')
        rdd.take(1)
        return True
    except Py4JJavaError as e:
        return False

# COMMAND ----------

try:
  dirs = dbutils.fs.ls ("dbfs:/myfolder2")
  pass
except IOError:
  print("The path does not exist")

# COMMAND ----------

try:
  dirs = dbutils.fs.ls ("dbfs:/myfolder")
  pass
except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
     except:
      print("The path does not exist")
    

# COMMAND ----------

# MAGIC %sh pip install psutil

# COMMAND ----------

dbutils.library.help()

# COMMAND ----------

dbutils.library.installPyPI("psutil")

# COMMAND ----------

