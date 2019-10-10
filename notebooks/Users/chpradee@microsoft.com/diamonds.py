# Databricks notebook source
# MAGIC %sql DROP TABLE IF EXISTS diamonds;
# MAGIC 
# MAGIC CREATE TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
# MAGIC diamonds.write.format("delta").save("/delta/diamonds")

# COMMAND ----------

# MAGIC %sql SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY COLOR

# COMMAND ----------

import numpy as np
import pandas as pd

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Generate a pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))

# Create a Spark DataFrame from a pandas DataFrame using Arrow
df = spark.createDataFrame(pdf)

# Convert the Spark DataFrame back to a pandas DataFrame using Arrow
result_pdf = df.select("*").toPandas()

# COMMAND ----------

import requests
r = requests.get("https://timeseries.surge.sh/usd_to_eur.csv")
df = spark.read.csv(sc.parallelize(r.text.splitlines()), header=True, inferSchema=True)
display(df)

# COMMAND ----------

