# Databricks notebook source
ReadMultiple = spark.read.format("csv").option("header", "true").load("/sample/*.csv")
display(ReadMultiple)

# COMMAND ----------

