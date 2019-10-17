-- Databricks notebook source
Select * from azurecostanalysis_csv

-- COMMAND ----------

SELECT * FROM azurecostanalysis_csv WHERE ResourceType="microsoft.hdinsight/clusters" AND PreTaxCost > 100


-- COMMAND ----------

