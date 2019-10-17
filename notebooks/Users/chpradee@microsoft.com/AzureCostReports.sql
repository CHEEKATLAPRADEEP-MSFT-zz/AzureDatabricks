-- Databricks notebook source
-- MAGIC %md # This notebook is dedicated to Azure Cost Analysis Reports

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **To get the sample data you may follow the steps mentioned below:**
-- MAGIC 
-- MAGIC * Login to **Azure Portal**
-- MAGIC * Select **Azure Internal Consumption subscription**
-- MAGIC * Under **Cost Management**, select **Cost analysis**
-- MAGIC * Select **Cost by resource** and **Custom data**
-- MAGIC * Click on **Export**
-- MAGIC * Select the format you want to get the data **CSV**
-- MAGIC * Click on **Download Data**
-- MAGIC 
-- MAGIC ![Azurewala](https://chepra.blob.core.windows.net/images/GetData.jpg)

-- COMMAND ----------

-- MAGIC %md #Upload data to Databricks File System (DBFS):
-- MAGIC * Login to **Databricks Portal**
-- MAGIC * Click **Data** and **Add Data**
-- MAGIC 
-- MAGIC ![DataDataFile](https://chepra.blob.core.windows.net/images/DataData.jpg)
-- MAGIC 
-- MAGIC * Click on **Drop files to upload, or browse**
-- MAGIC * Select a **file from local machine to upload**
-- MAGIC 
-- MAGIC ![UploadData](https://chepra.blob.core.windows.net/images/UploadData.JPG)
-- MAGIC 
-- MAGIC * Select a **Cluster to Preview the Table**
-- MAGIC * Click on **Preview Table**
-- MAGIC 
-- MAGIC ![PreviewTable](https://chepra.blob.core.windows.net/images/UploadData.JPG)
-- MAGIC 
-- MAGIC * Modify **Table Name** and **Schema**
-- MAGIC * Select **First row is header**
-- MAGIC * Click **Create Table**
-- MAGIC 
-- MAGIC ![DefineSchema](https://chepra.blob.core.windows.net/images/DefineSchema.JPG)
-- MAGIC 
-- MAGIC * Table has created.

-- COMMAND ----------

-- MAGIC %md # Create a Notebook 
-- MAGIC 
-- MAGIC * Provide the name of **notebook**
-- MAGIC * Select Language **SQL**
-- MAGIC * Select Cluster **azure**
-- MAGIC 
-- MAGIC ![Notebook](https://chepra.blob.core.windows.net/images/Notebook.JPG)

-- COMMAND ----------

-- MAGIC %md # Run SQL Queries as Follows
-- MAGIC 
-- MAGIC * To get complete result from the table run following SQL Query
-- MAGIC 
-- MAGIC ```sql        
-- MAGIC SELECT * FROM TABLENAME
-- MAGIC ```

-- COMMAND ----------

SELECT * FROM azurecostanalysiss_csv

-- COMMAND ----------

-- MAGIC %md #Engineers who consumed HDInsight resources greater than 100 USD
-- MAGIC 
-- MAGIC * Use the following sql query to get this results
-- MAGIC 
-- MAGIC ```sql
-- MAGIC SELECT * FROM TABLENAME WHERE ResourceType="microsoft.hdinsight/clusters" AND PreTaxCost > 100
-- MAGIC ```

-- COMMAND ----------

SELECT * FROM azurecostanalysis_csv WHERE ResourceType="microsoft.hdinsight/clusters" AND PreTaxCost > 100

-- COMMAND ----------

-- MAGIC %md Display a chart.
-- MAGIC 
-- MAGIC * Click the **Bar chart icon** and  **Chart Button**
-- MAGIC 
-- MAGIC ![Plot](https://chepra.blob.core.windows.net/images/Plot.jpg)
-- MAGIC 
-- MAGIC * Click Plot Options.
-- MAGIC 
-- MAGIC * Drag **ResourceGroupName** into the Keys box.
-- MAGIC 
-- MAGIC * Drag **PreTaxCost** into the Values box.
-- MAGIC 
-- MAGIC * In the Aggregation drop-down, select **SUM**.
-- MAGIC 
-- MAGIC * Click on the **Plot View**
-- MAGIC 
-- MAGIC ![CustomPlot](https://chepra.blob.core.windows.net/images/Customizeplot.JPG)

-- COMMAND ----------

SELECT * FROM azurecostanalysis_csv WHERE ResourceType="microsoft.hdinsight/clusters" AND PreTaxCost > 100

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Thanks for using Azure Databricks
-- MAGIC 
-- MAGIC ##Please do pratice and come up with more questions
-- MAGIC 
-- MAGIC ### Please do provide your valuable feedback on the same