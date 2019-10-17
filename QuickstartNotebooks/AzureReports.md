# This notebook is dedicated to Azure Cost Analysis Reports

 # **Step1: Get the sample data to make Azure Cost Analysis Reports, you may follow the below steps:**

* Login to **Azure Portal**
* Select **Azure Internal Consumption subscription**
 * Under **Cost Management**, select **Cost analysis**
 * Select **Cost by resource** and **Custom data**
 * Click on **Export**
 * Select the format you want to get the data **CSV**
 * Click on **Download Data**
 
    ![Azurewala](https://chepra.blob.core.windows.net/images/GetData.jpg)

# Step2: Upload data to Databricks File System (DBFS):
 * Login to **Databricks Portal**
  * Click **Data** and **Add Data**
 
    ![DataDataFile](https://chepra.blob.core.windows.net/images/DataData.jpg)
 
 * Click on **Drop files to upload, or browse**
 * Select a **file from local machine to upload**
 
     ![UploadData](https://chepra.blob.core.windows.net/images/UploadData.JPG)
 
 * Select a **Cluster to Preview the Table**
 * Click on **Preview Table**
 
    ![PreviewTable](https://chepra.blob.core.windows.net/images/UploadData.JPG)
 
 * Modify **Table Name** and **Schema**
 * Select **First row is header**
 * Click **Create Table**
 
     ![DefineSchema](https://chepra.blob.core.windows.net/images/DefineSchema.JPG)
 
 * Table has created.



 # Step3:  Create a Notebook
 
 * Provide the name of **notebook**
 * Select Language **SQL**
 * Select Cluster **azure**
 
     ![Notebook](https://chepra.blob.core.windows.net/images/Notebook.JPG)



 # Step4: Run SQL Queries as Follows
 
 * To get complete result from the table run following SQL Query
 
 ```sql        
 SELECT * FROM azurecostanalysiss_csv
 ```







 # Step5: Engineers who consumed HDInsight resources greater than 100 USD
 
 * Use the following sql query to get this results
 
 ```sql
 SELECT * FROM azurecostanalysiss_csv WHERE ResourceType="microsoft.hdinsight/clusters" AND PreTaxCost > 100
 ```




 # Step6: Display a chart.
 
 * Click the **Bar chart icon** and  **Chart Button**
 
     ![Plot](https://chepra.blob.core.windows.net/images/Plot.jpg)
 
 * Click Plot Options.
 
 * Drag **ResourceGroupName** into the Keys box.
 
 * Drag **PreTaxCost** into the Values box.
 
 * In the Aggregation drop-down, select **SUM**.
 
 * Click on the **Plot View**
 
    ![CustomPlot](https://chepra.blob.core.windows.net/images/Customizeplot.JPG)





 # Thanks for using Azure Databricks
 
 ## Please do pratice and come up with more questions
 
 ### Please do provide your valuable feedback on the same
