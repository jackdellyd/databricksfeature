# Databricks notebook source
# MAGIC %md
# MAGIC * Accessing files directly from ADLS Gen-2 without mounting to DBFS  
# MAGIC We can access any ADLS Gen-2 account that the service principal has access to.

# COMMAND ----------

# MAGIC %md
# MAGIC <scope> with the Databricks secret scope name.   
# MAGIC <service-credential-key> with the name of the key containing the client secret.  
# MAGIC <storage-account> with the name of the Azure storage account.  
# MAGIC <application-id> with the Application (client) ID for the Azure Active Directory application.  
# MAGIC <directory-id> with the Directory (tenant) ID for the Azure Active Directory application.  

# COMMAND ----------

# MAGIC %md
# MAGIC service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")
# MAGIC 
# MAGIC spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id/Client-id>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="g-dtb-2p-gdp-*****",key="platformSpnClientId"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="g-dtb-2p-gdp-compute-cprod-scope-02",key="platformClientSecret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/348a1296-55b6-466e-a7af-4ad1a1b79713/oauth2/token"}
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://merchandisingfoundation@gsta2sgdpdatalakecur04.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/gsta2sgdpdatalakecur04/merchandisingfoundation/",
# MAGIC   extra_configs = configs)

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="g-dtb-2n-dta-entana-scope-01",key="entana-spn-dtb")

spark.conf.set("fs.azure.account.auth.type.gsta2dgdprawzone02.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.gsta2dgdprawzone02.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.gsta2dgdprawzone02.dfs.core.windows.net", "e75fa2cb-2812-491b-b443-26be8375c27a")
spark.conf.set("fs.azure.account.oauth2.client.secret.gsta2dgdprawzone02.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.gsta2dgdprawzone02.dfs.core.windows.net", "https://login.microsoftonline.com/348a1296-55b6-466e-a7af-4ad1a1b79713/oauth2/token")

# COMMAND ----------

#df = spark.read.format("delta").load("abfss://location@gsta2dgdprawzone02.dfs.core.windows.net/location/location/data")
#Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks
storageAccount="gsta2nentana01"
mountpoint = "/mnt/test-gdp"
storageEndPoint ="abfss://gdp-mount@{}.dfs.core.windows.net/".format(storageAccount)
print ('Mount Point ='+mountpoint)

#ClientId, TenantId and Secret is for the Application(ADLSGen2App) was have created as part of this recipe
clientID ="e75fa2cb-2812-491b-b443-26be8375c27a" #Called as Application Id as well
tenantID ="348a1296-55b6-466e-a7af-4ad1a1b79713"
clientSecret =dbutils.secrets.get(scope="g-dtb-2n-dta-entana-scope-01",key="entana-spn-dtb")
oauth2Endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(tenantID)


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": oauth2Endpoint}

try:
  dbutils.fs.mount(
  source = storageEndPoint,
  mount_point = mountpoint,
  extra_configs = configs)
except:
    print("Already mounted...."+mountpoint)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#dbutils.fs.ls("abfss://customer@gsta2dgdprawzone02.dfs.core.windows.net/customer_transaction/customer_order_sales/data")
dbutils.fs.ls(("abfss://location@gsta2dgdprawzone02.dfs.core.windows.net/location/location/data"))

# COMMAND ----------

df = spark.read.format("delta").load("abfss://location@gsta2dgdprawzone02.dfs.core.windows.net/location/location/data")
display(df)

# COMMAND ----------

df = spark.read.format("delta").load("abfss://customer@gsta2dgdprawzone02.dfs.core.windows.net/customer_transaction/customer_transaction/customer_order_sales/data")
display(df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC **Mounting ADLS Gen2 and Azure Blob storage to Azure DBFS**  
# MAGIC Azure Databricks uses DBFS, which is a distributed file system that is mounted into an Azure Databricks workspace and that can be made available on Azure Databricks clusters. DBFS is an abstraction that is built on top of Azure Blob storage and ADLS Gen2. It mainly offers the following benefits:  
# MAGIC 
# MAGIC - It allows you to mount the Azure Blob and ADLS Gen2 storage objects so that you can access files and folders without requiring any storage credentials.
# MAGIC - You can read files directly from the mount point without needing to provide a full storage Uniform Resource Locator (URL).
# MAGIC - You can create folders and write files directly to the mount point.
# MAGIC - Data written to the mount point gets persisted after a cluster is terminated.

# COMMAND ----------

#You can also check the files and folders using the dbutils command, as shown in the following code snippet
display(dbutils.fs.ls("/mnt/test-gdp"))
# Upon executing the preceding command, you should see all the folders and files you have created in the storage account.

# COMMAND ----------


