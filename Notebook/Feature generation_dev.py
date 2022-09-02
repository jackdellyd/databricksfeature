# Databricks notebook source
# MAGIC %md
# MAGIC **To get access to GDP storage account**
# MAGIC * step 1
# MAGIC 
# MAGIC GDP team do not give access at storage account level, access is given at table level.  
# MAGIC To request GDP data access, you need to create AD group from CIA access to GDP will be granted to the AD group, in future new team member will be added to the AD group.
# MAGIC 
# MAGIC Link to CIA :                                                                                                                                                                                                                           <a href="https://gaptech.service-now.com/trc?id=kb_article_view&sys_kb_id=4ad4b7001b43cd1067b9da49b04bcb92" target="_blank">gaptech/cia.</a>  
# MAGIC **Note: I created AD group Azure-GdpFS-Dev.**

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 2
# MAGIC 
# MAGIC lists of tables that need access need to be provided to GDP team.
# MAGIC  <a href="https://folio.gap.com/:x:/r/sites/GapDataPlatform/_layouts/15/doc2.aspx?sourcedoc=%7B6f26062f-0520-4aec-9aca-67e324c6a966%7D&action=edit&activeCell=%27Prod%20Phase%201%27!C24&wdinitialsession=8953b90e-bd99-476f-95cd-953d0ee541a1&wdrldsc=8&wdrldc=1&wdrldr=AccessTokenExpiredWarning%2CRefreshingExpiredAccessT&cid=267d6aa0-659c-4c28-90de-30c0e9d473b3" target="_blank">gdp-table/lists/.</a>

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 3
# MAGIC Provide public subnet of the Databricks workspace to whitelist

# COMMAND ----------

# MAGIC %md
# MAGIC **There are 2 ways recommended to read from GDP**
# MAGIC - Reading the files from ADLS Gen-2 using AAD authentication of the user(credential passthrough).
# MAGIC - Using service principle

# COMMAND ----------

# MAGIC %md
# MAGIC **Using credential Passthrough**  
# MAGIC Whatever access to user in azure storage will be replicated to Databricks workspace (RBAC/ACL)  
# MAGIC Enable credential Passthrough authentication in Azure Databricks Cluster only work in premium account and can be significantly expensive  
# MAGIC **Note for Passthrough authentication to work, user must have access to File level for it to work**

# COMMAND ----------

# MAGIC %md
# MAGIC **How to configure credential passthrough**  
# MAGIC Credential passthrough will allow you to authenticate ADLS Gen1 and ADLS Gen-2 using the same AAD login that is used to log into the Azure Databricks workspace. Credential passthrough helps you control what users can see in a container with the use of RBAC and ACLs.
# MAGIC You can configure credential passthrough for standard and high concurrency Databricks clusters. A standard cluster with credential passthrough is limited to only a single user. To support multiple users, you need to spin up credential passthrough so that it is enabled for a high concurrency cluster.
# MAGIC 
# MAGIC - When you create a cluster, set the Cluster Mode to Standard for  single user or high concurrency for  multiple users.  
# MAGIC - Under Advanced Options, select Enable credential passthrough for user-level data access and select the user name from the Single User Access drop-down.

# COMMAND ----------

#Reading the files from ADLS Gen-2 using AAD authentication of the user.
#Credential passthrough allows you to authenticate automatically to Azure Data Lake Storage from Azure Databricks clusters using the identity that you use to log in to Azure Databricks
df = spark.read.format("delta").load("abfss://location@gsta2dgdprawzone02.dfs.core.windows.net/location/location/data")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Azure Data Lake Storage to DBFS using credential passthrough
# MAGIC You can mount an Azure Data Lake Storage account or a folder inside it to Databricks File System (DBFS). The mount is a pointer to a data lake store, so the data is never synced locally.
# MAGIC  When you mount data using a cluster enabled with Azure Data Lake Storage credential passthrough, any read or write to the mount point uses your Azure AD credentials. This mount point will be visible to other users, but the only users that will have read and write access are those who:
# MAGIC  - Have access to the underlying Azure Data Lake Storage storage account
# MAGIC - Are using a cluster enabled for Azure Data Lake Storage credential passthrough

# COMMAND ----------

dbutils.fs.mount(

  source = "abfss://container@storageaccountname.blob.core.windows.net/",

   mount_point = "/mnt/test-acls-jb/test123",

   extra_configs = configs)

# COMMAND ----------

from pyspark.sql.functions import count_distinct
df.select(count_distinct("location_id")).show()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
def remove_some_chars(col_name):
    removed_chars = ("/", "_","-")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")

# COMMAND ----------

def multi_remove_some_chars(col_names):
    def inner(df):
        for col_name in col_names:
            df = df.withColumn(
                col_name,
                remove_some_chars(col_name)
            )
        return df
    return inner

# COMMAND ----------

df2=multi_remove_some_chars(["location_name", "gdp_last_processed_by"])(df)

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql import functions as f
df4=df2.withColumn("gdp_processed_new_times",f.to_date("gdp_processed_timestamp"))
 
        

# COMMAND ----------

display(df4)

# COMMAND ----------

from pyspark.sql.functions import col
df1 = df4.select(
    col('gdp_processed_new_times').alias('processed_timestamp'), col('gdp_last_processed_by').alias('last_processed_by'),
    col('epoch_id'),col('location_id'),col('record_id'),col('location_number'),col('location_name'),
    col('source_brand_description').alias('brand_description'),col('enterprise_brand_code').alias('brand_code'),col('enterprise_market_code').alias('market_code'),
    col('enterprise_channel_code'),col('merchandise_channel_code'),col('enterprise_brand_number'),col('enterprise_market_number'),col('enterprise_channel_number'),col('merchandise_channel_number'),
    col('enterprise_brand_description')
    
    
)
df1.show()

# COMMAND ----------

#Create a feature table by instantiating a FeatureStoreClient
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table
#from databricks import feature_store
#feature_store_uri = f'databricks://devscope_james:fss'
fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mount it to external storage account not dbfs---
# MAGIC -- Before you create feature tables in the remote feature store, you must create a database to store them. The database must exist in the shared DBFS location in other to share features.
# MAGIC -- Before creating any feature tables, you must create a database to store them.
# MAGIC  CREATE DATABASE IF NOT EXISTS feature_store_DB LOCATION '/mnt/shared'

# COMMAND ----------

feature_table_name='feature_store_DB.customer_data1',
 

# COMMAND ----------

# Use this command with Databricks Runtime 10.2 ML or above
# if condition to update table 
fs.create_table(
    name='feature_store_DB.customer_data1',
    primary_keys="location_id",
    df=df1,
#    schema=df.schema,
    description="Sample feature table",
)
    

# COMMAND ----------

#test wed
from pyspark.sql.functions import col
df12 = df4.select(
    col('gdp_processed_new_times').alias('processed_timestamp'), col('gdp_last_processed_by').alias('last_processed_by'),
    col('epoch_id'),col('location_id'),col('record_id'),col('location_number'),col('location_name'),
    col('source_brand_description').alias('brand_description'),col('enterprise_brand_code').alias('brand_code'),col('enterprise_market_code').alias('market_code'),
    col('enterprise_channel_code'),col('merchandise_channel_code'),col('enterprise_brand_number'),col('enterprise_market_number'),col('enterprise_channel_number'),col('merchandise_channel_number'),
    col('enterprise_brand_description')
    
    
)
df12.show()

# COMMAND ----------

# MAGIC %md
# MAGIC how to get access to gdp data in dev

# COMMAND ----------

2 + 3
#testing

# COMMAND ----------

# MAGIC %python
# MAGIC assert spark.table("testnew"),"Table named `testnew` does not exist"
