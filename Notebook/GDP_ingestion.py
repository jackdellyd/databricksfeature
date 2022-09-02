# Databricks notebook source
#Reading the files from ADLS Gen-2 using AAD authentication of the user.
#Credential passthrough allows you to authenticate automatically to Azure Data Lake Storage from Azure Databricks clusters using the identity that you use to log in to Azure Databricks
df = spark.read.format("delta").load("abfss://location@gsta2dgdprawzone02.dfs.core.windows.net/location/location/data")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks no longer recommends Access Azure Data Lake Storage using Azure Active Directory credential passthrough.  
# MAGIC  <a href="https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage" target="_blank">credential passthrough and mounting/Deprecated.</a>  

# COMMAND ----------

# MAGIC %md
# MAGIC display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://testgdp@gsta2nentana02.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/gdpingestion",
# MAGIC   extra_configs = {"fs.azure.account.key.gsta2nentana02.blob.core.windows.net":dbutils.secrets.get(scope = "dotdata", key = "gsta2nentana02keys")})

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.unmount("/mnt/gdpingestion")

# COMMAND ----------

dbutils.fs.ls("/mnt/gdpingestion")

# COMMAND ----------

# MAGIC %md
# MAGIC configs = { 
# MAGIC "fs.azure.account.auth.type": "CustomAccessToken",
# MAGIC "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
# MAGIC }
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC 
# MAGIC   source = "abfss://testgdp@gsta2nentana02.blob.core.windows.net/",
# MAGIC 
# MAGIC #   mount_point = "/mnt/test-acls-jb/ingestgdp",
# MAGIC     mount_point="/mnt/ingestiongdp",
# MAGIC 
# MAGIC    extra_configs = configs)

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
display(df2)

# COMMAND ----------

from pyspark.sql import functions as f
df4=df2.withColumn("gdp_processed_new_times",f.to_date("gdp_processed_timestamp"))
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

# MAGIC %md
# MAGIC save_path = "/mnt/gdpingestion/testdataframe_N"
# MAGIC write_format = 'delta'
# MAGIC df1.write \
# MAGIC   .format(write_format) \
# MAGIC   .save(save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ls "/mnt/gdpingestion/testdataframe"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/gdpingestion"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create a new database in the mounted filesystem
# MAGIC CREATE DATABASE IF NOT EXISTS FSstore
# MAGIC LOCATION "/mnt/gdpingestion"

# COMMAND ----------

df1.write.mode("overwrite").format("delta").saveAsTable("FSstore.testnew")

# COMMAND ----------

# MAGIC %md
# MAGIC  CREATE DATABASE IF NOT EXISTS feature_store_DB LOCATION "/mnt/gdpingest/gdpingestion"

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE testnew
# MAGIC using DELTA
# MAGIC LOCATION  "/mnt/gdpingest/gdpingestion"
