# Databricks notebook source
df1=spark.sql("select * from hive_metastore.fsstore.testnew")
df1.show()


# COMMAND ----------

feature_table_name='feature_store_DB.customer_data1',

# COMMAND ----------

from databricks.feature_store import feature_table

def compute_customer_features(data):
  ''' Feature computation code returns a DataFrame with 'customer_id' as primary key'''
  pass

# create feature table keyed by customer_id
# take schema from DataFrame output by compute_customer_features
from databricks.feature_store import FeatureStoreClient

customer_features_df = compute_customer_features(df1)

fs = FeatureStoreClient()

customer_feature_table=fs.create_table(
    name='feature_store_DB.customer_data1',
    primary_keys="location_id",
    df=df1,
#    schema=df.schema,
    description="Sample feature table",
)

# COMMAND ----------

fs.write_table(
  name='feature_store_DB.customer_data1',
  df=df1,
  mode="merge",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ls "/mnt/gdpingestion/testdataframe"
