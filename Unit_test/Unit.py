# Databricks notebook source
# MAGIC %md
# MAGIC Build a simple ETL using PySpark.
# MAGIC 
# MAGIC Implement unit tests using the python module unittest.
# MAGIC 
# MAGIC Run automated tests using CI/CD pipeline in Jenkins.

# COMMAND ----------

import unittest
#from etl.etl import transform_data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


def transform_data(input_df):
    transformed_df = (input_df.groupBy('Location',).agg(sum('ItemCount').alias('TotalItemCount')))
    return transformed_df
