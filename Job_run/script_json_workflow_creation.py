# Databricks notebook source
import requests
import pandas as pd
import json
import os

# COMMAND ----------

body_json = """
   {
        "name": "new_mlop_test11",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Test_gdp_git_commit",
                "notebook_task": {
                    "notebook_path": "Notebook/GDP_ingestion",
                    "source": "GIT"
                },
                "job_cluster_key": "Test_gdp_git_commit_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "FS_UPDATE",
                "depends_on": [
                    {
                        "task_key": "Test_gdp_git_commit"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "Feature_store/Feature_store",
                    "source": "GIT"
                },
                "job_cluster_key": "Test_gdp_git_commit_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Test_gdp_git_commit_cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "11.1.x-cpu-ml-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.databricks.passthrough.enabled": "true"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1.0
                    },
                    "node_type_id": "Standard_DS3_v2",
                    "enable_elastic_disk": true,
                    "runtime_engine": "STANDARD",
                    "num_workers": 8
                }
            }
        ],
        "git_source": {
            "git_url": "https://github.gapinc.com/Enterprise-Analytics/PT-Databricksfeaturestore.git",
            "git_provider": "gitHubEnterprise",
            "git_branch": "PTFE-2103"
        },
        "format": "MULTI_TASK"
    }
"""

print("Request body in json format:")
print(body_json)

# COMMAND ----------

DOMAIN = 'adb-3452376949060279.19.azuredatabricks.net'
TOKEN = 'dapi01f73fd71715c387753168d7a31db5d9-3'
response_jobrun = requests.post(
  'https://%s/api/2.0/jobs/create' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN}
,data=body_json)

if response_jobrun .status_code == 200:
    print("Job created successfully!")
    print(response_jobrun.status_code)
    print(response_jobrun.content)
else:
    print("job failed!")

# COMMAND ----------


