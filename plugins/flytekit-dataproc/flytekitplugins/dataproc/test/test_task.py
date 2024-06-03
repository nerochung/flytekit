from flytekitplugins.dataproc import DataprocConfig, DataprocTask
from flytekit import kwtypes
import pandas as pd

project = "wpna-poc-00001"
location = "us-central1"
dataprocTask = DataprocTask(
    name=f"bigquery.doge_coin",
    inputs=kwtypes(version=int),
    task_config=DataprocConfig(
        ProjectID=project,
        Location=location,
        MainPythonFileUri="gs://toyota-woven/helloworld_pyspark.py",
        SparkHistoryDataprocCluster=f'projects/{project}/regions/{location}/clusters/dataproc-spark-server',
    ),
)
dataprocTask.execute()
