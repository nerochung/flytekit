from flytekitplugins.dataproc import DataprocConfig, DataprocTask
from flytekit import kwtypes, task, workflow

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

@task
def call_dataproc():
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

@workflow
def wf():
    call_dataproc()