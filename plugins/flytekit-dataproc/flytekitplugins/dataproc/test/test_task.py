from flytekitplugins.dataproc import DataprocConfig, DataprocTask
from flytekit import kwtypes, workflow

project = "<Project ID>"
location = "<Location>"
dataprocTask = DataprocTask(
    name=f"hello_dataproc",
    inputs=kwtypes(version=int),
    task_config=DataprocConfig(
        ProjectID=project,
        Location=location,
        MainPythonFileUri="gs://pyspark-hello-world.py",
        SparkHistoryDataprocCluster=f'projects/{project}/regions/{location}/clusters/dataproc-spark-server',
    ),
)
# dataprocTask.exeucte()
# print(dataprocTask(version=1))
@workflow
def wf():
    return dataprocTask(version=1)
