from flytekitplugins.dataproc import DataprocConfig, DataprocTask
from flytekit import kwtypes, task, workflow

@task
def call_dataproc():
    project = "wpna-poc-00001"
    location = "us-central1"
    dataprocTask = DataprocTask(
        name=f"hello_dataproc",
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
    
if __name__ == "__main__":
    # Execute the workflow by invoking it like a function and passing in
    # the necessary parameters
    print(f"Running wf() {wf()}")