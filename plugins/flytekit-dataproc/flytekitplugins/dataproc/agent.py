import datetime
import re
from dataclasses import dataclass
from typing import Dict, Optional

from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import Batch

from flytekit import FlyteContextManager, StructuredDataset, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

@dataclass
class DataprocMetadata(ResourceMeta):
    project: str
    location: str
    batch_name: str

class DataprocAgent(AsyncAgentBase):
    name = "Dataproc Agent"

    def __init__(self):
        super().__init__(task_type_name="dataproc", metadata_type=DataprocMetadata)

    def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> DataprocMetadata:
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " create() ")
        print(f"task_template: {task_template}")
        print(f"inputs: {inputs}")
        
        if inputs:
            ctx = FlyteContextManager.current_context()
            print(f"ctx: {ctx}")
            python_interface_inputs = {
                name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
            }
            print(f"python_interface_inputs: {python_interface_inputs}")
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            print(f"native_inputs: {native_inputs}")
            
        custom = task_template.custom
        project = custom["ProjectID"]
        location = custom["Location"]
        main_python_file_uri = custom["MainPythonFileUri"]
        spark_history_dataproc_cluster = custom["SparkHistoryDataprocCluster"]
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{location}-dataproc.googleapis.com:443"
        })

        # Initialize request argument(s)
        batch = dataproc_v1.Batch()
        batch.pyspark_batch.main_python_file_uri = main_python_file_uri
        batch.environment_config.peripherals_config.spark_history_server_config.dataproc_cluster = spark_history_dataproc_cluster

        request = dataproc_v1.CreateBatchRequest(
            parent=f"projects/{project}/locations/{location}",
            batch=batch,
        )

        # Make the request
        operation = client.create_batch(request=request)
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " Waiting for operation to complete...")

        batch_name = None
        try: 
            response = operation.result()
            batch_name = response.name
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " response: " + str(response))
        except Exception as e:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " Exception : -----------")
            # logger.error("failed to run Dataproc job with error:", e.message)
            # print("failed to run Dataproc job with error:", e.message)
            match = re.search(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', e.message)
            if match:
                uuid = match.group(0)
                print(f"Extracted UUID: {uuid}")
            else:
                print("UUID not found in the error message.")
            batch_name = f"projects/{project}/locations/{location}/batches/{uuid}"

        return DataprocMetadata(batch_name=batch_name, location=location, project=project)

    def get(self, resource_meta: DataprocMetadata, **kwargs) -> Resource:
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " get()")
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{resource_meta.location}-dataproc.googleapis.com:443"
        })
        # Initialize request argument(s)
        request = dataproc_v1.GetBatchRequest(
            name=resource_meta.batch_name,
        )
        print(f"request: {request}") 
        # Make the request
        response = client.get_batch(request=request)
        print(f"response: {response}") 
        log_link = TaskLog(
            uri=response.runtime_info.endpoints["Spark History Server"],
            name="Spark History Server",
        )

        cur_phase = TaskExecution.RUNNING
        res = None
        msg = None

        if response.state == Batch.State.STATE_UNSPECIFIED:
            cur_phase = TaskExecution.UNDEFINED
        elif response.state == Batch.State.SUCCEEDED:
            cur_phase = TaskExecution.SUCCEEDED
        elif response.state == Batch.State.PENDING:
            cur_phase = TaskExecution.INITIALIZING
        elif response.state == Batch.State.RUNNING:
            cur_phase = TaskExecution.RUNNING
        elif response.state == Batch.State.CANCELLING:
            cur_phase = TaskExecution.RUNNING
        elif response.state == Batch.State.CANCELLED:
            cur_phase = TaskExecution.ABORTED
        elif response.state == Batch.State.FAILED:
            cur_phase = TaskExecution.FAILED
            msg = response.state_message
            
        print(f"cur_phase: {cur_phase}, response.state: {response.state}")
        return Resource(phase=cur_phase, message=msg, log_links=[log_link], outputs=res)

    def delete(self, resource_meta: DataprocMetadata, **kwargs):
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " delete()")
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{resource_meta.location}-dataproc.googleapis.com:443"
        })
        # Initialize request argument(s)
        request = dataproc_v1.DeleteBatchRequest(
            name=resource_meta.batch_name,
        )
        # Make the request
        client.delete_batch(request=request)

AgentRegistry.register(DataprocAgent())
