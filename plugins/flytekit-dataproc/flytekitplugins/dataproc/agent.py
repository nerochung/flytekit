import datetime
import re
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, MutableSequence

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
        logger.info("Create()")
        logger.debug(f"task_template: {task_template}")
        logger.debug(f"inputs: {inputs}")
        
        args = []
        if inputs:
            ctx = FlyteContextManager.current_context()
            python_interface_inputs = {
                name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
            }
            logger.debug(f"python_interface_inputs: {python_interface_inputs}")
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            logger.debug(f"native_inputs: {native_inputs}")
            for key, value in native_inputs.items():
                args.extend([f"--{key}", str(value)])
            # args = [(f"--{key}", value) for key, value in native_inputs.items()]
            logger.debug(f"args(): {args}")
            
        custom = task_template.custom
        project = custom["ProjectID"]
        location = custom["Location"]
        main_python_file_uri = custom["MainPythonFileUri"]
        batch_id = str(uuid.uuid4())
        batch_name = f"projects/{project}/locations/{location}/batches/{batch_id}"

        spark_history_dataproc_cluster = custom["SparkHistoryDataprocCluster"]
        container_image = custom["ContainerImage"]
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{location}-dataproc.googleapis.com:443"
        })

        # Initialize request argument(s)
        batch = dataproc_v1.Batch()
        batch.name = batch_name
        batch.pyspark_batch.main_python_file_uri = main_python_file_uri
        batch.pyspark_batch.args = args
        batch.environment_config.peripherals_config.spark_history_server_config.dataproc_cluster = spark_history_dataproc_cluster
        batch.runtime_config.container_image = container_image
        
        request = dataproc_v1.CreateBatchRequest(
            batch_id = batch_id,
            parent = f"projects/{project}/locations/{location}",
            batch = batch,
        )

        # Make the request
        operation = client.create_batch(request=request)
        return DataprocMetadata(batch_name=batch_name, location=location, project=project)

    def get(self, resource_meta: DataprocMetadata, **kwargs) -> Resource:
        logger.info("get()")
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{resource_meta.location}-dataproc.googleapis.com:443"
        })
        # Initialize request argument(s)
        request = dataproc_v1.GetBatchRequest(
            name=resource_meta.batch_name,
        )
        # print(f"request: {request}") 
        # Make the request
        response = client.get_batch(request=request)
        logger.debug(f"response: {response}") 
        log_link = TaskLog(
            uri=response.runtime_info.endpoints["Spark History Server"],
            name="Spark History Server",
        )

        cur_phase = TaskExecution.RUNNING
        res = None
        msg = None
        
        if response.state == Batch.State.STATE_UNSPECIFIED.value:
            cur_phase = TaskExecution.UNDEFINED
            msg = "STATE_UNSPECIFIED"
        elif response.state == Batch.State.SUCCEEDED.value:
            cur_phase = TaskExecution.SUCCEEDED
            msg = "SUCCEEDED"
        elif response.state == Batch.State.PENDING.value:
            cur_phase = TaskExecution.INITIALIZING
            msg = "PENDING"
        elif response.state == Batch.State.RUNNING.value:
            cur_phase = TaskExecution.RUNNING
            msg = "RUNNING"
        elif response.state == Batch.State.CANCELLING.value:
            cur_phase = TaskExecution.RUNNING
            msg = "CANCELLING"
        elif response.state == Batch.State.CANCELLED.value:
            cur_phase = TaskExecution.ABORTED
            msg = "CANCELLED"
        elif response.state == Batch.State.FAILED.value:
            cur_phase = TaskExecution.FAILED
            msg = "FAILED"
            #msg = response.state_message
        # logger.info(f"cur_phase: {TaskExecution.Phase(cur_phase).name}, response.state: {Batch.State(response.state).name}")
        logger.info(f"cur_phase: {cur_phase}, response.state: {Batch.State(response.state).name}")
        return Resource(phase=cur_phase, message=msg, log_links=[log_link])

    def delete(self, resource_meta: DataprocMetadata, **kwargs):
        logger.info("delete()")
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
