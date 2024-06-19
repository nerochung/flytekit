from datetime import datetime
import re
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, MutableSequence

from google.cloud import dataproc_v1
from google.cloud import storage
from google.cloud.dataproc_v1.types import Batch
from google.longrunning.operations_pb2 import CancelOperationRequest
from google.protobuf.timestamp_pb2 import Timestamp

from flytekit import FlyteContextManager, StructuredDataset, logger
from flyteidl.core.execution_pb2 import TaskExecution
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.models.core.execution import TaskLog
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
        
    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> DataprocMetadata:
        logger.info("Create()")
        logger.debug(f"task_template: {task_template}")
        logger.debug(f"inputs: {inputs}")
        
        if (task_template.custom["MainPythonFileUri"] != None and not is_valid_gcs_path(task_template.custom["MainPythonFileUri"])):
            raise FlyteUserException(f"The path '{task_template.custom['MainPythonFileUri']}' is invalid.")
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
            
        batch_id = str(uuid.uuid4())
        batch_name = f"projects/{task_template.custom['ProjectID']}/locations/{task_template.custom['Location']}/batches/{batch_id}"

        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{task_template.custom['Location']}-dataproc.googleapis.com:443"
        })

        # Initialize request argument(s)
        batch = dataproc_v1.Batch()
        batch.name = batch_name
        batch.pyspark_batch.main_python_file_uri = task_template.custom["MainPythonFileUri"]
        batch.pyspark_batch.args = args
        batch.environment_config.peripherals_config.spark_history_server_config.dataproc_cluster = task_template.custom["SparkHistoryDataprocCluster"]
        batch.runtime_config.container_image = task_template.custom["ContainerImage"]
        batch.runtime_config.version = task_template.custom["RuntimeConfigVersion"]
        batch.runtime_config.properties = task_template.custom["RuntimeConfigProperties"]
        batch.environment_config.execution_config.service_account = task_template.custom["ServiceAccount"]
        batch.environment_config.execution_config.network_tags = task_template.custom["NetworkTags"]
        batch.environment_config.execution_config.kms_key = task_template.custom["KmsKey"]
        batch.environment_config.execution_config.network_uri = task_template.custom["NetworkUri"]
        batch.environment_config.execution_config.subnetwork_uri = task_template.custom["SubnetworkUri"]
            
        request = dataproc_v1.CreateBatchRequest(
            batch_id = batch_id,
            parent = f"projects/{task_template.custom['ProjectID']}/locations/{task_template.custom['Location']}",
            batch = batch,
        )

        # Make the request
        operation = client.create_batch(request=request)
            
        return DataprocMetadata(project=task_template.custom['ProjectID'], location=task_template.custom['Location'], batch_name=batch_name)

    async def get(self, resource_meta: DataprocMetadata, **kwargs) -> Resource:
        logger.info("get()")
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{resource_meta.location}-dataproc.googleapis.com:443"
        })
        # Initialize request argument(s)
        request = dataproc_v1.GetBatchRequest(
            name=resource_meta.batch_name,
        )
        # Make the request
        response = client.get_batch(request=request)
        logger.debug(f"response: {response}")
        
        cur_phase = TaskExecution.RUNNING
        end_time = ""
        res = None
        msg = None
        
        if response.state == Batch.State.STATE_UNSPECIFIED.value:
            cur_phase = TaskExecution.UNDEFINED
            msg = "STATE_UNSPECIFIED"
        elif response.state == Batch.State.SUCCEEDED.value:
            cur_phase = TaskExecution.SUCCEEDED
            msg = "SUCCEEDED"
            end_time = response.state_time.rfc3339()
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
            end_time = response.state_time.rfc3339()
        elif response.state == Batch.State.FAILED.value:
            cur_phase = TaskExecution.FAILED
            msg = "FAILED"
            end_time = response.state_time.rfc3339()
            #msg = response.state_message

        log_links: List[TaskLog] = []
        log_link = TaskLog(
            uri=f"https://console.cloud.google.com/dataproc/batches/{resource_meta.location}/{response.labels['goog-dataproc-batch-id']}?project={resource_meta.project}",
            name="Dataproc Serverless Batch",
        ).to_flyte_idl()
        log_links.append(log_link)
        log_link = TaskLog(
            uri=response.runtime_info.endpoints["Spark History Server"],
            name="Spark History Server",
        ).to_flyte_idl()
        log_links.append(log_link)
        log_link = TaskLog(
            uri=f"https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_dataproc_batch%22%0Aresource.labels.project_id%3D%22{resource_meta.project}%22%0Aresource.labels.location%3D%22{resource_meta.location}%22%0Aresource.labels.batch_id%3D%22{response.labels['goog-dataproc-batch-id']}%22%0AjsonPayload.component%3D%22driver%22;startTime={response.create_time.rfc3339()};endTime={end_time}?project={resource_meta.project}",
            name="Spark Driver Logs",
        ).to_flyte_idl()
        log_links.append(log_link)
        log_link = TaskLog(
            uri=f"https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_dataproc_batch%22%0Aresource.labels.project_id%3D%22{resource_meta.project}%22%0Aresource.labels.location%3D%22{resource_meta.location}%22%0Aresource.labels.batch_id%3D%22{response.labels['goog-dataproc-batch-id']}%22%0AjsonPayload.component%3D%22executor%22;startTime={response.create_time.rfc3339()};endTime={end_time}?project={resource_meta.project}",
            name="Spark Executor Logs",
        ).to_flyte_idl()
        log_links.append(log_link)
        
        logger.info(f"cur_phase: {cur_phase}, response.state: {Batch.State(response.state).name}")
        res = Resource(phase=cur_phase, message=msg, log_links=log_links)
        return res

    async def delete(self, resource_meta: DataprocMetadata, **kwargs):
        logger.info("delete()")
        # Create a client
        client = dataproc_v1.BatchControllerClient(client_options={
            "api_endpoint": f"{resource_meta.location}-dataproc.googleapis.com:443"
        })
        # Initialize request argument(s)
        get_batch_request = dataproc_v1.GetBatchRequest(
            name=resource_meta.batch_name,
        )
        # Make the request
        response = client.get_batch(request=get_batch_request)
        # Initialize request argument(s)
        cancel_operation_request = CancelOperationRequest(
            name=response.operation,
        )
        # Make the request
        client.cancel_operation(request=cancel_operation_request)

def is_valid_gcs_path(path):
    # only check gs://, do not check local file files:// in container
    isValid = True
    try:
        if path.startswith('gs://'):
            # Create a storage client
            storage_client = storage.Client()
            path = path.split('gs://')[1]
            bucket_name, object_name = path.split('/', 1)
            bucket = storage_client.get_bucket(bucket_name)
            isValid = bucket.blob(object_name).exists()
    except Exception as e:
        logger.debug(str(e))
        raise FlyteUserException(e.message)
        isValid = False
    return isValid

AgentRegistry.register(DataprocAgent())
