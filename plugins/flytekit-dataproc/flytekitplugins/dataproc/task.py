from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, MutableMapping, MutableSequence

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import FlyteContextManager, PythonFunctionTask, lazy_module, logger
from flytekit.configuration import SerializationSettings
# from flytekit.extend import SQLTask
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
# from flytekit.models import task as _task_model
# from flytekit.types.structured import StructuredDataset

@dataclass
class DataprocConfig(object):
    """
    DataprocConfig should be used to configure a Dataproc Task.
    """

    ProjectID: str
    Location: str
    MainPythonFileUri: str
    SparkHistoryDataprocCluster: str = None
    ContainerImage: str = None
    RuntimeConfigVersion: str = None
    RuntimeConfigProperties: MutableMapping[str, str] = None
    ServiceAccount: str = None
    NetworkTags: MutableSequence[str] = None
    KmsKey: str = None
    NetworkUri: str = None
    SubnetworkURI: str = None
    

class DataprocTask(AsyncAgentExecutorMixin, PythonTask[DataprocConfig]):
    """
    This is the simplest form of a Dataproc Task, that can be used even for tasks that do not produce any output.
    """

    # This task is executed using the Dataproc handler in the backend.
    # https://github.com/flyteorg/flyteplugins/blob/43623826fb189fa64dc4cb53e7025b517d911f22/go/tasks/plugins/webapi/Dataproc/plugin.go#L34
    _TASK_TYPE = "dataproc"

    def __init__(
        self,
        name: str = None,
        task_config: Optional[DataprocConfig] =None,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        config = {
            "Location": self.task_config.Location,
            "ProjectID": self.task_config.ProjectID,
            "MainPythonFileUri": self.task_config.MainPythonFileUri,
            "SparkHistoryDataprocCluster": self.task_config.SparkHistoryDataprocCluster,
            "ContainerImage": self.task_config.ContainerImage,
            "RuntimeConfigVersion": self.task_config.RuntimeConfigVersion,
            "RuntimeConfigProperties": self.task_config.RuntimeConfigProperties,
            "ServiceAccount": self.task_config.ServiceAccount,
            "NetworkTags": self.task_config.NetworkTags,
            "KmsKey": self.task_config.KmsKey,
            "NetworkUri": self.task_config.NetworkUri,
            "SubnetworkUri": self.task_config.SubnetworkURI,
        }
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)