import uuid
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional, Union

from parsl.errors import OptionalModuleMissing
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin

UUID_LIKE_T = Union[uuid.UUID, str]


class GlobusComputeExecutor(ParslExecutor, RepresentationMixin):
    """ GlobusComputeExecutor enables remote execution on Globus Compute endpoints

    GlobusComputeExecutor is a thin wrapper over globus_compute_sdk.Executor
    Refer to `globus-compute user documentation <https://globus-compute.readthedocs.io/en/latest/executor.html>`_
    and `reference documentation <https://globus-compute.readthedocs.io/en/latest/reference/executor.html>`_
    for more details.
    """

    def __init__(
            self,
            endpoint_id: Optional[UUID_LIKE_T] = None,
            task_group_id: Optional[UUID_LIKE_T] = None,
            resource_specification: Optional[Dict[str, Any]] = None,
            user_endpoint_config: Optional[Dict[str, Any]] = None,
            label: str = "GlobusComputeExecutor",
            batch_size: int = 128,
            amqp_port: Optional[int] = None,
            **kwargs,
    ):
        """
        Parameters
        ----------

        endpoint_id:
            id of the endpoint to which to submit tasks

        task_group_id:
            The Task Group to which to associate tasks.  If not set,
            one will be instantiated.

        resource_specification:
            Specify resource requirements for individual task execution.

        user_endpoint_config:
            User endpoint configuration values as described
            and allowed by endpoint administrators. Must be a JSON-serializable dict
            or None.

        label:
            a label to name the executor; mainly utilized for
            logging and advanced needs with multiple executors.

        batch_size:
            the maximum number of tasks to coalesce before
            sending upstream [min: 1, default: 128]

        amqp_port:
            Port to use when connecting to results queue. Note that the
            Compute web services only support 5671, 5672, and 443.

        kwargs:
            Other kwargs listed will be passed through to globus_compute_sdk.Executor
            as is
        """
        super().__init__()
        self.endpoint_id = endpoint_id
        self.task_group_id = task_group_id
        self.resource_specification = resource_specification
        self.user_endpoint_config = user_endpoint_config
        self.label = label
        self.batch_size = batch_size
        self.amqp_port = amqp_port

        try:
            from globus_compute_sdk import Executor
        except ImportError:
            raise OptionalModuleMissing(
                ['globus-compute-sdk'],
                "GlobusComputeExecutor requires globus-compute-sdk installed"
            )
        self._executor: Executor = Executor(
            endpoint_id=endpoint_id,
            task_group_id=task_group_id,
            resource_specification=resource_specification,
            user_endpoint_config=user_endpoint_config,
            label=label,
            batch_size=batch_size,
            amqp_port=amqp_port,
            **kwargs
        )

    def start(self) -> None:
        """Empty function
        """
        pass

    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """ Submit fn to globus-compute


        Parameters
        ----------

        func: Callable
            Python function to execute remotely
        resource_specification: Dict[str, Any]
            Resource specification used to run MPI applications on Endpoints configured
            to use globus compute's MPIEngine
        args:
            Args to pass to the function
        kwargs:
            kwargs to pass to the function

        Returns
        -------

        Future
        """
        self._executor.resource_specification = resource_specification or self.resource_specification
        return self._executor.submit(func, *args, **kwargs)

    def shutdown(self, wait=True, *, cancel_futures=False):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other methods
        can be called after this one.

        Parameters
        ----------

        wait: If True, then this method will not return until all pending
            futures have received results.
        cancel_futures: If True, then this method will cancel all futures
            that have not yet registered their tasks with the Compute web services.
            Tasks cannot be cancelled once they are registered.
        """
        return self._executor.shutdown()
