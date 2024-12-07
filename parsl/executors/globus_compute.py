from __future__ import annotations

import copy
import uuid
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional, Union

import typeguard

from parsl.errors import OptionalModuleMissing
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin

try:
    from globus_compute_sdk import Executor
    _globus_compute_enabled = True
except ImportError:
    _globus_compute_enabled = False

UUID_LIKE_T = Union[uuid.UUID, str]


class GlobusComputeExecutor(ParslExecutor, RepresentationMixin):
    """ GlobusComputeExecutor enables remote execution on Globus Compute endpoints

    GlobusComputeExecutor is a thin wrapper over globus_compute_sdk.Executor
    Refer to `globus-compute user documentation <https://globus-compute.readthedocs.io/en/latest/executor.html>`_
    and `reference documentation <https://globus-compute.readthedocs.io/en/latest/reference/executor.html>`_
    for more details.

    .. note::
       As a remote execution system, Globus Compute relies on serialization to ship
       tasks and results between the Parsl client side and the remote Globus Compute
       Endpoint side. Serialization is unreliable across python versions, and
       wrappers used by Parsl assume identical Parsl versions across on both sides.
       We recommend using matching Python, Parsl and Globus Compute version on both
       the client side and the endpoint side for stable behavior.

    """

    @typeguard.typechecked
    def __init__(
            self,
            executor: Executor,
            label: str = 'GlobusComputeExecutor',
            resource_specification: Optional[dict] = None,
            user_endpoint_config: Optional[dict] = None,
    ):
        """
        Parameters
        ----------

        executor: globus_compute_sdk.Executor
            Pass a globus_compute_sdk Executor that will be used to execute
            tasks on a globus_compute endpoint. Refer to `globus-compute docs
            <https://globus-compute.readthedocs.io/en/latest/reference/executor.html#globus-compute-executor>`_

        label:
            a label to name the executor

        resource_specification:
            Specify resource requirements for individual task execution.

        user_endpoint_config:
            User endpoint configuration values as described
            and allowed by endpoint administrators. Must be a JSON-serializable dict
            or None. Refer docs from `globus-compute
            <https://globus-compute.readthedocs.io/en/latest/endpoints/endpoints.html#templating-endpoint-configuration>`_
            for more info.

        """
        if not _globus_compute_enabled:
            raise OptionalModuleMissing(
                ['globus-compute-sdk'],
                "GlobusComputeExecutor requires globus-compute-sdk installed"
            )

        super().__init__()
        self.executor: Executor = executor
        self.resource_specification = resource_specification
        self.user_endpoint_config = user_endpoint_config
        self.label = label

    def start(self) -> None:
        """ Start the Globus Compute Executor """
        pass

    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """ Submit func to globus-compute


        Parameters
        ----------

        func: Callable
            Python function to execute remotely

        resource_specification: Dict[str, Any]
            Resource specification can be used specify MPI resources required by MPI applications on
            Endpoints configured to use globus compute's MPIEngine. GCE also accepts *user_endpoint_config*
            to configure endpoints when the endpoint is a `Multi-User Endpoint
            <https://globus-compute.readthedocs.io/en/latest/endpoints/endpoints.html#templating-endpoint-configuration>`_

        args:
            Args to pass to the function

        kwargs:
            kwargs to pass to the function

        Returns
        -------

        Future
        """
        res_spec = copy.deepcopy(resource_specification or self.resource_specification)
        # Pop user_endpoint_config since it is illegal in resource_spec for globus_compute
        if res_spec:
            user_endpoint_config = res_spec.pop('user_endpoint_config', self.user_endpoint_config)
        else:
            user_endpoint_config = self.user_endpoint_config

        self.executor.resource_specification = res_spec
        self.executor.user_endpoint_config = user_endpoint_config
        return self.executor.submit(func, *args, **kwargs)

    def shutdown(self):
        """Clean-up the resources associated with the Executor.

        GCE.shutdown will cancel all futures that have not yet registered with
        Globus Compute and will not wait for the launched futures to complete.
        """
        self.executor.shutdown(wait=False, cancel_futures=True)
