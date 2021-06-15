import threading
import queue
import time
import logging
import random
import os
import inspect
from concurrent.futures import Future
from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import NoStatusHandlingExecutor
from parsl.executors.errors import (
    BadMessage, ScalingFailed,
    DeserializationError, SerializationError,
    UnsupportedFeatureError
)
from parsl.app.errors import RemoteExceptionWrapper

from funcx.sdk.client import FuncXClient
from funcx.sdk.executor import FuncXExecutor as FXEX


logger = logging.getLogger(__name__)


class FuncXExecutor(NoStatusHandlingExecutor, RepresentationMixin):

    def __init__(self,
                 label="FuncXExecutor",
                 # provider=None,
                 endpoints=None,
                 workdir='FXEX',
                 managed=True):

        logger.info("Initializing FuncXExecutor")
        self.label = label
        self.endpoints = endpoints
        self.workdir = os.path.abspath(workdir)
        self.managed = managed

        self._scaling_enabled = False

        self._task_counter = 0
        self._tasks = {}

        # Create a funcx client and disable throttling
        self.fxc = FuncXClient(funcx_service_address='https://api.dev.funcx.org/v2/',
                               results_ws_uri='wss://api.dev.funcx.org/ws/v2/')
        self.fxc.throttling_enabled = False

        # Create a funcx executor
        self.fxex = FXEX(self.fxc)

    def start(self):
        """ Called when DFK starts the executor when the config is loaded
        1. Make workdir
        2. Start task_status_poller
        3. Create a funcx SDK client
        """
        os.makedirs(self.workdir, exist_ok=True)

        # Create a dict to record the function uuids for apps
        self.functions = {}
        # Record the mapping between funcX function uuids and internal task ids
        self.task_uuids = {}

        return []

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the the task_outgoing queue.
        The task_outgoing queue is an external process listens on this
        queue for new work. This method behaves like a
        submit call as described here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_
        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.
        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        Returns:
            Future
        """
        if resource_specification:
            logger.error("Ignoring the resource specification. "
                         "Parsl resource specification is not supported in FuncXExecutor. "
                         "Please check WorkQueueExecutor if resource specification is needed.")
            raise UnsupportedFeatureError('resource specification', 'FuncXExecutor', 'WorkQueue Executor')

        task_id = self._task_counter
        self._task_counter += 1

        # randomly select an endpoint to submit
        ep_id = random.choice(self.endpoints)
        fut = self.fxex.submit(func, *args, endpoint_id=ep_id, **kwargs)
        self.tasks[task_id] = fut
        return fut

    def shutdown(self):
        logger.info("FuncXExecutor shutdown")
        return True

    def scale_in(self):
        pass

    def scale_out(self):
        pass

    @property
    def scaling_enabled(self):
        return self._scaling_enabled
