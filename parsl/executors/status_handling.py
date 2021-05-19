import logging
import threading
from itertools import compress
from abc import abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import List, Any, Dict, Optional, Tuple, Union

import parsl  # noqa F401
from parsl.executors.base import ParslExecutor
from parsl.executors.errors import ScalingFailed
from parsl.providers.provider_base import JobStatus, ExecutionProvider, JobState


logger = logging.getLogger(__name__)


class BlockProviderExecutor(ParslExecutor):
    """A base class for executors which scale using blocks.

    This base class is intended to help with executors which:

    - use blocks of workers to execute tasks
    - blocks of workers are launched on a batch system through
      an `ExecutionProvider`

    An implementing class should implement the abstract methods required by
    `ParslExecutor` to submit tasks, as well as BlockProviderExecutor
    abstract methods to provide the executor-specific command to start a block
    of workers (the ``_get_launch_command`` method), and some basic scaling
    information (``outstanding`` and ``workers_per_node`` properties).

    This base class provides a ``scale_out`` method which will launch new
    blocks. It does not provide a ``scale_in`` method, because scale-in
    behaviour is not well defined in the Parsl scaling model and so behaviour
    is left to individual executors.

    Parsl scaling will provide scaling between min_blocks and max_blocks by
    invoking scale_out, but it will not initialize the blocks requested by
    any init_blocks parameter. Subclasses must implement that behaviour
    themselves.
    """
    def __init__(self, provider: ExecutionProvider):
        super().__init__()
        self._provider = provider
        # errors can happen during the submit call to the provider; this is used
        # to keep track of such errors so that they can be handled in one place
        # together with errors reported by status()
        self._simulated_status: Dict[Any, JobStatus] = {}
        self._executor_bad_state = threading.Event()
        self._executor_exception: Optional[Exception] = None
        self._generated_block_id_counter = 1
        self._tasks = {}  # type: Dict[object, Future]
        self.blocks = {}  # type: Dict[str, str]
        self.block_mapping = {}  # type: Dict[str, str]

    def _make_status_dict(self, block_ids: List[str], status_list: List[JobStatus]) -> Dict[str, JobStatus]:
        """Given a list of block ids and a list of corresponding status strings,
        returns a dictionary mapping each block id to the corresponding status

        :param block_ids: the list of block ids
        :param status_list: the list of job status strings
        :return: the resulting dictionary
        """
        if len(block_ids) != len(status_list):
            raise IndexError("block id list and status string list differ in size")
        d = {}
        for i in range(len(block_ids)):
            d[block_ids[i]] = status_list[i]

        return d

    def _set_provider(self, provider: ExecutionProvider):
        self._provider = provider

    @property
    def status_polling_interval(self):
        if self._provider is None:
            return 0
        else:
            return self._provider.status_polling_interval

    def _fail_job_async(self, block_id: Any, message: str):
        """Marks a job that has failed to start but would not otherwise be included in status()
        as failed and report it in status()
        """
        if block_id is None:
            block_id = "failed-block-{}".format(self._generated_block_id_counter)
            self._generated_block_id_counter += 1
        self._simulated_status[block_id] = JobStatus(JobState.FAILED, message)

    @abstractproperty
    def outstanding(self) -> int:
        """This should return the number of tasks that the executor has been given to run (waiting to run, and running now)"""

        raise NotImplementedError("Classes inheriting from BlockProviderExecutor must implement "
                                  "outstanding()")

    def status(self) -> Dict[str, JobStatus]:
        """Return status of all blocks."""

        if self._provider:
            block_ids, job_ids = self._get_block_and_job_ids()
            status = self._make_status_dict(block_ids, self._provider.status(job_ids))
        else:
            status = {}
        status.update(self._simulated_status)

        return status

    def set_bad_state_and_fail_all(self, exception: Exception):
        logger.exception("Exception: {}".format(exception))
        self._executor_exception = exception
        # Set bad state to prevent new tasks from being submitted
        self._executor_bad_state.set()
        # We set all current tasks to this exception to make sure that
        # this is raised in the main context.
        for task in self._tasks:
            self._tasks[task].set_exception(Exception(str(self._executor_exception)))

    @property
    def bad_state_is_set(self):
        return self._executor_bad_state.is_set()

    @property
    def executor_exception(self):
        return self._executor_exception

    @property
    def error_management_enabled(self):
        return True

    def handle_errors(self, error_handler: "parsl.dataflow.job_error_handler.JobErrorHandler",
                      status: Dict[str, JobStatus]) -> bool:
        init_blocks = 3
        if hasattr(self.provider, 'init_blocks'):
            init_blocks = self.provider.init_blocks  # type: ignore
        if init_blocks < 1:
            init_blocks = 1
        error_handler.simple_error_handler(self, status, init_blocks)
        return True

    @property
    def tasks(self) -> Dict[object, Future]:
        return self._tasks

    @property
    def provider(self):
        return self._provider

    def _filter_scale_in_ids(self, to_kill, killed):
        """ Filter out job id's that were not killed
        """
        assert len(to_kill) == len(killed)
        # Filters first iterable by bool values in second
        return list(compress(to_kill, killed))

    def scale_out(self, blocks: int = 1) -> List[str]:
        """Scales out the number of blocks by "blocks"
        """
        if not self.provider:
            raise (ScalingFailed(None, "No execution provider available"))
        block_ids = []
        for i in range(blocks):
            block_id = str(len(self.blocks))
            try:
                job_id = self._launch_block(block_id)
                self.blocks[block_id] = job_id
                self.block_mapping[job_id] = block_id
                block_ids.append(block_id)
            except Exception as ex:
                self._fail_job_async(block_id,
                                     "Failed to start block {}: {}".format(block_id, ex))
        return block_ids

    def _launch_block(self, block_id: str) -> Any:
        launch_cmd = self._get_launch_command(block_id)
        # if self.launch_cmd is None:
        #   raise ScalingFailed(self.provider.label, "No launch command")
        # launch_cmd = self.launch_cmd.format(block_id=block_id)
        job_id = self.provider.submit(launch_cmd, 1)
        logger.debug("Launched block {}->{}".format(block_id, job_id))
        if not job_id:
            raise(ScalingFailed(self.provider.label,
                                "Attempts to provision nodes via provider has failed"))
        return job_id

    @abstractmethod
    def _get_launch_command(self, block_id: str) -> str:
        pass

    def _get_block_and_job_ids(self) -> Tuple[List[str], List[Any]]:
        # Not using self.blocks.keys() and self.blocks.values() simultaneously
        # The dictionary may be changed during invoking this function
        # As scale_in and scale_out are invoked in multiple threads
        block_ids = list(self.blocks.keys())
        job_ids = []  # types: List[Any]
        for bid in block_ids:
            job_ids.append(self.blocks[bid])
        return block_ids, job_ids

    @abstractproperty
    def workers_per_node(self) -> Union[int, float]:
        pass


class NoStatusHandlingExecutor(ParslExecutor):
    def __init__(self):
        super().__init__()
        self._tasks = {}  # type: Dict[object, Future]

    @property
    def status_polling_interval(self):
        return -1

    @property
    def bad_state_is_set(self):
        return False

    @property
    def error_management_enabled(self):
        return False

    @property
    def executor_exception(self):
        return None

    def set_bad_state_and_fail_all(self, exception: Exception):
        pass

    def status(self):
        return {}

    def handle_errors(self, error_handler: "parsl.dataflow.job_error_handler.JobErrorHandler",
                      status: Dict[str, JobStatus]) -> bool:
        return False

    @property
    def tasks(self) -> Dict[object, Future]:
        return self._tasks

    @property
    def provider(self):
        return self._provider
