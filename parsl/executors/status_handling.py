from __future__ import annotations
import logging
import threading
from itertools import compress
from abc import abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import List, Any, Dict, Optional, Tuple, Union, Callable

import parsl  # noqa F401
from parsl.executors.base import ParslExecutor
from parsl.executors.errors import BadStateException, ScalingFailed
from parsl.jobs.states import JobStatus, JobState
from parsl.jobs.error_handlers import simple_error_handler, noop_error_handler
from parsl.providers.base import ExecutionProvider
from parsl.utils import AtomicIDCounter

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

    BENC: TODO: block error handling: maybe I want this more user pluggable?
    I'm not sure of use cases for switchability at the moment beyond "yes or no"
    """
    def __init__(self, *,
                 provider: Optional[ExecutionProvider],
                 block_error_handler: Union[bool, Callable[[BlockProviderExecutor, Dict[str, JobStatus]], None]]):
        super().__init__()
        self._provider = provider
        self.block_error_handler: Callable[[BlockProviderExecutor, Dict[str, JobStatus]], None]
        if isinstance(block_error_handler, bool):
            if block_error_handler:
                self.block_error_handler = simple_error_handler
            else:
                self.block_error_handler = noop_error_handler
        else:
            self.block_error_handler = block_error_handler

        # errors can happen during the submit call to the provider; this is used
        # to keep track of such errors so that they can be handled in one place
        # together with errors reported by status()
        self._simulated_status: Dict[Any, JobStatus] = {}
        self._executor_bad_state = threading.Event()
        self._executor_exception: Optional[Exception] = None

        self._block_id_counter = AtomicIDCounter()

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

    @property
    def status_polling_interval(self):
        """Returns the interval, in seconds, at which the status method should be called. The
        assumption here is that, once initialized, an executor's polling interval is fixed.
        In practice, at least given the current situation, the executor uses a single task provider
        and this method is a delegate to the corresponding method in the provider.

        :return: the number of seconds to wait between calls to status() or zero if no polling
                 should be done
        """
        if self._provider is None:
            return 0
        else:
            return self._provider.status_polling_interval

    def _fail_job_async(self, block_id: Any, message: str):
        """Marks a job that has failed to start but would not otherwise be included in status()
        as failed and report it in status()
        """
        if block_id is None:
            block_id = str(self._block_id_counter.get_id())
            logger.info(f"Allocated block ID {block_id} for simulated failure")
        self._simulated_status[block_id] = JobStatus(JobState.FAILED, message)

    @abstractproperty
    def outstanding(self) -> int:
        """This should return the number of tasks that the executor has been given to run (waiting to run, and running now)"""

        raise NotImplementedError("Classes inheriting from BlockProviderExecutor must implement "
                                  "outstanding()")

    def status(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks currently known to this executor.

        :return: a dictionary mapping block ids (in string) to job status
        """
        if self._provider:
            block_ids, job_ids = self._get_block_and_job_ids()
            status = self._make_status_dict(block_ids, self._provider.status(job_ids))
        else:
            status = {}
        status.update(self._simulated_status)

        return status

    def set_bad_state_and_fail_all(self, exception: Exception):
        """Allows external error handlers to mark this executor as irrecoverably bad and cause
        all tasks submitted to it now and in the future to fail. The executor is responsible
        for checking  :method:bad_state_is_set() in the :method:submit() method and raising the
        appropriate exception, which is available through :method:executor_exception().
        """
        logger.exception("Setting bad state due to exception", exc_info=exception)
        self._executor_exception = exception
        # Set bad state to prevent new tasks from being submitted
        self._executor_bad_state.set()
        # We set all current tasks to this exception to make sure that
        # this is raised in the main context.
        for task in self._tasks:
            self._tasks[task].set_exception(BadStateException(self, self._executor_exception))

    @property
    def bad_state_is_set(self):
        """Returns true if this executor is in an irrecoverable error state. If this method
        returns true, :property:executor_exception should contain an exception indicating the
        cause.
        """
        return self._executor_bad_state.is_set()

    @property
    def executor_exception(self):
        """Returns an exception that indicates why this executor is in an irrecoverable state."""
        return self._executor_exception

    def handle_errors(self, status: Dict[str, JobStatus]) -> None:
        """This method is called by the error management infrastructure after a status poll. The
        executor implementing this method is then responsible for detecting abnormal conditions
        based on the status of submitted jobs. If the executor does not implement any special
        error handling, this method should return False, in which case a generic error handling
        scheme will be used.
        :param status: status of all jobs launched by this executor
        """
        self.block_error_handler(self, status)

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
            raise ScalingFailed(self, "No execution provider available")
        block_ids = []
        logger.info(f"Scaling out by {blocks} blocks")
        for i in range(blocks):
            block_id = str(self._block_id_counter.get_id())
            logger.info(f"Allocated block ID {block_id}")
            try:
                job_id = self._launch_block(block_id)
                self.blocks[block_id] = job_id
                self.block_mapping[job_id] = block_id
                block_ids.append(block_id)
            except Exception as ex:
                self._fail_job_async(block_id,
                                     "Failed to start block {}: {}".format(block_id, ex))
        return block_ids

    @abstractmethod
    def scale_in(self, blocks: int) -> List[str]:
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.

        :return: A list of block ids corresponding to the blocks that were removed.
        """
        pass

    def _launch_block(self, block_id: str) -> Any:
        launch_cmd = self._get_launch_command(block_id)
        job_name = f"parsl.{self.label}.block-{block_id}"
        logger.debug("Submitting to provider with job_name %s", job_name)
        job_id = self.provider.submit(launch_cmd, 1, job_name)
        if job_id:
            logger.debug(f"Launched block {block_id} on executor {self.label} with job ID {job_id}")
        else:
            raise ScalingFailed(self,
                                "Attempt to provision nodes did not return a job ID")
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
