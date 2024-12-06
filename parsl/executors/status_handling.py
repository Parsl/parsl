from __future__ import annotations

import datetime
import logging
import threading
import time
from abc import abstractmethod, abstractproperty
from concurrent.futures import Future
from itertools import compress
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from parsl.executors.base import ParslExecutor
from parsl.executors.errors import BadStateException, ScalingFailed
from parsl.jobs.error_handlers import noop_error_handler, simple_error_handler
from parsl.jobs.states import TERMINAL_STATES, JobState, JobStatus
from parsl.monitoring.message_type import MessageType
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

        self._executor_bad_state = threading.Event()
        self._executor_exception: Optional[Exception] = None

        self._block_id_counter = AtomicIDCounter()

        self._tasks = {}  # type: Dict[object, Future]

        self._last_poll_time = 0.0

        # these four structures track, in loosely coordinated fashion, the
        # existence of blocks and jobs and how to map between their
        # identifiers.
        self.blocks_to_job_id = {}  # type: Dict[str, str]
        self.job_ids_to_block = {}  # type: Dict[str, str]

        # errors can happen during the submit call to the provider; this is used
        # to keep track of such errors so that they can be handled in one place
        # together with errors reported by status()
        self._simulated_status: Dict[str, JobStatus] = {}

        # this stores an approximation (sometimes delayed) of the latest status
        # of pending, active and recently terminated blocks
        self._status = {}  # type: Dict[str, JobStatus]

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

    @abstractproperty
    def outstanding(self) -> int:
        """This should return the number of tasks that the executor has been given to run (waiting to run, and running now)"""

        raise NotImplementedError("Classes inheriting from BlockProviderExecutor must implement "
                                  "outstanding()")

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

    def _filter_scale_in_ids(self, to_kill: Sequence[Any], killed: Sequence[bool]) -> Sequence[Any]:
        """ Filter out job id's that were not killed
        """
        assert len(to_kill) == len(killed)

        if False in killed:
            killed_job_ids = [jid for jid, k in zip(to_kill, killed) if k]
            not_killed_job_ids = [jid for jid, k in zip(to_kill, killed) if not k]
            logger.warning("Some jobs were not killed successfully: "
                           f"killed jobs: {killed_job_ids}, "
                           f"not-killed jobs: {not_killed_job_ids}")

        # Filters first iterable by bool values in second
        return list(compress(to_kill, killed))

    def scale_out_facade(self, n: int) -> List[str]:
        """Scales out the number of blocks by "blocks"
        """
        if not self.provider:
            raise ScalingFailed(self, "No execution provider available")
        block_ids = []
        monitoring_status_changes = {}
        logger.info(f"Scaling out by {n} blocks")
        for _ in range(n):
            block_id = str(self._block_id_counter.get_id())
            logger.info(f"Allocated block ID {block_id}")
            try:
                job_id = self._launch_block(block_id)

                pending_status = JobStatus(JobState.PENDING)

                self.blocks_to_job_id[block_id] = job_id
                self.job_ids_to_block[job_id] = block_id
                self._status[block_id] = pending_status

                monitoring_status_changes[block_id] = pending_status
                block_ids.append(block_id)

            except Exception as ex:
                failed_status = JobStatus(JobState.FAILED, "Failed to start block {}: {}".format(block_id, ex))
                self._simulated_status[block_id] = failed_status
                self._status[block_id] = failed_status

        self.send_monitoring_info(monitoring_status_changes)
        return block_ids

    def scale_in(self, blocks: int) -> List[str]:
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        The default implementation will kill blocks without regard to their
        status or whether they are executing tasks. Executors with more
        nuanced scaling strategies might overload this method to work with
        that strategy - see the HighThroughputExecutor for an example of that.

        :return: A list of block ids corresponding to the blocks that were removed.
        """

        active_blocks = [block_id for block_id, status in self._status.items()
                         if status.state not in TERMINAL_STATES]

        block_ids_to_kill = active_blocks[:blocks]

        job_ids_to_kill = [self.blocks_to_job_id[block] for block in block_ids_to_kill]

        # Cancel the blocks provisioned
        if self.provider:
            logger.info(f"Scaling in jobs: {job_ids_to_kill}")
            r = self.provider.cancel(job_ids_to_kill)
            job_ids = self._filter_scale_in_ids(job_ids_to_kill, r)
            block_ids_killed = [self.job_ids_to_block[job_id] for job_id in job_ids]
            return block_ids_killed
        else:
            logger.error("No execution provider available to scale in")
            return []

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
        block_ids = list(self.blocks_to_job_id.keys())
        job_ids = []  # types: List[Any]
        for bid in block_ids:
            job_ids.append(self.blocks_to_job_id[bid])
        return block_ids, job_ids

    @abstractproperty
    def workers_per_node(self) -> Union[int, float]:
        pass

    def send_monitoring_info(self, status: Dict) -> None:
        # Send monitoring info for HTEX when monitoring enabled
        if self.submit_monitoring_radio:
            msg = self.create_monitoring_info(status)
            logger.debug("Sending block monitoring message: %r", msg)
            self.submit_monitoring_radio.send((MessageType.BLOCK_INFO, msg))

    def create_monitoring_info(self, status: Dict[str, JobStatus]) -> Sequence[object]:
        """Create a monitoring message for each block based on the poll status.
        """
        msg = []
        for bid, s in status.items():
            d: Dict[str, Any] = {}
            d['run_id'] = self.run_id
            d['status'] = s.status_name
            d['timestamp'] = datetime.datetime.now()
            d['executor_label'] = self.label
            d['job_id'] = self.blocks_to_job_id.get(bid, None)
            d['block_id'] = bid
            msg.append(d)
        return msg

    def poll_facade(self) -> None:
        now = time.time()
        if now >= self._last_poll_time + self.status_polling_interval:
            previous_status = self._status
            self._status = self.status()
            self._last_poll_time = now
            delta_status = {}
            for block_id in self._status:
                if block_id not in previous_status \
                   or previous_status[block_id].state != self._status[block_id].state:
                    delta_status[block_id] = self._status[block_id]

            if delta_status:
                self.send_monitoring_info(delta_status)

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

    @property
    def status_facade(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks of the executor of this poller.

        :return: a dictionary mapping block ids (in string) to job status
        """
        return self._status

    def scale_in_facade(self, n: int, max_idletime: Optional[float] = None) -> List[str]:

        if max_idletime is None:
            block_ids = self.scale_in(n)
        else:
            # This is a HighThroughputExecutor-specific interface violation.
            # This code hopes, through pan-codebase reasoning, that this
            # scale_in method really does come from HighThroughputExecutor,
            # and so does have an extra max_idletime parameter not present
            # in the executor interface.
            block_ids = self.scale_in(n, max_idletime=max_idletime)  # type: ignore[call-arg]
        if block_ids is not None:
            new_status = {}
            for block_id in block_ids:
                logger.debug("Marking block %s as SCALED_IN", block_id)
                s = JobStatus(JobState.SCALED_IN)
                new_status[block_id] = s
                self._status[block_id] = s
                self._simulated_status[block_id] = s
            self.send_monitoring_info(new_status)
        return block_ids
