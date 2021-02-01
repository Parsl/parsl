import logging
import threading
from itertools import compress
from abc import abstractmethod
from concurrent.futures import Future
from typing import List, Any, Dict

import parsl  # noqa F401
from parsl.executors.base import ParslExecutor
from parsl.providers.provider_base import JobStatus, ExecutionProvider, JobState

logger = logging.getLogger(__name__)


class StatusHandlingExecutor(ParslExecutor):
    def __init__(self, provider):
        super().__init__()
        self._provider = provider  # type: ExecutionProvider
        # errors can happen during the submit call to the provider; this is used
        # to keep track of such errors so that they can be handled in one place
        # together with errors reported by status()
        self._simulated_status = {}
        self._executor_bad_state = threading.Event()
        self._executor_exception = None
        self._generated_job_id_counter = 1
        self._tasks = {}  # type: Dict[object, Future]

    def _make_status_dict(self, job_ids: List[Any], status_list: List[JobStatus]) -> Dict[Any, JobStatus]:
        """Given a list of job ids and a list of corresponding status strings,
        returns a dictionary mapping each job id to the corresponding status

        :param job_ids: the list of job ids
        :param status_list: the list of job status strings
        :return: the resulting dictionary
        """
        if len(job_ids) != len(status_list):
            raise IndexError("job id list and status string list differ in size")
        d = {}
        for i in range(len(job_ids)):
            d[job_ids[i]] = status_list[i]

        return d

    def _set_provider(self, provider: ExecutionProvider):
        self._provider = provider

    @property
    def status_polling_interval(self):
        if self._provider is None:
            return 0
        else:
            return self._provider.status_polling_interval

    @abstractmethod
    def _get_job_ids(self) -> List[object]:
        raise NotImplementedError("Classes inheriting from StatusHandlingExecutor must implement "
                                  "_get_job_ids()")

    def _fail_job_async(self, job_id: Any, message: str):
        """Marks a job that has failed to start but would not otherwise be included in status()
        as failed and report it in status()
        """
        if job_id is None:
            job_id = "failed-block-{}".format(self._generated_job_id_counter)
            self._generated_job_id_counter += 1
        self._simulated_status[job_id] = JobStatus(JobState.FAILED, message)

    def status(self) -> Dict[object, JobStatus]:
        """Return status of all blocks."""

        if self._provider:
            job_ids = list(self._get_job_ids())
            status = self._make_status_dict(job_ids, self._provider.status(job_ids))
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
                      status: Dict[Any, JobStatus]) -> bool:
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
                      status: Dict[Any, JobStatus]) -> bool:
        return False

    @property
    def tasks(self) -> Dict[object, Future]:
        return self._tasks

    @property
    def provider(self):
        return self._provider
