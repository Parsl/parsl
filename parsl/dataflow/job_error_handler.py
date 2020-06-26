from typing import List, Any, Dict

from parsl.dataflow.task_status_poller import ExecutorStatus
from parsl.executors.base import ParslExecutor
from parsl.providers.provider_base import JobStatus, JobState


class JobErrorHandler(object):
    def __init__(self):
        pass

    def run(self, status: List[ExecutorStatus]):
        for es in status:
            self._check_irrecoverable_executor(es)

    def _check_irrecoverable_executor(self, es: ExecutorStatus):
        if not es.executor.error_management_enabled:
            return
        custom_handling = es.executor.handle_errors(self, es.status)
        if not custom_handling:
            self.simple_error_handler(es.executor, es.status, 3)

    def simple_error_handler(self, executor: ParslExecutor, status: Dict[Any, JobStatus], threshold: int):
        (total_jobs, failed_jobs) = self.count_jobs(status)
        if total_jobs >= threshold and failed_jobs == total_jobs:
            executor.set_bad_state_and_fail_all(self.get_error(status))

    def count_jobs(self, status: Dict[Any, JobStatus]):
        total = 0
        failed = 0
        for js in status.values():
            total += 1
            if js.state == JobState.FAILED:
                failed += 1
        return total, failed

    def get_error(self, status: Dict[Any, JobStatus]) -> Exception:
        """Concatenate all errors."""
        err = ""
        count = 1
        for js in status.values():
            if js.message is not None:
                err = err + "{}. {}\n".format(count, js.message)
                count += 1
            stdout = js.stdout_summary
            if stdout:
                err = err + "\tSTDOUT: {}\n".format(stdout)
            stderr = js.stderr_summary
            if stderr:
                err = err + "\tSTDOUT: {}\n".format(stderr)

        if len(err) == 0:
            err = "[No error message received]"
        # wrapping things in an exception here doesn't really help in providing more information
        # than the string itself
        return Exception(err)
