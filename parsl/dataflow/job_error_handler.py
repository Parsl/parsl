from typing import List, Dict

from parsl.dataflow.job_status_poller import ExecutorStatus
from parsl.executors.base import ParslExecutor
from parsl.providers.provider_base import JobStatus, JobState


class JobErrorHandler:
    def run(self, status: List[ExecutorStatus]):
        for es in status:
            self._check_irrecoverable_executor(es)

    def _check_irrecoverable_executor(self, es: ExecutorStatus):
        if not es.executor.error_management_enabled:
            return
        es.executor.handle_errors(self, es.status)

    def simple_error_handler(self, executor: ParslExecutor, status: Dict[str, JobStatus], threshold: int):
        (total_jobs, failed_jobs) = self.count_jobs(status)
        if total_jobs >= threshold and failed_jobs == total_jobs:
            executor.set_bad_state_and_fail_all(self.get_error(status))

    def count_jobs(self, status: Dict[str, JobStatus]):
        total = 0
        failed = 0
        for js in status.values():
            total += 1
            if js.state == JobState.FAILED:
                failed += 1
        return total, failed

    def get_error(self, status: Dict[str, JobStatus]) -> Exception:
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
                err = err + "\tSTDERR: {}\n".format(stderr)

        if len(err) == 0:
            err = "[No error message received]"
        # wrapping things in an exception here doesn't really help in providing more information
        # than the string itself
        return Exception(err)
