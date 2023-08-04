from typing import Dict

from parsl.executors.base import ParslExecutor
from parsl.jobs.states import JobStatus, JobState


def simple_error_handler(executor: ParslExecutor, status: Dict[str, JobStatus], threshold: int):
    (total_jobs, failed_jobs) = _count_jobs(status)
    if total_jobs >= threshold and failed_jobs == total_jobs:
        executor.set_bad_state_and_fail_all(_get_error(status))


def _count_jobs(status: Dict[str, JobStatus]):
    total = 0
    failed = 0
    for js in status.values():
        total += 1
        if js.state == JobState.FAILED:
            failed += 1
    return total, failed


def _get_error(status: Dict[str, JobStatus]) -> Exception:
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
