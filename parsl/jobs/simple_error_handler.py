from __future__ import annotations

from typing import Dict

import parsl.executors.status_handling as status_handling
from parsl.jobs.states import JobStatus, JobState
from parsl.jobs.errors import TooManyJobFailuresError


def simple_error_handler(executor: status_handling.BlockProviderExecutor, status: Dict[str, JobStatus], threshold: int):
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
        err = err + f"Error {count}:\n"
        count += 1

        if js.message is not None:
            err = err + f"\t{js.message}\n"

        if js.exit_code is not None:
            err = err + f"\tEXIT CODE: {js.exit_code}\n"

        stdout = js.stdout_summary
        if stdout:
            err = err + "\tSTDOUT: {}\n".format(stdout)

        stderr = js.stderr_summary
        if stderr:
            err = err + "\tSTDERR: {}\n".format(stderr)

    if len(err) == 0:
        err = "No error messages received"
    # wrapping things in an exception here doesn't really help in providing more information
    # than the string itself
    return TooManyJobFailuresError(err)
