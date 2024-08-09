from __future__ import annotations

from typing import Dict, Tuple

import parsl.executors.status_handling as status_handling
from parsl.jobs.errors import TooManyJobFailuresError
from parsl.jobs.states import JobState, JobStatus


def noop_error_handler(executor: status_handling.BlockProviderExecutor, status: Dict[str, JobStatus], threshold: int = 3) -> None:
    pass


def simple_error_handler(executor: status_handling.BlockProviderExecutor, status: Dict[str, JobStatus], threshold: int = 3) -> None:
    logger.info(f"Simple error handler running for {executor.label}")
    logger.info(f"status = {status}")
    (total_jobs, failed_jobs) = _count_jobs(status)
    logger.info(f"(total_jobs, failed_jobs) = {total_jobs}, {failed_jobs}")
    if hasattr(executor.provider, "init_blocks"):
        threshold = max(1, executor.provider.init_blocks)
        logger.info("overriding threshold with max init blocks")

    logger.info(f"threshold is now: {threshold}")
    if total_jobs >= threshold and failed_jobs == total_jobs:
        logger.info("entering failure path")
        executor.set_bad_state_and_fail_all(_get_error(status))
    else:
        logger.info("not entering failure path")
    logger.info(f"Simple error handler finished for {executor.label}")


def windowed_error_handler(executor: status_handling.BlockProviderExecutor, status: Dict[str, JobStatus], threshold: int = 3) -> None:
    sorted_status = [(key, status[key]) for key in sorted(status, key=lambda x: int(x))]
    current_window = dict(sorted_status[-threshold:])
    total, failed = _count_jobs(current_window)
    if failed == threshold:
        executor.set_bad_state_and_fail_all(_get_error(status))


def _count_jobs(status: Dict[str, JobStatus]) -> Tuple[int, int]:
    total = 0
    failed = 0
    for js in status.values():
        total += 1
        if js.state == JobState.FAILED or js.state == JobState.MISSING:
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
