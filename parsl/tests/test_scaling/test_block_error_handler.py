from functools import partial
from unittest.mock import Mock

import pytest

from parsl.executors import HighThroughputExecutor
from parsl.jobs.error_handlers import (
    noop_error_handler,
    simple_error_handler,
    windowed_error_handler,
)
from parsl.jobs.states import JobState, JobStatus
from parsl.providers import LocalProvider


@pytest.mark.local
def test_block_error_handler_false():
    mock = Mock()
    htex = HighThroughputExecutor(block_error_handler=False, encrypted=True)
    assert htex.block_error_handler is noop_error_handler
    htex.set_bad_state_and_fail_all = mock

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}

    htex.handle_errors(bad_jobs)
    mock.assert_not_called()


@pytest.mark.local
def test_block_error_handler_mock():
    handler_mock = Mock()
    htex = HighThroughputExecutor(block_error_handler=handler_mock, encrypted=True)
    assert htex.block_error_handler is handler_mock

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}

    htex.handle_errors(bad_jobs)
    handler_mock.assert_called()
    handler_mock.assert_called_with(htex, bad_jobs)


@pytest.mark.local
def test_simple_error_handler():
    htex = HighThroughputExecutor(block_error_handler=simple_error_handler,
                                  encrypted=True,
                                  provider=LocalProvider(init_blocks=3))

    assert htex.block_error_handler is simple_error_handler

    bad_state_mock = Mock()
    htex.set_bad_state_and_fail_all = bad_state_mock

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    # Check the bad behavior where if any job is not failed
    # bad state won't be set
    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}

    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}

    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_called()


@pytest.mark.local
def test_windowed_error_handler():
    htex = HighThroughputExecutor(block_error_handler=windowed_error_handler, encrypted=True)
    assert htex.block_error_handler is windowed_error_handler

    bad_state_mock = Mock()
    htex.set_bad_state_and_fail_all = bad_state_mock

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.COMPLETED),
                '4': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_called()


@pytest.mark.local
def test_windowed_error_handler_sorting():
    htex = HighThroughputExecutor(block_error_handler=windowed_error_handler, encrypted=True)
    assert htex.block_error_handler is windowed_error_handler

    bad_state_mock = Mock()
    htex.set_bad_state_and_fail_all = bad_state_mock

    bad_jobs = {'8': JobStatus(JobState.FAILED),
                '9': JobStatus(JobState.FAILED),
                '10': JobStatus(JobState.FAILED),
                '11': JobStatus(JobState.COMPLETED),
                '12': JobStatus(JobState.COMPLETED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'8': JobStatus(JobState.COMPLETED),
                '9': JobStatus(JobState.FAILED),
                '21': JobStatus(JobState.FAILED),
                '22': JobStatus(JobState.FAILED),
                '10': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_called()


@pytest.mark.local
def test_windowed_error_handler_with_threshold():
    error_handler = partial(windowed_error_handler, threshold=2)
    htex = HighThroughputExecutor(block_error_handler=error_handler, encrypted=True)
    assert htex.block_error_handler is error_handler

    bad_state_mock = Mock()
    htex.set_bad_state_and_fail_all = bad_state_mock

    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.COMPLETED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.COMPLETED),
                '3': JobStatus(JobState.COMPLETED),
                '4': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_not_called()

    bad_jobs = {'1': JobStatus(JobState.COMPLETED),
                '2': JobStatus(JobState.COMPLETED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}
    htex.handle_errors(bad_jobs)
    bad_state_mock.assert_called()
