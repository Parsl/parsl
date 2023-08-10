import pytest

from parsl.executors import HighThroughputExecutor
from unittest.mock import Mock
from parsl.jobs.states import JobStatus, JobState
from parsl.jobs.simple_error_handler import simple_error_handler, windowed_error_handler


@pytest.mark.local
def test_block_error_handler_false():
    mock = Mock()
    htex = HighThroughputExecutor(block_error_handler=False)
    assert htex.block_error_handler is False
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
    htex = HighThroughputExecutor(block_error_handler=handler_mock)
    assert htex.block_error_handler is handler_mock

    bad_state_mock = Mock()
    htex.set_bad_state_and_fail_all = bad_state_mock

    bad_jobs = {'1': JobStatus(JobState.FAILED),
                '2': JobStatus(JobState.FAILED),
                '3': JobStatus(JobState.FAILED),
                '4': JobStatus(JobState.FAILED)}

    htex.handle_errors(bad_jobs)
    handler_mock.assert_called()
    handler_mock.assert_called_with(htex, bad_jobs, threshold=3)


@pytest.mark.local
def test_simple_error_handler():
    htex = HighThroughputExecutor(block_error_handler=simple_error_handler)
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
    htex = HighThroughputExecutor(block_error_handler=windowed_error_handler)
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
def test_windowed_error_handler_with_threshold():
    htex = HighThroughputExecutor(block_error_handler=windowed_error_handler,
                                  block_error_threshold=2)
    assert htex.block_error_handler is windowed_error_handler

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
