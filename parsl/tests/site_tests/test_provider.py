import argparse
import logging
import time

import pytest

import parsl
from parsl.app.app import python_app  # , bash_app
from parsl.jobs.states import JobState
from parsl.tests.site_tests.site_config_selector import fresh_config

logger = logging.getLogger(__name__)


@python_app
def platform(sleep=10, stdout=None):
    import time
    time.sleep(sleep)
    return True


@pytest.mark.local
@pytest.mark.skip("This test cannot run on sites which cannot be identified by site_config_selector")
def test_provider():
    """ Provider scaling
    """
    logger.info("Starting test_provider")
    config = fresh_config()
    name = config.executors[0].label
    parsl.load(config)

    dfk = parsl.dfk()
    logger.info("Trying to get executor : {}".format(name))

    x = platform(sleep=0)
    logger.info("Result is {}".format(x.result()))

    executor = dfk.executors[name]
    provider = dfk.executors[name].provider

    # At this point we should have 1 job
    _, current_jobs = executor._get_block_and_job_ids()
    assert len(current_jobs) == 1, "Expected 1 job at init, got {}".format(len(current_jobs))

    logger.info("Getting provider status (1)")
    status = provider.status(current_jobs)
    logger.info("Got provider status")
    assert status[0].state == JobState.RUNNING, "Expected job to be in state RUNNING"

    # Scale down to 0
    scale_in_blocks = executor.scale_in(blocks=1)
    logger.info("Now sleeping 60 seconds")
    time.sleep(60)
    logger.info("Sleep finished")
    logger.info("Getting provider status (2)")
    status = executor.status()
    logger.info("Got executor status")
    logger.info("Block status: {}".format(status))
    assert status[scale_in_blocks[0]].terminal is True, "Terminal state"
    logger.info("Job in terminal state")

    _, current_jobs = executor._get_block_and_job_ids()
    # PR 1952 stoped removing scale_in blocks from self.blocks_to_job_id
    # A new PR will handle removing blocks from self.block
    # this includes failed/completed/canceled blocks
    assert len(current_jobs) == 1, "Expected current_jobs == 1"
    dfk.cleanup()
    parsl.clear()
    logger.info("Ended test_provider")
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="4",
                        help="Count of apps to launch")
    parser.add_argument("-t", "--time", default="60",
                        help="Sleep time for each app")

    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_provider()
